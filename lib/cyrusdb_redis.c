// TODO add license?
#include <config.h>

#include <sysexits.h>
#include <syslog.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "assert.h"
#include "bsearch.h"
#include "cyrusdb.h"
#include "libcyr_cfg.h"
#include "xmalloc.h"
#include "util.h"

extern void fatal(const char *, int);

typedef struct redis_options {
    int is_sentinel;
    size_t nhosts;
    char **hosts;
    int *ports;
    int database;
    const char *service;        /* Sentinel service name */
    int usessl; /* XXX placeholder for future implementation of SSL/TLS */
    const char *username;       /* Username for AUTH */
    const char *password;       /* Password for AUTH */
    struct timeval timeout;     /* Connect timeout */
    /* Sentinel only */
    size_t reconnect_interval;  /* Reconnect interval in milliseconds */
    size_t max_reconnects;      /* Maximum number of reconnects */
    /* Global list */
    const char *uri;            /* Saved uri */
    size_t refcount;            /* Reference count for shared options */
    struct redis_options *next; /* Next in global list */
} redis_options;

static redis_options *global_opts = NULL;

typedef struct redis_engine {
    const char *name;
    int (*redis_open)(struct dbengine *db);
    int (*redis_exec)(struct dbengine *db, void **reply, const char *format, ...);
    int (*redis_foreach)(struct dbengine *db, const char *prefix, size_t prefixlen,
                         foreach_p *p, foreach_cb *cb, void *rock, struct txn **tid);
    int (*redis_reply)(struct dbengine *db, void *reply, char **data, size_t *datalen);
    void (*redis_close)(struct dbengine *db);
} redis_engine_t;

struct dbengine {
    void *conn;          /* redisContext / sentinelContext */
    char *fname;         /* Database file */
    redis_options *opts; /* Redis options */
};

static int dbinit = 0;
static const redis_engine_t *dbengine = NULL;

#ifdef HAVE_HIREDIS
#include <hiredis/hiredis.h>
#include <sentinel.h>

int hiredis_exec(struct dbengine *db, void **reply_ret, const char *format, ...)
{
    redisReply *reply;
    int r = 0;
    va_list ap;
    va_start(ap, format);
    if (db->opts->is_sentinel) {
        r = sentinelCheckConn(db->conn);
        if (!r) reply = sentinelvCommand(db->conn, format, ap);
        if (!r && reply == NULL) {
            r = sentinelCheckConn(db->conn);
            va_start(ap, format);
            if (!r) reply = sentinelvCommand(db->conn, format, ap);
        }
    } else {
        reply = redisvCommand(db->conn, format, ap);
    }
    va_end(ap);
    if (r == SENTINEL_ERR || reply == NULL) {
        syslog(LOG_ERR, "DBERROR: REDIS%s connection, error name=<%s>",
                (db->opts->is_sentinel ? "sentinel" : " "), db->fname);
        r = CYRUSDB_INTERNAL;
    } else if (reply_ret) {
        *reply_ret = reply;
    } else {
        freeReplyObject(reply);
    }
    return r;
}

int hiredis_open(struct dbengine *db)
{
    if (db->conn) return CYRUSDB_OK;

    if (db->opts->is_sentinel) {
        db->conn = sentinelInit(db->opts);
        if (db->conn == NULL) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to connect",
                            "name=<%s>", db->fname);
            return CYRUSDB_INTERNAL;
        }
    } else {
        syslog(LOG_INFO, "REDIS: Connecting to %s:%d",
                         db->opts->hosts[0], db->opts->ports[0]);
        db->conn = redisConnectWithTimeout(db->opts->hosts[0], db->opts->ports[0],
                                db->opts->timeout);
        if (db->conn == NULL || ((redisContext*)db->conn)->err) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to connect",
                            "name=<%s>", db->fname);
            return CYRUSDB_INTERNAL;
        }
        if (db->opts->username) {
            redisReply *reply = redisCommand(db->conn, "AUTH %s %s",
                                             db->opts->username, db->opts->password);
            if (reply) {
                syslog(LOG_DEBUG, "REDIS: Auth reply: %s", reply->str);
                freeReplyObject(reply);
            }
        } else if (db->opts->password) {
            redisReply *reply = redisCommand(db->conn, "AUTH %s",
                                             db->opts->password);
            if (reply) {
                syslog(LOG_DEBUG, "REDIS: Auth reply: %s", reply->str);
                freeReplyObject(reply);
            }
        }
        redisEnableKeepAlive(db->conn);
        int r = hiredis_exec(db, NULL, "SELECT %d", db->opts->database);
        if (r) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to select database",
                            "name=<%s>", db->fname);
            return r;
        }
    }
    return CYRUSDB_OK;
}

/* This function is very generic, this implementation should only return string and integer (for EXISTS) */
int hiredis_reply(struct dbengine *db, void *reply_v, char **data, size_t *datalen)
{
    redisReply *reply = (redisReply*)reply_v;
    if (reply == NULL) {
        return CYRUSDB_INTERNAL;
    }
    if (reply->type & (REDIS_REPLY_STRING | REDIS_REPLY_BIGNUM | REDIS_REPLY_ERROR
                     | REDIS_REPLY_STATUS | REDIS_REPLY_DOUBLE | REDIS_REPLY_VERB)) {
        *datalen = reply->len;
        if (*datalen == 0) { /* NULL data and return */
            *data = NULL;
            freeReplyObject(reply);
            return CYRUSDB_OK;
        }
        *data = xzmalloc(*datalen);
        memcpy(*data, reply->str, *datalen);
        syslog(LOG_DEBUG, "REDIS: String reply: %zu %s", *datalen, *data);
        if (*data == NULL) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to allocate memory",
                            "name=<%s>", db->fname);
            freeReplyObject(reply);
            return CYRUSDB_INTERNAL;
        }
    } else if (reply->type & (REDIS_REPLY_INTEGER | REDIS_REPLY_BOOL)) {
        *data = xzmalloc(21);
        if (*data == NULL) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to allocate memory",
                             "name=<%s>", db->fname);
            freeReplyObject(reply);
            return CYRUSDB_INTERNAL;
        }
        snprintf(*data, 21, "%lld", reply->integer);
        *datalen = strlen(*data);
        syslog(LOG_DEBUG, "REDIS: Integer reply: %s", *data);
    } else { /* Just in case */
        *data = NULL;
        *datalen = 0;
    }
    freeReplyObject(reply);
    return CYRUSDB_OK;
}

/* If new keys are inserted during foreach then behavior is undefined */
/* XXX find a possible fix? */
int hiredis_foreach(struct dbengine *db, const char *prefix, size_t prefixlen,
                  foreach_p *goodp, foreach_cb *cb, void *rock, struct txn **tid)
{
    redisReply *reply;
    int r;
    char *cursor = xstrdup("0");
    assert(!tid);

    /* Iterative SCAN */
    int iteration = 0;
    do {
        syslog(LOG_DEBUG, "REDIS: Scanning iteration %d, cursor: %s", iteration++, cursor);
        r = hiredis_exec(db, (void**)&reply, "SCAN %s MATCH %b*", cursor, prefix, prefixlen);
        if (r) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to execute SCAN",
                             "name=<%s>", db->fname);
            return r;
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2
        || reply->element[0]->type != REDIS_REPLY_STRING) {
            freeReplyObject(reply);
            return CYRUSDB_INTERNAL;
        }
        free(cursor);
        cursor = xstrdup(reply->element[0]->str);

        /* Callback for every element */
        if (reply->element[1]->type == REDIS_REPLY_ARRAY) {
            for (size_t i = 0 ; i < reply->element[1]->elements ; ++i) {
                size_t keylen = reply->element[1]->element[i]->len;
                const char *key = reply->element[1]->element[i]->str;
                char *data;
                size_t datalen;
                redisReply *get_reply;
                r = hiredis_exec(db, (void**)&get_reply, "GET %b", key, keylen);
                if (r) {
                    xsyslog(LOG_ERR, "DBERROR: REDIS failed to execute GET",
                                    "name=<%s>", db->fname);
                    free(cursor);
                    return r;
                }
                hiredis_reply(db, get_reply, (char**)&data, &datalen);
                if (!goodp || goodp(rock, key, keylen, data, datalen)) {
                    if (cb(rock, key, keylen, data, datalen)) {
                        freeReplyObject(reply);
                        free(cursor);
                        free(data);
                        return CYRUSDB_INTERNAL;
                    }
                }
                free(data);
            }
        }
        freeReplyObject(reply);
    } while (strcmp(cursor, "0")); /* Continue scanning till scan returns 0 */
    free(cursor);
    return CYRUSDB_OK;
}

void hiredis_close(struct dbengine *db)
{
    if (db->conn) {
        if (db->opts->is_sentinel) {
            syslog(LOG_INFO, "REDIS: Disconnecting from sentinel");
            sentinelDestroy(db->conn);
        } else {
            redisFree(db->conn);
        }
    }
}

#endif

static const redis_engine_t redis_engines[] = {
#ifdef HAVE_HIREDIS
    { "hiredis", &hiredis_open, &hiredis_exec,
      &hiredis_foreach, &hiredis_reply, &hiredis_close },
#endif /* HAVE_HIREDIS */
    { NULL, NULL, NULL, NULL, NULL, NULL}
};

/* Deallocate all memory used in options */
void redis_destroy_options(redis_options *opts) {
    if (opts == NULL) return;
    if (opts->hosts) {
        if (*opts->hosts) xzfree(*opts->hosts);
        xzfree(opts->hosts);
    }
    if (opts->ports) xzfree(opts->ports);
    xzfree(opts);
}

/* Parse filename as URI with options */
int redis_parse_options(const char *uri, redis_options **ret)
{

    syslog(LOG_DEBUG, "REDIS: Parsing options from URI: \"%s\"", uri);

    /* Check global array for matching URI */
    redis_options *opts_iter = global_opts;
    while (opts_iter) {
        if (strcmp(opts_iter->uri, uri) == 0) {
            ++opts_iter->refcount;
            *ret = opts_iter;
            return CYRUSDB_OK;
        }
        opts_iter = opts_iter->next;
    }

    syslog(LOG_DEBUG, "REDIS: Creating new options for URI: \"%s\"", uri);

    redis_options *opts = xzmalloc(sizeof(redis_options));
    if (!opts) goto mem_error;

    const char *ptr = uri;

    /* Parse mode */
    // XXX Maybe SSL should be specified by adding an s at the end
    // of the protocol ( e.g. rediss:// or sentinels:// )
    if (!strncmp(ptr, "redis://", 8)) {
        opts->is_sentinel = 0;
        ptr += 8;
    } else if (!strncmp(ptr, "sentinel://", 11)) {
        opts->is_sentinel = 1;
        ptr += 11;
    } else {
        xsyslog(LOG_ERR, "DBERROR: REDIS invalid URI scheme",
                         "uri=<%s>", uri);
        goto error;
    }

    /* Check if there are credentials */
    const char *at = strchr(ptr, '@');
    if (at) {
        const char *colon = strchr(ptr, ':');
        char *username = NULL, *password = NULL;
        if (colon && colon < at) { /* We have username */
            username = xzmalloc(colon - ptr + 1);
            if (!username) goto mem_error;
            strncpy(username, ptr, colon - ptr);
            username[colon - ptr] = '\0';
            ptr = colon + 1;
        }
        password = xzmalloc(at - ptr + 1);
        if (!password) goto mem_error;
        strncpy(password, ptr, at - ptr);
        password[at - ptr] = '\0';
        ptr = at + 1;
        opts->username = username;
        opts->password = password;
    }

    /* Hosts array */
    const char *slash = strchr(ptr, '/');
    if (!slash) {
        xsyslog(LOG_ERR, "DBERROR: REDIS invalid URI, missing path",
                         "uri=<%s>", uri);
        goto error;
    }
    if (slash == ptr) { /* No hosts */
        xsyslog(LOG_ERR, "DBERROR: REDIS no hosts specified",
                         "uri=<%s>", uri);
        goto error;
    }
    char *hosts_line = xzmalloc(slash - ptr + 1);
    if (!hosts_line) goto mem_error;
    strncpy(hosts_line, ptr, slash - ptr);
    hosts_line[slash - ptr] = '\0';
    ptr = slash + 1;
    syslog(LOG_DEBUG, "REDIS: Hosts line: \"%s\"", hosts_line);
    /* Separate hosts */
    opts->nhosts = 1;
    char *host_linep = hosts_line - 1;
    while (*++host_linep) opts->nhosts += (*host_linep == ',');
    syslog(LOG_DEBUG, "REDIS: Found %zu hosts", opts->nhosts);
    if (opts->nhosts == 0) {
        xsyslog(LOG_ERR, "DBERROR: REDIS no hosts specified",
                         "uri=<%s>", uri);
        goto error;
    }
    opts->hosts = xzmalloc(sizeof(char*) * opts->nhosts);
    opts->ports = xzmalloc(sizeof(int) * opts->nhosts);
    if (!opts->hosts || !opts->ports) goto mem_error;
    size_t i = 0; /* Index */
    char *host = strtok(hosts_line, ",");
    while (host) {
        opts->hosts[i++] = host;
        host = strtok(NULL, ",");
    }
    for (i = 0; i < opts->nhosts; ++i) {
        char *port = strchr(opts->hosts[i], ':');
        if (!port) {
            opts->ports[i] = 6379; /* Default port */
            continue;
        }
        *port++ = '\0';
        opts->ports[i] = atoi(port);
        if (opts->ports[i] <= 0 || opts->ports[i] > 65535) {
            xsyslog(LOG_ERR, "DBERROR: REDIS invalid port",
                             "uri=<%s> port=<%s>", uri, port);
            goto error;
        }
    }

    syslog(LOG_DEBUG, "REDIS: Parsed hosts");

    /* Service and database */
    if (opts->is_sentinel) { /* Only sentinel has service */
        slash = strchr(ptr, '/');
        if (!slash) {
            xsyslog(LOG_ERR, "DBERROR: REDIS invalid URI, missing service",
                            "uri=<%s>", uri);
            goto error;
        }
        char *service = xzmalloc(slash - ptr + 1);
        if (!service) goto mem_error;
        strncpy(service, ptr, slash - ptr);
        service[slash - ptr] = '\0';
        ptr = slash + 1;
        opts->service = service;
    }
    opts->database = atoi(ptr); /* Dont care if / at the end or not */

    syslog(LOG_DEBUG, "REDIS: Parsed service and database");

    opts->timeout.tv_sec = libcyrus_config_getint(CYRUSOPT_REDIS_TIMEOUT);
    opts->max_reconnects = libcyrus_config_getint(CYRUSOPT_REDIS_MAX_RETRIES);
    opts->reconnect_interval = libcyrus_config_getint(CYRUSOPT_REDIS_RECONNECT_DELAY) * 1000;

    if (opts->timeout.tv_sec < 1) opts->timeout.tv_sec = 1;

    opts->uri = xstrdup(uri);
    if (!opts->uri) goto mem_error;
    opts->refcount = 1;
    opts->next = global_opts;
    global_opts = opts;

    *ret = opts;
    return CYRUSDB_OK;

mem_error:
    xsyslog(LOG_ERR, "DBERROR: REDIS failed to allocate memory",
                     "name=<%s>", uri);
    if (opts) redis_destroy_options(opts);
    return CYRUSDB_INTERNAL;
error:
    if (opts) redis_destroy_options(opts);
    return CYRUSDB_IOERROR;
}

static int init(const char *dbdir __attribute__((unused)),
                int flags __attribute__((unused)))
{
    if (dbinit++) return CYRUSDB_OK;
    /* TODO maybe add config option for multiple engines */
    dbengine = redis_engines;
    return CYRUSDB_OK;
}


static int myopen(const char *fname, int flags, struct dbengine **ret, struct txn **mytid)
{
    struct dbengine *db;
    int r;

    assert(!mytid);

    /* Allocate engine */
    db = (struct dbengine *) xzmalloc(sizeof(struct dbengine));
    if (db == NULL) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed to allocate memory",
                         "name=<%s>", fname);
        return CYRUSDB_INTERNAL;
    }

    r = redis_parse_options(fname, &db->opts);
    if (r) {
        xzfree(db);
        return r;
    }
    db->fname = xstrdup(fname);
    if (db->fname == NULL) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed to allocate memory",
                         "name=<%s>", fname);
        redis_destroy_options(db->opts);
        xzfree(db);
        return CYRUSDB_INTERNAL;
    }

    /* Open connection */
    r = dbengine->redis_open(db);
    if (r) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed to open connection",
                         "name=<%s>", fname);
        redis_destroy_options(db->opts);
        xzfree(db->fname);
        xzfree(db);
        return CYRUSDB_INTERNAL;
    }

    *ret = db;
    return CYRUSDB_OK;
}

static int mystore(struct dbengine *db,
                   const char *key, int keylen,
                   const char *data, int datalen,
                   struct txn **tid, int overwrite,
                   int isdelete)
{
    const char dummy = 0;
    int r = 0;

    assert(db);
    assert(key);
    assert(keylen);
    assert(!tid);

    if (datalen) assert(data);
    if (!data) data = &dummy;

    if (isdelete) {
        /* DELETE the entry */
        syslog(LOG_DEBUG, "REDIS: DEL %.*s", keylen, key);
        r = dbengine->redis_exec(db, NULL, "DEL %b", key, (size_t) keylen);
    }
    else {
        /* Check if entry exists */
        /**
         *  This check could potentially be avoided cause redis has the NX arg
         *  but cyrus might care about this error being thrown.
         */
        void *reply;
        r = dbengine->redis_exec(db, &reply, "EXISTS %b", key, (size_t) keylen);
        if (r) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed",
                             "command=<EXISTS>");
            // if (tid) abort_txn(db, *tid);
            return CYRUSDB_INTERNAL;
        }
        char exists[7] = {0}; /* Command will return "0" or "1" or "QUEUED" (MULTI) */
        size_t existslen;
        r = dbengine->redis_reply(db, reply, (char**)&exists, &existslen);
        if (r) {
            xsyslog(LOG_ERR, "DBERROR: REDIS failed to parse reply",
                             "command=<EXISTS>");
            return CYRUSDB_INTERNAL;
        }
        if (strncmp(exists, "1", 1) == 0 && !overwrite) { /* Entry exists */
            return CYRUSDB_EXISTS;
        }
        r = dbengine->redis_exec(db, NULL, "SET %b %b%s", key, (size_t) keylen
                                                     , data, (size_t) datalen
                                                     , overwrite ? "" : " NX");
    }

    if (r) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed",
                         "command=<%s %.*s>",
                         isdelete ? "DEL" : "SET", keylen, key);
        return CYRUSDB_INTERNAL;
    }

    return 0;
}

static int create(struct dbengine *db,
                  const char *key, size_t keylen,
                  const char *data, size_t datalen,
                  struct txn **tid)
{
    return mystore(db, key, keylen, data, datalen, tid, 0, 0);
}

static int store(struct dbengine *db,
                 const char *key, size_t keylen,
                 const char *data, size_t datalen,
                 struct txn **tid)
{
    return mystore(db, key, keylen, data, datalen, tid, 1, 0);
}

static int delete(struct dbengine *db,
                  const char *key, size_t keylen,
                  struct txn **tid,
                  int force __attribute__((unused)))
{
    return mystore(db, key, keylen, NULL, 0, tid, 1, 1);
}

// TODO specify whole command in error
static int fetch(struct dbengine *db,
                 const char *key, size_t keylen,
                 const char **data, size_t *datalen,
                 struct txn **tid)
{
    int r = 0;

    assert(db);
    assert(key);
    assert(keylen);
    assert(!tid);

    void *reply;
    r = dbengine->redis_exec(db, &reply, "GET %b", key, (size_t) keylen);
    if (r) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed",
                         "command=<GET>");
        // if (tid) abort_txn(db, *tid);
        return CYRUSDB_INTERNAL;
    }
    r = dbengine->redis_reply(db, reply, (char **) data, datalen);
    if (r) { /* TODO make sure this can happen */
        xsyslog(LOG_ERR, "DBERROR: REDIS failed to parse reply",
                         "command=<GET>");
        return CYRUSDB_INTERNAL;
    }

    return CYRUSDB_OK;
}

static int myclose(struct dbengine *db)
{
    assert(db);

    dbengine->redis_close(db);

    /* We don't really need to ever free these structs */
    if (--db->opts->refcount == 0) {
        redis_options *t = global_opts, *p = NULL;
        while (t) {
            if (t == db->opts) {
                if (p) p->next = t->next;
                else global_opts = t->next;
                break;
            }
            p = t;
            t = t->next;
        }
        redis_destroy_options(db->opts);
    }

    xzfree(db->fname);
    xzfree(db);

    return CYRUSDB_OK;
}

static int foreach(struct dbengine *db,
                   const char *prefix, size_t prefixlen,
                   foreach_p *goodp,
                   foreach_cb *cb, void *rock,
                   struct txn **tid)
{
    int r;

    assert(db);
    assert(cb);
    assert(!tid);
    if (prefixlen) assert(prefix);


    r = dbengine->redis_foreach(db, prefix, prefixlen, goodp, cb, rock, tid);

    if (r) {
        xsyslog(LOG_ERR, "DBERROR: REDIS failed",
                         "command=<SCAN>");
        // if (tid) abort_txn(db, *tid);
        return CYRUSDB_INTERNAL;
    }

    return CYRUSDB_OK;
}

static int done(void)
{
    --dbinit;
    return CYRUSDB_OK;
}

HIDDEN struct cyrusdb_backend cyrusdb_redis =
{
    "redis",                      /* name */

    &init,
    &done,
    &cyrusdb_generic_noarchive,
    NULL, // unlink

    &myopen,
    &myclose,

    &fetch,
    &fetch,
    NULL, // fetch_next

    &foreach,
    &create,
    &store,
    &delete,

    NULL, // lock
    NULL, // commit
    NULL, // abort

    NULL, // dump
    NULL, // consistent
    NULL, // repack
    &bsearch_ncompare_raw
};
