#include <config.h>

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sysexits.h>
#include <syslog.h>

#include "xmalloc.h"
#include "util.h"
#include "sentinel.h"

#ifdef HAVE_HIREDIS
#include <hiredis/hiredis.h>

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
} redis_options;

typedef struct sentinelContext {
    redis_options *opts;  /* Redis options */
    size_t sentinel_idx;  /* Current sentinel index */
    redisContext *rctx;   /* Active master context */
} *sentinelContext;

int _connectToMasterWithRetry(sentinelContext sctx);
int _connectToMaster(sentinelContext sctx);

sentinelContext sentinelInit(void *opts) {
    /* Initialize context */
    sentinelContext c = xzmalloc(sizeof(*c));
    if (c == NULL)
        goto sentinel_init_failure;
    c->opts = opts;

    /* Try to connect to master */
    if (_connectToMasterWithRetry(c) == SENTINEL_ERR)
        goto sentinel_init_failure;

    return c;
    sentinel_init_failure:;
    sentinelDestroy(c);
    return NULL;
}

int sentinelCheckConn(sentinelContext sctx) {
    if (sctx->rctx == NULL)
        return _connectToMasterWithRetry(sctx);
    return SENTINEL_OK;
}

void *sentinelvCommand(sentinelContext sctx, const char *format, va_list ap) {
    void *reply = redisvCommand(sctx->rctx, format, ap);
    /* This is fatal in any way it failed */
    if (reply == NULL) {
        redisFree(sctx->rctx);
        sctx->rctx = NULL;
    }
    return reply;
}

void sentinelDestroy(sentinelContext c) {
    if (c->rctx) redisFree(c->rctx);
    if (c) xzfree(c);
}

int _connectToMasterWithRetry(sentinelContext sctx) {
    int r = _connectToMaster(sctx);
    for (size_t i = 0; r != SENTINEL_OK && i < sctx->opts->max_reconnects; ++i) {
        xsyslog(LOG_INFO, "connectToMaster",
                "Reconnecting in %zu milliseconds", sctx->opts->reconnect_interval);
        usleep(sctx->opts->reconnect_interval * 1000);
        r = _connectToMaster(sctx);
    }
    return r;
}

// TODO implement service name error handling
int _connectToMaster(sentinelContext sctx) {
    redisContext *c;
    redisReply *reply;
    char *master_ip = NULL, *sent_ip = NULL;
    int master_port = 0, sent_port = 0;

    for (size_t i = 0 ; i < sctx->opts->nhosts ; ++i) {
        /* Connect to sentinel */
        sent_ip = sctx->opts->hosts[sctx->sentinel_idx];
        sent_port = sctx->opts->ports[sctx->sentinel_idx];
        redisOptions options = {0};
        REDIS_OPTIONS_SET_TCP(&options, sent_ip, sent_port);
        options.connect_timeout = &sctx->opts->timeout; /* Always have timeout on connect */
        options.options |= REDIS_OPT_REUSEADDR | REDIS_OPT_PREFER_IP_UNSPEC;
        c = redisConnectWithOptions(&options);
        xsyslog(LOG_DEBUG, "connectToMaster",
               "Connecting to Sentinel[%s:%d]", sent_ip, sent_port);
        if (c == NULL || c->err) goto next_sentinel;

        /* If a password has been provided for Sentinels */
        if (sctx->opts->username != NULL) { /* Non null user means extended */
            reply = redisCommand(c, "AUTH %s %s", sctx->opts->username, sctx->opts->password);
            if (!reply) goto next_sentinel;
            xsyslog(LOG_DEBUG, "connectToMaster",
                    "Auth Sentinel[%s:%d] %s", sent_ip, sent_port, reply->str);
            freeReplyObject(reply);
        }
        else if (sctx->opts->password != NULL) { /* Only password */
            reply = redisCommand(c, "AUTH %s", sctx->opts->password);
            if (!reply) goto next_sentinel;
            xsyslog(LOG_DEBUG, "connectToMaster",
                    "Auth Sentinel[%s:%d] %s", sent_ip, sent_port, reply->str);
            freeReplyObject(reply);
        }

        /* Get master information */
        xsyslog(LOG_DEBUG, "connectToMaster",
               "Querying Sentinel[%s:%d] for master of service %s",
               sent_ip, sent_port, sctx->opts->service);
        reply = redisCommand(c, "SENTINEL get-master-addr-by-name %s", sctx->opts->service);
        if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) goto next_sentinel_reply;
        master_ip = xstrdup(reply->element[0]->str);
        master_port = atoi(reply->element[1]->str);
        freeReplyObject(reply);
        redisFree(c);
        xsyslog(LOG_DEBUG, "connectToMaster",
                "Sentinel[%s:%d] gave master at Master[%s:%d]",
                sent_ip, sent_port, master_ip, master_port);

        /* Try to connect to master */
        redisOptions moptions = {0};
        REDIS_OPTIONS_SET_TCP(&moptions, master_ip, master_port);
        moptions.connect_timeout = &sctx->opts->timeout; /* Always have timeout on connect */
        moptions.options |= REDIS_OPT_REUSEADDR | REDIS_OPT_PREFER_IP_UNSPEC;
        c = redisConnectWithOptions(&moptions);
        xsyslog(LOG_DEBUG, "connectToMaster",
               "Connecting to Master[%s:%d]", master_ip, master_port);
        if (c == NULL || c->err) goto next_sentinel;

        /* If a password has been provided for the master */
        if (sctx->opts->username != NULL) { /* Non null user means extended */
            reply = redisCommand(c, "AUTH %s %s", sctx->opts->username, sctx->opts->password);
            if (!reply) goto next_sentinel;
            xsyslog(LOG_DEBUG, "connectToMaster",
                    "Auth Master[%s:%d] %s", master_ip, master_port, reply->str);
            freeReplyObject(reply);
        }
        else if (sctx->opts->password != NULL) { /* Only password */
            reply = redisCommand(c, "AUTH %s", sctx->opts->password);
            if (!reply) goto next_sentinel;
            xsyslog(LOG_DEBUG, "connectToMaster",
                    "Auth Master[%s:%d] %s", master_ip, master_port, reply->str);
            freeReplyObject(reply);
        }

        reply = redisCommand(c, "ROLE"); /* TODO Implement legacy info */
        if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 1) goto next_sentinel_reply;
        if (strcmp(reply->element[0]->str, "master") == 0) { /* Found master */
            redisEnableKeepAlive(c);
            freeReplyObject(reply);
            reply = redisCommand(c, "SELECT %d", sctx->opts->database);
            if (!reply || reply->type != REDIS_REPLY_STATUS) goto next_sentinel_reply;
            xsyslog(LOG_DEBUG, "connectToMaster",
                    "Selected database %d", sctx->opts->database);
            sctx->rctx = c;
            xsyslog(LOG_INFO, "connectToMaster",
                    "Connected to Master[%s:%d]", master_ip, master_port);
            xzfree(master_ip);
            freeReplyObject(reply);
            return SENTINEL_OK;
        }
        next_sentinel_reply:;
        if (reply) freeReplyObject(reply);
        next_sentinel:;
        xsyslog(LOG_NOTICE, "connectToMaster",
                "Sentinel[%s:%d] failed to provide master", sent_ip, sent_port);
        sctx->sentinel_idx = (sctx->sentinel_idx + 1) % sctx->opts->nhosts;
        if (c) redisFree(c);
        if (master_ip) xzfree(master_ip);
        sctx->rctx = NULL;
    }
    /* TODO this log is notice because reconnect will call this again
        maybe change this according to reconnects */
    xsyslog(LOG_NOTICE, "connectToMaster", "Failed to connect to master");
    return SENTINEL_ERR;
}

#endif /* HAVE_HIREDIS */