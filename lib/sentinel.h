#ifndef REDIS_SENINEL_H
#define REDIS_SENINEL_H

#include <stddef.h>
#include <stdarg.h>

#define SENTINEL_ERR -1
#define SENTINEL_OK 0
#define SENTINEL_TRYAGAIN 1

typedef struct sentinelContext *sentinelContext;

#define freeSentinelReply(reply) freeReplyObject(reply);

sentinelContext sentinelInit(void *opts);
int sentinelCheckConn(sentinelContext sctx);
void *sentinelvCommand(sentinelContext sctx, const char *format, va_list ap);
void sentinelDestroy(sentinelContext c);

#endif /* REDIS_SENINEL_H */