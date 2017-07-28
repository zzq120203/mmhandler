/*
 *
 * Header file for MM C Client internal use.
 *
 * please do NOT include this file directly.
 *
 */

#ifndef __MMCC_LL_H__
#define __MMCC_LL_H__

#include "mmcc.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <netinet/in.h> 
#include "hiredis.h"
#include <netdb.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <limits.h>
#include "hvfs_u.h"
#include "xlist.h"
#include "xhash.h"
#include "xlock.h"
#include "memory.h"

#define HVFS_TRACING
#include "tracing.h"

struct redisConnection
{
    struct list_head list;

    time_t born;                /* born time */
    time_t ttl;                 /* this connection used last time */
    long pid;                   /* pool id */

    redisContext *rc;
    xlock_t lock;
    atomic_t ref;
};

struct redis_pool
{
    struct redis_pool_config rpc;
    /* migth be inserted into g_l2 */
    struct hlist_node hlist;
    struct list_head pool;
    xlock_t plock;

    long pid;
    atomic64_t alloced;
    atomic64_t balance_target;
    int pool_size;

    /* master redis instance info */
    char *redis_host;
    int redis_port;

    /* connect err counter */
    atomic_t master_connect_err;
};

struct redisConnection *getRC(struct redis_pool *rp);

void putRC(struct redis_pool *rp, struct redisConnection *rc);

static inline void __get_rc(struct redisConnection *rc)
{
    atomic_inc(&rc->ref);
}

static inline void __free_rc(struct redisConnection *rc)
{
    xfree(rc);
}

static inline void freeRC(struct redisConnection *rc)
{
    if (rc->rc) {
        redisFree(rc->rc);
        rc->rc = NULL;
    }
}

void __fetch_from_sentinel(struct redis_pool *rp);

struct redis_pool *create_redis_pool(struct redis_pool_config rpc, long pid);

void destroy_redis_pool(struct redis_pool *rp);

int rpool_init_l1(struct redis_pool_config rpc);

int rpool_init_l2();

void rpool_fina_l1();

void rpool_fina_l2();

struct redis_pool *get_l1();

int get_l2(struct redis_pool ***rps, int *pnr);

struct redisConnection *getRC_l1();

struct redisConnection *getRC_l2(char *set, int create);

void putRC_l1(struct redisConnection *rc);

void putRC_l2(struct redisConnection *rc);

void __master_connect_fail_check(struct redis_pool *rp);

/* origin in client.c */
struct MMSConf
{
/* set default copies number to 2 */
#define DUPNUM          2
    int dupnum;
#define MMSCONF_DEDUP   0x01
#define MMSCONF_NODUP   0x02
#define MMSCONF_DUPSET  0x04
    int mode;
    int sockperserver;
    int logdupinfo;
    int redistimeout;
#define MMS_ALLOC_RR    0x00    /* by round-robin */
#define MMS_ALLOC_FR    0x01    /* by free ratio */
    int alloc_policy;
    __timer_cb tcb;
};

struct MMSCSock
{
    struct list_head list;
    int sock;
#define SOCK_FREE               0
#define SOCK_INUSE              1
    int state;
};

struct loadBalanceInfo
{
    u64 total;
    u64 free;
};

struct MMSConnection
{
    long sid;
    char *hostname;

    struct hlist_node hlist;
    
    xlock_t lock;
    struct list_head socks;
    int sock_nr;
    int port;
    time_t ttl;                 /* update ttl to detect removal */
    char *sp;                   /* server:port info from active.mms */
    struct loadBalanceInfo lb;
};

extern struct MMSConf g_conf;
int client_init();
struct MMSConnection *__alloc_mmsc();
struct MMSConnection *__mmsc_lookup(long sid);
int __mmsc_insert(struct MMSConnection *c);

#endif
