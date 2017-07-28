/*
 * Redis Pool and pool selector
 */
#include "mmcc_ll.h"
#include <hiredis/hiredis.h>

extern unsigned int hvfs_mmcc_tracing_flags;

static struct redis_pool *g_l1rp = NULL;
static struct regular_hash *g_l2 = NULL;
static int g_l2_rh_size = 128;
static long g_max_pid = -1;
static long g_last_alloc = 1;

void __fetch_from_sentinel(struct redis_pool *rp)
{
    char *dup = strdup(rp->rpc.uri), *p, *n = NULL, *q, *m = NULL;
    int inited = 0;

    p = dup;
    do {
        p = strtok_r(p, ";", &n);
        if (p) {
            /* parse sentinel server */
            char *rh = NULL;
            int rport;

            q = p;
            q = strtok_r(q, ":", &m);
            if (q) {
                rh = strdup(p);
            } else {
                hvfs_err(mmcc, "parse hostname failed.\n");
                continue;
            }
            q = strtok_r(NULL, ":", &m);
            if (q) {
                rport = atol(q);
            } else {
                hvfs_err(mmcc, "parse port failed.\n");
                continue;
            }
            /* ok, connect to sentinel here */
            {
                redisContext *rc = redisConnect(rh, rport);
                redisReply *r = NULL;

                if (rc->err) {
                    hvfs_err(mmcc, "can't connect to redis sentinel at %s:%d %s\n",
                             rh, rport, rc->errstr);
                    continue;
                }
                r = redisCommand(rc, "SENTINEL get-master-addr-by-name %s", 
                                 rp->rpc.master_name);
                if (r == NULL) {
                    hvfs_err(mmcc, "get master %s failed.\n", rp->rpc.master_name);
                    goto free_rc;
                }
                if (r->type == REDIS_REPLY_NIL || r->type == REDIS_REPLY_ERROR) {
                    hvfs_warning(mmcc, "SENTINEL get master from %s:%d failed, "
                                 "try next one\n", rh, rport);
                }
                if (r->type == REDIS_REPLY_ARRAY) {
                    if (r->elements != 2) {
                        hvfs_warning(mmcc, "Invalid SENTINEL reply? len=%ld\n",
                                     r->elements);
                    } else {
                        if (rp->redis_host == NULL || rp->redis_port == -1) {
                            rp->redis_host = strdup(r->element[0]->str);
                            rp->redis_port = atoi(r->element[1]->str);
                            hvfs_info(mmcc, "OK, got pool master %s @ %s:%d\n",
                                      rp->rpc.master_name, 
                                      rp->redis_host, rp->redis_port);
                            inited = 1;
                        } else {
                            if ((strcmp(r->element[0]->str, rp->redis_host) != 0) ||
                                atoi(r->element[1]->str) != rp->redis_port) {
                                hvfs_info(mmcc, "Change pool master %s from "
                                          "%s:%d to %s:%s\n",
                                          rp->rpc.master_name,
                                          rp->redis_host, rp->redis_port,
                                          r->element[0]->str,
                                          r->element[1]->str);
                                xfree(rp->redis_host);
                                rp->redis_host = strdup(r->element[0]->str);
                                rp->redis_port = atoi(r->element[1]->str);
                                inited = 1;
                            }
                        }
                    }
                }
                freeReplyObject(r);
            free_rc:                
                redisFree(rc);
            }
            xfree(rh);
            if (inited)
                break;
        } else
            break;
    } while (p = NULL, 1);

    xfree(dup);
}

static int init_with_sentinel(struct redis_pool *rp)
{
    int err = 0;

    __fetch_from_sentinel(rp);

    return err;
}

static inline
struct redisConnection *__rp_lookup(struct redis_pool *rp)
{
    struct redisConnection *rc = NULL;

    if (rp) {
        xlock_lock(&rp->plock);
        if (!list_empty(&rp->pool)) {
            /* remove the first entry */
            rc = list_first_entry(&rp->pool, struct redisConnection, list);
            list_del_init(&rc->list);
            rp->pool_size--;
        }
        xlock_unlock(&rp->plock);
    }
    if (rc) atomic_inc(&rc->ref);

    return rc;
}

/*
 * alloc new rc
 */
static struct redisConnection *__alloc_rc(long pid)
{
    struct redisConnection *rc = xzalloc(sizeof(*rc));

    if (rc) {
        rc->born = time(NULL);
        rc->ttl = rc->born;
        rc->pid = pid;
        atomic_set(&rc->ref, 0);
        xlock_init(&rc->lock);
        INIT_LIST_HEAD(&rc->list);
    }

    return rc;
}

/*
 * 0 => inserted
 * 1 => not inserted
 */
static int __rc_insert(struct redis_pool *rp, struct redisConnection *rc)
{
    int inserted = 0;

    xlock_lock(&rp->plock);
    if (rp->pool_size + 1 > rp->rpc.max_conn)
        inserted = 1;
    else {
        list_add_tail(&rc->list, &rp->pool);
        rp->pool_size++;
    }
    xlock_unlock(&rp->plock);

    return inserted;
}

void __rc_remove(struct redisConnection *rc)
{
}

static inline void __put_rc(struct redis_pool *rp, struct redisConnection *rc)
{
    if (atomic_dec_return(&rc->ref) < 0) {
        hvfs_debug(mmcc, "truely free RC %p in pool %ld\n", rc, rp->pid);
        __free_rc(rc);
    } else {
        /* insert back to pool */
        switch (__rc_insert(rp, rc)) {
        case 0:
            /* ok */
            break;
        default:
        case 1:
            /* exceed max conn limit */
            hvfs_debug(mmcc, "Guy, run out max conn limit? pid=%ld "
                       "psize=%d max=%d\n", 
                       rp->pid, rp->pool_size, rp->rpc.max_conn);
            freeRC(rc);
            putRC(rp, rc);
            break;
        }
    }
}

void putRC(struct redis_pool *rp, struct redisConnection *rc)
{
    __put_rc(rp, rc);
}

static int inline __is_l1_pool(struct redis_pool *rp)
{
    return rp->pid == 0;
}

static int update_mmserver(struct redisConnection *rc)
{
    redisReply *rpy = NULL, *_rr = NULL;
    int err = 0;

    rpy = redisCMD(rc->rc, "zrange mm.active 0 -1 withscores");
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i = 0, j;

        /* alloc server list */
        struct MMSConnection *c;

        for (j = 0; j < rpy->elements; j += 2) {
            char *p = rpy->element[j]->str, *n = NULL, *hostname;
            char *q = strdup(rpy->element[j]->str);
            int port;
            long sid;

            /* try to parse hostname to ip address */
            {
                _rr = redisCMD(rc->rc, "hget mm.dns %s", p);
                if (_rr == NULL) {
                    hvfs_warning(mmcc, "try to do dns for '%s' failed %s\n",
                                 p, rc->rc->errstr);
                } else if (_rr->type == REDIS_REPLY_STRING) {
                    hvfs_info(mmcc, "do DNS for %s -> %s\n", p, _rr->str);
                    p = _rr->str;
                }
            }
            
            p = strtok_r(p, ":", &n);
            if (p) {
                hostname = strdup(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer failed, ignore.\n");
                xfree(q);
                if (_rr) freeReplyObject(_rr);
                continue;
            }
            p = strtok_r(NULL, ":", &n);
            if (p) {
                port = atoi(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer port failed, ignore.\n");
                xfree(hostname);
                xfree(q);
                if (_rr) freeReplyObject(_rr);
                continue;
            }
            sid = atol(rpy->element[j + 1]->str);
            
            c = __mmsc_lookup(sid);
            if (!c) {
                c = __alloc_mmsc();
                if (!c) {
                    hvfs_err(mmcc, "alloc MMSC failed, ignore %s:%d\n",
                             hostname, port);
                    xfree(hostname);
                    xfree(q);
                    if (_rr) freeReplyObject(_rr);
                    continue;
                }
                xlock_lock(&c->lock);
                c->ttl = time(NULL);
                xlock_unlock(&c->lock);
            }

            hvfs_info(mmcc, "Update MMServer ID=%ld %s:%d\n", sid,
                      hostname, port);

            xlock_lock(&c->lock);
            xfree(c->hostname);
            xfree(c->sp);
            c->hostname = hostname;
            c->port = port;
            c->sid = sid;
            c->sp = q;
            xlock_unlock(&c->lock);
            
            __mmsc_insert(c);
            i++;
            if (_rr) {
                freeReplyObject(_rr);
                _rr = NULL;
            }
        }
        hvfs_info(mmcc, "Got %d MMServer from Meta Server.\n", i);
    }

    freeReplyObject(rpy);
out:
    return err;
}

struct redisConnection *getRC(struct redis_pool *rp)
{
    struct redisConnection *rc = __rp_lookup(rp);
    struct timeval tv = {
        .tv_sec = g_conf.redistimeout,
        .tv_usec = 0,
    };
    int err = 0;
    
    if (!rc) {
        rc = __alloc_rc(rp->pid);
        if (!rc) {
            hvfs_err(mmcc, "alloc new RC for pool %ld failed\n", rp->pid);
            err = EMMNOMEM;
            goto out;
        }
        rc->rc = redisConnect(rp->redis_host, rp->redis_port);
        if (rc->rc->err) {
            hvfs_err(mmcc, "can't connect to redis master at %s:%d %s\n", 
                     rp->redis_host, rp->redis_port, rc->rc->errstr);
            redisFree(rc->rc);
            rc->rc = NULL;
            err = EMMMETAERR;
            __put_rc(rp, rc);
            __master_connect_fail_check(rp);
            goto out;
        }
        err = redisSetTimeout(rc->rc, tv);
        if (err) {
            hvfs_err(mmcc, "set redis timeout to %d seconds failed w/ %d\n",
                     (int)tv.tv_sec, err);
        }
        atomic_set(&rp->master_connect_err, 0);
        if (__is_l1_pool(rp)) {
            err = update_mmserver(rc);
            if (err) {
                hvfs_err(mmcc, "update_mmserver() failed w/ %d\n", err);
                redisFree(rc->rc);
                __put_rc(rp, rc);
                goto out;
            }
        }
        __get_rc(rc);
    } else {
        if (unlikely(!rc->rc)) {
            /* reconnect to redis server */
        reconnect:
            rc->rc = redisConnect(rp->redis_host, rp->redis_port);
            if (rc->rc->err) {
                hvfs_err(mmcc, "can't connect to redis master at %s:%d %s\n",
                         rp->redis_host, rp->redis_port, rc->rc->errstr);
                redisFree(rc->rc);
                rc->rc = NULL;
                __put_rc(rp, rc);
                err = EMMMETAERR;
                __master_connect_fail_check(rp);
                goto out;
            }
            err = redisSetTimeout(rc->rc, tv);
            if (err) {
                hvfs_err(mmcc, "set redis timeout to %d seconds failed w/ %d\n",
                         (int)tv.tv_sec, err);
            }
            /* update server array */
            if (__is_l1_pool(rp)) {
                err = update_mmserver(rc);
                if (err) {
                    __put_rc(rp, rc);
                    hvfs_err(mmcc, "update_mmserver() failed w/ %d\n", err);
                    goto out;
                }
            }
            atomic_set(&rp->master_connect_err, 0);
        } else {
            /* if ttl larger than 10min, re-test it */
            if (time(NULL) - rc->ttl >= 600) {
                redisReply *rpy = NULL;

                rpy = redisCommand(rc->rc, "ping");
                if (rpy == NULL) {
                    hvfs_err(mmcc, "ping %s:%d failed.\n", 
                             rp->redis_host, rp->redis_port);
                    redisFree(rc->rc);
                    rc->rc = NULL;
                    goto reconnect;
                }
                freeReplyObject(rpy);
            }
        }
        rc->ttl = time(NULL);
    }

out:
    if (err)
        rc = NULL;
    
    return rc;
}

static int init_standalone(struct redis_pool *rpp)
{
    char *dup = strdup(rpp->rpc.uri), *p, *n = NULL;
	char *rh = NULL;
	int rp, err = 0;
    
    p = dup;
    p = strtok_r(p, ":", &n);
    if (p) {
        rh = strdup(p);
    } else {
        hvfs_err(mmcc, "parse hostname failed.\n");
        err = EMMINVAL;
        goto out_free;
    }
    p = strtok_r(NULL, ":", &n);
    if (p) {
        rp = atol(p);
    } else {
        hvfs_err(mmcc, "parse port failed.\n");
        err = EMMINVAL;
        goto out_free;
    }

    hvfs_info(mmcc, "Try to connect to redis server: %s\n", rpp->rpc.uri);
    
    rpp->redis_host = rh;
    rpp->redis_port = rp;

    {
        struct redisConnection *rc = getRC(rpp);
        
        if (!rc) {
            hvfs_err(mmcc, "can't connect to redis at %s:%d\n", rh, rp);
            err = EMMMETAERR;
            goto out_free;
        }
        putRC(rpp, rc);
    }

    xfree(dup);

    return err;

out_free:
    xfree(dup);
    xfree(rh);

    return err;
}

struct redis_pool *create_redis_pool(struct redis_pool_config rpc, long pid)
{
    struct redis_pool *rp = NULL;

    rp = xzalloc(sizeof(*rp));
    if (!rp) {
        hvfs_err(mmcc, "xzalloc() redis_pool failed, no memory.\n");
        return NULL;
    }

    rp->rpc = rpc;
    rp->pid = pid;
    INIT_HLIST_NODE(&rp->hlist);
    INIT_LIST_HEAD(&rp->pool);
    xlock_init(&rp->plock);
    rp->rpc.uri = strdup(rpc.uri);
    rp->rpc.master_name = strdup(rpc.master_name);
    rp->redis_port = -1;

    switch (rpc.ptype) {
    default:
    case RP_CONF_STA:
        init_standalone(rp);
        break;
    case RP_CONF_STL:
        init_with_sentinel(rp);
        break;
    case RP_CONF_CLUSTER:
        break;
    }
    if (pid > g_max_pid)
        g_max_pid = pid;

    hvfs_info(mmcc, "Create redis pool %ld @ %s:%d by (%s)\n",
              pid, rp->redis_host, rp->redis_port, rp->rpc.uri);

    return rp;
}

void destroy_redis_pool(struct redis_pool *rp)
{
    struct redisConnection *pos, *n;
    
    hvfs_info(mmcc, "Destroy pool %ld name %s uri %s psize %d: alloced %ld\n",
              rp->pid, rp->rpc.master_name,
              rp->rpc.uri, rp->pool_size,
              atomic64_read(&rp->alloced));

    xfree(rp->rpc.uri);
    xfree(rp->rpc.master_name);
    xfree(rp->redis_host);
    xlock_lock(&rp->plock);
    list_for_each_entry_safe(pos, n, &rp->pool, list) {
        list_del(&pos->list);
        freeRC(pos);
        __put_rc(rp, pos);
    }
    xlock_unlock(&rp->plock);
    xfree(rp);
}

int rpool_init_l1(struct redis_pool_config rpc)
{
    g_l1rp = create_redis_pool(rpc, 0);
    if (!g_l1rp) {
        hvfs_err(mmcc, "create_redis_pool() type %d uri %s mname %s failed.\n",
                 rpc.ptype, rpc.uri, rpc.master_name);
        return -EINVAL;
    }

    return 0;
}

/*
 * 0 => inserted
 * 1 => not inserted
 */
static int __l2_insert(struct redis_pool *rp)
{
    int idx = rp->pid % g_l2_rh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct redis_pool *n;

    rh = g_l2 + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(n, pos, &rh->h, hlist) {
        if (rp->pid == n->pid) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* ok, insert it to hash table */
        hlist_add_head(&rp->hlist, &rh->h);
    }
    xlock_unlock(&rh->lock);

    return found;
}

static struct redis_pool *__l2_lookup(long pid)
{
    int idx = pid % g_l2_rh_size;
    int found = 0;
    struct redis_pool *rp;
    struct hlist_node *pos;
    struct regular_hash *rh;

    rh = g_l2 + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(rp, pos, &rh->h, hlist) {
        if (pid == rp->pid) {
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (!found)
        return NULL;
    return rp;
}

int rpool_init_l2()
{
    struct redisConnection *rc = NULL;
    int err = 0, i;

    g_l2 = xzalloc(sizeof(*g_l2) * g_l2_rh_size);
    if (!g_l2) {
        hvfs_err(mmcc, "xzalloc() hash table failed, no memory.\n");
        err = EMMNOMEM;
        goto out;
    }

    for (i = 0; i < g_l2_rh_size; i++) {
        xlock_init(&g_l2[i].lock);
        INIT_HLIST_HEAD(&g_l2[i].h);
    }

    /* generate new l2 pool by id, insert it to l2 hash table */
    rc = getRC_l1();
    {
        redisReply *rpy = redisCMD(rc->rc, "hgetall mm.l2");

        if (rpy == NULL) {
            hvfs_err(mmcc, "invalid redis status, connection broken?\n");
            redisFree(rc->rc);
            rc->rc = NULL;
            goto out_put;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "redis error to get mm.l2: %s\n", rpy->str);
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            struct redis_pool_config rpc = {
                .uri = g_l1rp->rpc.uri,
                .master_name = NULL,
                .ptype = g_l1rp->rpc.ptype,
                .max_conn = RP_CONF_MAX_CONN,
                .min_conn = RP_CONF_MIN_CONN,
            };
            struct redis_pool *rp = NULL;
            int i, pid;

            for (i = 0; i < rpy->elements; i+=2) {
                pid = atoi(rpy->element[i]->str);
                if (pid == 0) {
                    hvfs_err(mmcc, "invalid pool id %s for redis HaP %s\n",
                             rpy->element[i]->str,
                             rpy->element[i + 1]->str);
                    err = -EINVAL;
                    goto out_free;
                }
                rpc.master_name = rpy->element[i + 1]->str;
                rp = create_redis_pool(rpc, pid);
                if (!rp) {
                    hvfs_err(mmcc, "create redis pool %d failed\n", pid);
                    err = -EINVAL;
                    goto out_free;
                }
                switch(__l2_insert(rp)) {
                default:
                case 0:
                    /* ok */
                    hvfs_info(mmcc, "insert pool %ld to L2 pools, ok.\n", rp->pid);
                    break;
                case 1:
                    /* someone inserted it yet */
                    hvfs_info(mmcc, "insert pool %ld to L2 pools, conflict.\n",
                              rp->pid);
                    destroy_redis_pool(rp);
                    break;
                }
            }
        }
    out_free:
        freeReplyObject(rpy);
    }
    /* random set last alloc id in [1, g_max_pid] */
    if (g_max_pid > 0)
        g_last_alloc = random() % g_max_pid + 1;
out_put:
    putRC_l1(rc);

out:
    return err;
}

void rpool_fina_l1()
{
    if (g_l1rp)
        destroy_redis_pool(g_l1rp);
}

void rpool_fina_l2()
{
    struct redis_pool *tpos;
    struct hlist_node *pos, *n;
    struct regular_hash *rh;
    int i;
    
    for (i = 0; i < g_l2_rh_size; i++) {
        rh = g_l2 + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(tpos, pos, n, &rh->h, hlist) {
            hlist_del_init(&tpos->hlist);
            hvfs_debug(mmcc, "FINA clean pool %ld \n", tpos->pid);
            destroy_redis_pool(tpos);
        }
        xlock_unlock(&rh->lock);
    }
    xfree(g_l2);
}

struct redis_pool *get_l1()
{
    return g_l1rp;
}

/* Return each pool in L2 pools
 */
int get_l2(struct redis_pool ***rps, int *pnr)
{
    struct redis_pool *rp;
    struct hlist_node *pos;
    struct regular_hash *rh;
    int i, j = 0, err = 0, nr = 0;

    *pnr = 0;
    for (i = 0; i < g_l2_rh_size; i++) {
        rh = g_l2 + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(rp, pos, &rh->h, hlist) {
            nr++;
        }
        xlock_unlock(&rh->lock);
    }
    if (nr <= 0) return -EINVAL;

    *rps = xmalloc(sizeof(struct redis_pool *) * nr);
    if (!*rps) {
        hvfs_err(mmcc, "xmalloc() %d redis_pool pointer failed, no memory",
                 nr);
        err = -ENOMEM;
        goto out;
    }
    for (i = 0; i < g_l2_rh_size; i++) {
        rh = g_l2 + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(rp, pos, &rh->h, hlist) {
            (*rps)[j++] = rp;
            if (j >= nr) break;
        }
        xlock_unlock(&rh->lock);
        if (j >= nr) break;
    }
    *pnr = nr;

out:
    return err;
}

struct redisConnection *getRC_l1()
{
    return getRC(g_l1rp);
}

static inline long __create_set_rr(char *set)
{
    long pid = -1L;

    /* round-robin allocate in [1, g_max_pid] */
    if (g_max_pid < 1) {
        hvfs_err(mmcc, "No active L2 pool? mis-config? max_pid %ld\n", 
                 g_max_pid);
        return pid;
    }
    if (g_last_alloc > g_max_pid)
        g_last_alloc = 1;
    pid = g_last_alloc;
    g_last_alloc++;

    return pid;
}

static long __create_set(char *set)
{
    long pid = __create_set_rr(set);
    struct redisConnection *rc = NULL;
    int need_get = 0;

    if (pid > 0) {
        /* create set indicator to L1 pool */
        rc = getRC_l1();
        if (rc != NULL) {
            redisReply *rpy = redisCMD(rc->rc, "setnx `%s %ld",
                                       set, pid);

            if (rpy == NULL) {
                hvfs_err(mmcc, "invalid redis status, connection broken?\n");
                redisFree(rc->rc);
                rc->rc = NULL;
                pid = -1L;
                goto out_put;
            }
            if (rpy->type == REDIS_REPLY_ERROR) {
                hvfs_err(mmcc, "redis error to set `%s: %s\n", set, rpy->str);
                pid = -1L;
                goto out_free;
            }
            if (rpy->type == REDIS_REPLY_INTEGER) {
                if (rpy->integer == 0) {
                    /* already exists */
                    need_get = 1;
                }
            }
        out_free:
            freeReplyObject(rpy);

            if (need_get) {
                rpy = redisCMD(rc->rc, "get `%s", set);
                if (rpy == NULL) {
                    hvfs_err(mmcc, "invalid redis status, connection broken?\n");
                    redisFree(rc->rc);
                    rc->rc = NULL;
                    pid = -1L;
                    goto out_put;
                }
                if (rpy->type == REDIS_REPLY_ERROR) {
                    hvfs_err(mmcc, "redis error to set `%s: %s\n", set, rpy->str);
                    pid = -1L;
                    goto out_free2;
                }
                pid = -1L;
                if (rpy->type == REDIS_REPLY_STRING) {
                    pid = atol(rpy->str);
                }
            out_free2:
                freeReplyObject(rpy);
            }
        } else {
            pid = -1L;
        }
    out_put:
        putRC_l1(rc);
    }

    return pid;
}

struct redisConnection *getRC_l2(char *set, int create)
{
    struct redisConnection *rc = NULL;
    long pid = -1;
    int err = 0, created = 0;

    /* lookup set in l1 pool to get l2_id */
    rc = getRC_l1();
    {
        redisReply *rpy = redisCMD(rc->rc, "get `%s", set);

        if (rpy == NULL) {
            hvfs_err(mmcc, "invalid redis status, connection broken?\n");
            redisFree(rc->rc);
            rc->rc = NULL;
            err = EMMMETAERR;
            goto out_put;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "redis error to get `%s: %s\n", set, rpy->str);
            err = EMMMETAERR;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            pid = atol(rpy->str);
        }
    out_free:
        freeReplyObject(rpy);
    }
out_put:
    putRC_l1(rc);
    if (err)
        return NULL;

    /* lookup l2 pools by pid(l2_id) */
    if (pid >= 1) {
        /* found it, use it */
        hvfs_debug(mmcc, "lookup pid of set %s -> %ld.\n",
                   set, pid);
    } else if (create) {
        /* create it, use it */
        pid = __create_set(set);
        created = 1;
    } else {
        hvfs_warning(mmcc, "lookup pid of set %s failed, not found.\n",
                     set);
        return NULL;
    }
    if (pid < 1) {
        hvfs_err(mmcc, "invalid pid %ld for set %s\n", pid, set);
        return NULL;
    }
    {
        struct redis_pool *rp = __l2_lookup(pid);

        if (!rp) {
            hvfs_err(mmcc, "find pool %ld by pid failed.\n", pid);
            return NULL;
        }
        if (created)
            atomic64_inc(&rp->alloced);
        rc = getRC(rp);
        if (!rc) {
            hvfs_err(mmcc, "find RC from pool %ld failed.\n", pid);
            return NULL;
        }
    }

    return rc;
}

void putRC_l1(struct redisConnection *rc)
{
    __put_rc(g_l1rp, rc);
}

void putRC_l2(struct redisConnection *rc)
{
    struct redis_pool *rp = NULL;

    /* lookup pool by pid */
    rp = __l2_lookup(rc->pid);
    if (!rp) {
        hvfs_err(mmcc, "putRC() failed, lookup pool %ld failed.\n",
                 rc->pid);
        return;
    }
    /* putRC in the pool */
    __put_rc(rp, rc);
}
