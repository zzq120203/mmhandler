/*
 * Client to interactive with MMS/Redis.
 *
 * 
 */

#include "mmcc_ll.h"
#include "xhash.h"

HVFS_TRACING_INIT();

unsigned int hvfs_mmcc_tracing_flags = HVFS_DEFAULT_LEVEL;

#define SYNCSTORE       1
#define SEARCH          2
#define DELSET          3
#define ASYNCSTORE      4
#define SERVERINFO      5
#define XSEARCH         9

long g_ckpt_ts = -1;
long g_ssid = -1;
pid_t g_tgid = -1;

struct MMSConf g_conf = {
    .dupnum = DUPNUM,
    .mode = MMSCONF_DEDUP,
    .sockperserver = 5,
    .logdupinfo = 1,
    .redistimeout = 60,
    .alloc_policy = MMS_ALLOC_RR,
    .tcb = NULL,
};

/* quick lookup from SID to MMSConnection */
static struct regular_hash *g_rh = NULL;
static int g_rh_size = 32;
static int g_sid_max = 0;

static sem_t g_timer_sem;
static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;
static int g_timer_interval = 30;

time_t g_client_tick = 0;

void do_update();
void __master_connect_fail_check(struct redis_pool *rp);

void mmcc_debug_mode(int enable)
{
    if (enable)
        hvfs_mmcc_tracing_flags = 0xffffffff;
    else
        hvfs_mmcc_tracing_flags = HVFS_DEFAULT_LEVEL;
}

static void *__timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_timer_thread_stop) {
        err = sem_wait(&g_timer_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(mmcc, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);

        do_update();

        /* trigger callbacks */
        if (g_conf.tcb) {
            g_conf.tcb(&cur);
        }
    }

    hvfs_debug(mmcc, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    g_client_tick = time(NULL);

    return;
}

static int setup_timers(int interval)
{
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL;
    int err;

    sem_init(&g_timer_sem, 0, 0);

    err = pthread_create(&g_timer_thread, NULL, &__timer_thread_main,
                         NULL);
    if (err) {
        hvfs_err(mmcc, "Create timer thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    memset(&ac, 0, sizeof(ac));
    sigemptyset(&ac.sa_mask);
    ac.sa_flags = 0;
    ac.sa_sigaction = __itimer_default;
    err = sigaction(SIGALRM, &ac, NULL);
    if (err) {
        err = -errno;
        goto out;
    }
    err = getitimer(which, &pvalue);
    if (err) {
        err = -errno;
        goto out;
    }

    if (!interval) interval = 1; /* trigger it every seconds */
    if (interval > 0) {
        value.it_interval.tv_sec = interval;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = interval;
        value.it_value.tv_usec = 0;
        err = setitimer(which, &value, &ovalue);
        if (err) {
            err = -errno;
            goto out;
        }
        hvfs_debug(mmcc, "OK, we have created a timer thread to "
                   " do scans every %d second(s).\n", interval);
    } else {
        hvfs_debug(mmcc, "Hoo, there is no need to setup itimers based on the"
                   " configration.\n");
        g_timer_thread_stop = 1;
    }
    
out:
    return err;
}

struct __client_r_op
{
    char *opname;
    char *script;
    char *sha;
};

#define __CLIENT_R_OP_DUP_DETECT        0
#define __CLIENT_R_OP_DEREF             1

static struct __client_r_op g_ops[] = {
    {
        "dup_detect",
        "local x = redis.call('hexists', KEYS[1], ARGV[1]); if x == 1 then redis.call('hincrby', '_DUPSET_', KEYS[1]..'@'..ARGV[1], 1); return redis.call('hget', KEYS[1], ARGV[1]); else return nil; end",
    },
    {
        "deref",
        "local x = redis.call('hincrby', '_DUPSET_', KEYS[1]..'@'..ARGV[1], -1); if x < 0 then redis.call('hdel', '_DUPSET_', KEYS[1]..'@'..ARGV[1]); x=redis.call('hdel', KEYS[1], ARGV[1]); else x=-(x+1); end; return x;",
    },
};

int client_config(mmcc_config_t *mc)
{
    int err = 0;

    if (mc->dupnum)
        g_conf.dupnum = mc->dupnum;
    if (mc->tcb)
        g_conf.tcb = mc->tcb;
    if (mc->ti > 0)
        g_timer_interval = mc->ti;
    if (mc->mode > 0)
        g_conf.mode = mc->mode;
    if (mc->sockperserver)
        g_conf.sockperserver = mc->sockperserver;

    return err;
}

int client_init()
{
    int err = 0, i;

    srandom(time(NULL));
    g_tgid = getpid();

    g_rh = xzalloc(sizeof(*g_rh) * g_rh_size);
    if (!g_rh) {
        hvfs_err(mmcc, "xzalloc() hash table failed, no memory.\n");
        err = EMMNOMEM;
        goto out;
    }

    for (i = 0; i < g_rh_size; i++) {
        xlock_init(&g_rh[i].lock);
        INIT_HLIST_HEAD(&g_rh[i].h);
    }

    err = setup_timers(g_timer_interval);
    if (err) {
        hvfs_err(mmcc, "setup_timers() failed w/ %d\n", err);
        goto out_free;
    }

out:
    return err;
out_free:
    xfree(g_rh);
    goto out;
}

int __clean_up_socks(struct MMSConnection *c);

int __client_load_scripts(struct redisConnection *rc, int idx)
{
    redisReply *rpy = NULL;
    int err = 0, i;

    for (i = 0; i < sizeof(g_ops) / sizeof(struct __client_r_op); i++) {
        if (idx == -1 || i == idx) {
            rpy = redisCommand(rc->rc, "script load %s", g_ops[i].script);
            if (rpy == NULL) {
                hvfs_err(mmcc, "read from MM Meta failed: %s(pid: %ld)\n", 
                         rc->rc->errstr, rc->pid);
                freeRC(rc);
                err = -EMMMETAERR;
                goto out;
            }
            if (rpy->type == REDIS_REPLY_ERROR) {
                hvfs_err(mmcc, "script %d load on pool %ld failed w/ \n%s.\n",
                         i, rc->pid, rpy->str);
                err = -EINVAL;
                goto out_free;
            }
            if (rpy->type == REDIS_REPLY_STRING) {
                hvfs_info(mmcc, "[%ld] Script %d %s \tloaded as '%s'.\n",
                          rc->pid, i, g_ops[i].opname, rpy->str);
                if (g_ops[i].sha) xfree(g_ops[i].sha);
                g_ops[i].sha = strdup(rpy->str);
            } else {
                g_ops[i].sha = NULL;
            }
        out_free:
            freeReplyObject(rpy);
        }
    }

out:
    return err;
}

int client_fina()
{
    /* free active redis connections */
    struct MMSConnection *t2pos;
    struct hlist_node *pos, *n;
    struct regular_hash *rh;
    int i;

    /* wait for pending threads */
    g_timer_thread_stop = 1;
    sem_post(&g_timer_sem);
    pthread_join(g_timer_thread, NULL);
    
    /* free active MMS connections */
    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(t2pos, pos, n, &rh->h, hlist) {
            hlist_del_init(&t2pos->hlist);
            hvfs_debug(mmcc, "FINA clean active MMSC for Server %s:%d\n",
                       t2pos->hostname, t2pos->port);
            __clean_up_socks(t2pos);
            xfree(t2pos->hostname);
            xfree(t2pos->sp);
            xfree(t2pos);
        }4.
        xlock_unlock(&rh->lock);
    }
    xfree(g_rh);

    /* free any other global resources, and reset them */
    for (i = 0; i < sizeof(g_ops) / sizeof(struct __client_r_op); i++) {
        if (g_ops[i].sha)
            xfree(g_ops[i].sha);
    }

    return 0;
}

struct MMSConnection *__alloc_mmsc()
{
    struct MMSConnection *c = xzalloc(sizeof(*c));

    if (c) {
        xlock_init(&c->lock);
        INIT_LIST_HEAD(&c->socks);
        INIT_HLIST_NODE(&c->hlist);
    }

    return c;
}

struct MMSConnection *__mmsc_lookup(long sid)
{
    int idx = sid % g_rh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *c;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(c, pos, &rh->h, hlist) {
        if (sid == c->sid) {
            /* ok, found it */
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);
    if (!found) {
        c = NULL;
    }

    return c;
}

struct MMSConnection *__mmsc_lookup_byname(char *host, int port)
{
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *c;
    char sp[256];
    int found = 0, i;
    
    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(c, pos, &rh->h, hlist) {
            sprintf(sp, "%s:%d", host, port);
            if (c->sp && port == c->port && strcmp(sp, c->sp) == 0) {
                /* ok, found it */
                found = 1;
                break;
            }
        }
        xlock_unlock(&rh->lock);
        if (found)
            break;
    }
    if (!found)
        c = NULL;

    return c;
}


/*
 *  0 => inserted
 *  1 => not inserted
 */
int __mmsc_insert(struct MMSConnection *c)
{
    int idx = c->sid % g_rh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *n;

    if (c->sid > g_sid_max)
        g_sid_max = c->sid;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(n, pos, &rh->h, hlist) {
        if (c->sid == n->sid) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* ok, insert it to hash table */
        hlist_add_head(&c->hlist, &rh->h);
    }
    xlock_unlock(&rh->lock);

    return found;
}

void __mmsc_remove(struct MMSConnection *c)
{
    int idx = c->sid % g_rh_size;
    struct regular_hash *rh;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_del_init(&c->hlist);
    xlock_unlock(&rh->lock);
}

void __add_to_mmsc(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    list_add(&s->list, &c->socks);
    c->sock_nr++;
    xlock_unlock(&c->lock);
}

int __clean_up_socks(struct MMSConnection *c)
{
    struct MMSCSock *pos, *n;
    int err = 0;
    
    xlock_lock(&c->lock);
    list_for_each_entry_safe(pos, n, &c->socks, list) {
        if (pos->state == SOCK_INUSE) {
            err = 1;
            break;
        } else {
            list_del(&pos->list);
            close(pos->sock);
            c->sock_nr--;
            xfree(pos);
        }
    }
    xlock_unlock(&c->lock);

    return err;
}

struct MMSCSock *__alloc_mmscsock()
{
    struct MMSCSock *s = xzalloc(sizeof(*s));

    if (s) {
        INIT_LIST_HEAD(&s->list);
    }

    return s;
}

void __free_mmscsock(struct MMSCSock *s) 
{
    xfree(s);
}

void __del_from_mmsc(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    list_del(&s->list);
    c->sock_nr--;
    xlock_unlock(&c->lock);
}

void __put_inuse_sock(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    s->state = SOCK_FREE;
    xlock_unlock(&c->lock);
}

void update_mmserver2(struct redisConnection *rc)
{
    struct MMSConnection *c;
    redisReply *rpy = NULL;
    struct loadBalanceInfo *lba;
    int i;

    lba = alloca(sizeof(struct loadBalanceInfo) * (g_sid_max + 1));
    memset(lba, 0, sizeof(struct loadBalanceInfo) * (g_sid_max + 1));

    for (i = 0; i <= g_sid_max; i++) {
        c = __mmsc_lookup(i);
        if (c) {
            rpy = redisCMD(rc->rc, "exists mm.hb.%s",
                           c->sp);
            if (rpy == NULL) {
                hvfs_err(mmcc, "invalid redis status, connection broken?\n");
                redisFree(rc->rc);
                rc->rc = NULL;
                break;
            }
            if (rpy->type == REDIS_REPLY_INTEGER) {
                if (rpy->integer == 1) {
                    c->ttl = time(NULL);
                    hvfs_debug(mmcc, "Update MMServer %s ttl to %ld\n",
                               c->sp, (u64)c->ttl);
                } else {
                    if (time(NULL) - c->ttl > 360)
                        c->ttl = 0;
                    hvfs_warning(mmcc, "MMServer %s ttl lost, do NOT update (ttl=%ld).\n",
                                 c->sp, c->ttl);
                }
            }
            freeReplyObject(rpy);
        }
    }

    /* get space info for load balance */
    rpy = redisCMD(rc->rc, "hgetall mm.space");
    if (rpy == NULL) {
        hvfs_err(mmcc, "invalid redis status, connection broken?\n");
        redisFree(rc->rc);
        rc->rc = NULL;
        return;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "redis error to get mm.space: %s\n", rpy->str);
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        char *f, *v, *p, *n = NULL;
        int i, j;

        for (i = 0; i < rpy->elements; i+=2) {
            long sid = -1;
            char type = '?';

            f = p = strdup(rpy->element[i]->str);
            v = rpy->element[i + 1]->str;
            for (j = 0; j < 3; j++, p = NULL) {
                p = strtok_r(p, "|", &n);
                if (p != NULL) {
                    switch (j) {
                    case 0:
                        sid = atol(p);
                        break;
                    case 2:
                        type = *p;
                        break;
                    }
                } else {
                    break;
                }
            }
            xfree(f);
            if (sid >= 0 && type != '?') {
                switch (type) {
                case 'T':
                    lba[sid].total += atol(v);
                    break;
                case 'F':
                    lba[sid].free += atol(v);
                    break;
                }
            }
        }
    }
    freeReplyObject(rpy);

    for (i = 0; i <= g_sid_max; i++) {
        c = __mmsc_lookup(i);
        if (c) {
            c->lb.total = lba[i].total;
            c->lb.free = lba[i].free;
        }
    }
}

void update_g_info(struct redisConnection *rc)
{
    redisReply *rpy = NULL;

    /* Step 1: get ssid */
    rpy = redisCMD(rc->rc, "get mm.ss.id");
    if (rpy == NULL) {
        hvfs_err(mmcc, "invalid redis status, connection broken?\n");
        redisFree(rc->rc);
        rc->rc = NULL;
        return;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        long _ssid = atol(rpy->str);

        if (_ssid != g_ssid) {
            hvfs_info(mmcc, "MM SS Server change from %ld to %ld.\n",
                      g_ssid, _ssid);
            g_ssid = _ssid;
        }
    }
    freeReplyObject(rpy);

    /* Step 2: get ckpt_ts */
    rpy = redisCMD(rc->rc, "get mm.ckpt.ts");
    if (rpy == NULL) {
        hvfs_err(mmcc, "invalid redis status, connection broken?\n");
        redisFree(rc->rc);
        rc->rc = NULL;
        return;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        long _ckpt = atol(rpy->str);

        if (_ckpt > g_ckpt_ts) {
            hvfs_info(mmcc, "MM CKPT TS change from %ld to %ld\n",
                      g_ckpt_ts, _ckpt);
            g_ckpt_ts = _ckpt;
        } else if (_ckpt < g_ckpt_ts) {
            hvfs_warning(mmcc, "Detect MM CKPT TS change backwards"
                         "(cur %ld, got %ld).\n",
                         g_ckpt_ts, _ckpt);
        }
    }
    freeReplyObject(rpy);
}

void do_update()
{
    struct redisConnection *rc = NULL;
    static time_t last_check = 0;
    static time_t last_fetch = 0;
    time_t cur;

    rc = getRC_l1();
    if (!rc) return;

    cur = time(NULL);
    if (cur >= last_check + 30) {
        update_mmserver2(rc);
        last_check = cur;
    }

    if (cur >= last_fetch + 10) {
        update_g_info(rc);
        last_fetch = cur;
    }

    putRC_l1(rc);
}

int do_connect(struct MMSConnection *c)
{
    struct MMSCSock *s = NULL;
	int sock = socket(AF_INET, SOCK_STREAM, 0);
    int err = 0;

	if (sock == -1) {
		hvfs_err(mmcc, "failed to create a socket!\n");
		return EMMCONNERR;
	}
    s = __alloc_mmscsock();
    if (!s) {
		hvfs_err(mmcc, "failed to create a MMSCSock!\n");
		return EMMNOMEM;
    }
    
	struct hostent *hptr = gethostbyname(c->hostname);

	if (hptr == NULL) {
        hvfs_err(mmcc, "resolve nodename:%s failed!\n", c->hostname);
        err = EMMMETAERR;
        goto out_free;
	}

	struct sockaddr_in dest_addr;

	dest_addr.sin_family = AF_INET;
	dest_addr.sin_port = htons(c->port);
	dest_addr.sin_addr.s_addr = ((struct in_addr *)(hptr->h_addr))->s_addr;
	bzero(&(dest_addr.sin_zero), 8);
    
	int cr = connect(sock, (struct sockaddr*)&dest_addr, sizeof(struct sockaddr));	

	if (cr == -1) {
        hvfs_err(mmcc, "connect to server %s:%d failed\n", c->hostname, c->port);
        err = EMMCONNERR;
        goto out_free;
	}
	
    s->sock = sock;
    s->state = SOCK_FREE;
    __add_to_mmsc(c, s);
    hvfs_info(mmcc, "Create new connection for MMS Server ID=%ld.\n",
              c->sid);

    return 0;
out_free:
    __free_mmscsock(s);
    return err;
}

struct MMSCSock *__get_free_sock(struct MMSConnection *c)
{
    struct MMSCSock *pos = NULL;
    int found = 0;
    int retry = 0;

    do {
        if (unlikely(retry >= 10)) {
            pos = NULL;
            break;
        }

        xlock_lock(&c->lock);
        list_for_each_entry(pos, &c->socks, list) {
            if (pos->state == SOCK_FREE) {
                found = 1;
                pos->state = SOCK_INUSE;
                break;
            }
        }
        xlock_unlock(&c->lock);
        
        if (!found) {
            if (c->sock_nr < g_conf.sockperserver) {
                /* new connection */
                pos = __alloc_mmscsock();

                if (pos) {
                    int sock = socket(AF_INET, SOCK_STREAM, 0);
                    int port;
                    char *hostname;
                    struct hostent *hptr;
                    struct sockaddr_in dest_addr;
                    int err = 0;

                    if (sock < 0) {
                        int n = 2;

                        hvfs_err(mmcc, "create socket failed w/ %s(%d) (retry=%d)\n",
                                 strerror(errno), errno, retry);
                        __free_mmscsock(pos);
                        do {
                            n = sleep(n);
                        } while (n);
                        retry++;
                        continue;
                    }

                    {
                        int i = 1;
                        err = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, 
                                         (void *)&i, sizeof(i));
                        if (err) {
                            hvfs_warning(mmcc, "setsockopt TCP_NODELAY failed w/ %s\n",
                                         strerror(errno));
                        }
                    }

                    xlock_lock(&c->lock);
                    hostname = strdup(c->hostname);
                    port = c->port;
                    xlock_unlock(&c->lock);
                    
                    hptr = gethostbyname(hostname);
                    if (hptr == NULL) {
                        hvfs_err(mmcc, "resolve nodename:%s failed! (retry=%d)\n", 
                                 hostname, retry);
                        __free_mmscsock(pos);
                        sleep(1);
                        retry++;
                        xfree(hostname);
                        continue;
                    }
                    xfree(hostname);

                    dest_addr.sin_family = AF_INET;
                    dest_addr.sin_port = htons(port);
                    dest_addr.sin_addr.s_addr = ((struct in_addr *)
                                                 (hptr->h_addr))->s_addr;
                    bzero(&(dest_addr.sin_zero), 8);

                    err = connect(sock, (struct sockaddr *)&dest_addr, 
                                  sizeof(struct sockaddr));
                    if (err) {
                        hvfs_err(mmcc, "Connection to server %ld failed w/ %s(%d) "
                                 "(retry=%d).\n",
                                 c->sid, strerror(errno), errno, retry);
                        __free_mmscsock(pos);
                        sleep(1);
                        retry++;
                        continue;
                    }
                    pos->sock = sock;
                    pos->state = SOCK_INUSE;
                    __add_to_mmsc(c, pos);
                    hvfs_info(mmcc, "Create new connection for MMS Server ID=%ld.\n",
                              c->sid);
                    break;
                }
            } else {
                sched_yield();
            }
        } else
            break;
    } while (1);

    /* FIXME: should we fast failed on connection error? set c->ttl to ZERO?
     */

    return pos;
}

void __free_mmsc(struct MMSConnection *c)
{
    xfree(c);
}

int recv_bytes(int sockfd, void *buf, int n)
{
	int i = 0;

    do {
		int a = recv(sockfd, buf + i, n - i, 0);

		if (a == -1) {
			return EMMCONNERR;
		}
		i += a;
		
		if (a == 0 && i < n) {
            hvfs_debug(mmcc, "recv EOF\n");
            return EMMCONNERR;
		}
    } while (i < n);

    return 0;
}

int send_bytes(int sockfd, void *buf, int n)
{
	int i = 0;
    
    do {
		int a = send(sockfd, buf + i, n - i, MSG_NOSIGNAL);
		if (a == -1) {
            return EMMCONNERR;
		}
		i += a;
    } while (i < n);

    return 0;
}

int recv_int(int sockfd) 
{
	int bytes, err = 0;

    err = recv_bytes(sockfd, &bytes, 4);
    if (err) {
        hvfs_err(mmcc, "recv_bytes() for length failed w/ %d.\n", err);
        goto out;
    }

    err = ntohl(bytes);
out:
    return err;
}

int send_int(int sockfd, int num)
{
    int bytes;

    bytes = htonl(num);
    
	return send_bytes(sockfd, &bytes, 4);
}

/* 
 * type@set@serverid@block@offset@length@disk
 */
int search_by_info(char *info, void **buf, size_t *length)
{
    /* split the info by @ */
    char *p = strdup(info), *n = NULL;
    char *x = p;
    long sid = -1;
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0, i;

    for (i = 0; i < 3; i++, p = NULL) {
        p = strtok_r(p, "@", &n);
        if (p != NULL) {
            switch (i) {
            case 2:
                /* serverId */
                sid = atol(p);
                break;
            default:
                break;
            }
        } else {
            err = EMMINVAL;
            goto out;
        }
    }
    xfree(x);

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = EMMINVAL;
        goto out;
    }
    if (c->ttl <= 0) {
        err = EMMCONNERR;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        char header[4] = {
            (char)SEARCH,
            (char)strlen(info),
        };
        int count;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed, connection broken?\n");
            goto out_disconn;
        }
        
        err = send_bytes(s->sock, info, strlen(info));
        if (err) {
            hvfs_err(mmcc, "send info failed, connection broken?\n");
            goto out_disconn;
        }
	
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMNOTFOUND;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }
        
        *buf = xmalloc(count);
        if (!*buf) {
            hvfs_err(mmcc, "xmalloc() buffer %dB failed.\n", count);
            err = EMMNOMEM;
            goto out_disconn;
        }
        *length = (size_t)count;

        err = recv_bytes(s->sock, *buf, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed, "
                     "connection broken?\n", count);
            xfree(*buf);
            goto out_disconn;
        }
    }

out_put:    
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

int search_mm_object(char *infos, void **buf, size_t *length)
{
    char *p = infos, *n = NULL;
    int err = 0, found = 0;
#ifdef DEBUG_LATENCY
    struct timeval tv;
    double begin, end;

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec * 1000000.0 + tv.tv_usec;
#endif

    /* Note: We should random read from backend servers. But if # list had
     * already randomized in mmcc_put stage, thus here we can read by
     * sequence.
     */
    do {
        p = strtok_r(p, "#", &n);
        if (p != NULL) {
            /* handle this info */
            int __retry = 1;
        retry:
            err = search_by_info(p, buf, length);
            if (err) {
                hvfs_err(mmcc, "search_by_info(%s) failed %d(%s)\n",
                         p, err, mmcc_strerr(err));
                /* Note that, for long running system, some socket ends by not
                 * freed, we should do another retry to reconnect.
                 */
                if (err == EMMCONNERR && __retry) {
                    __retry = 0;
                    goto retry;
                }
            } else {
                found = 1;
                break;
            }
        } else {
            break;
        }
    } while (p = NULL, 1);

#ifdef DEBUG_LATENCY
    gettimeofday(&tv, NULL);
    end = tv.tv_sec * 1000000.0 + tv.tv_usec;

    {
        static int xx = 0;
        static double acc = 0.0;
        
        xx++;
        acc += (end - begin);
        if (xx % 100 == 0)
            hvfs_info(mmcc, "search mm obj AVG latency %lf us\n", acc / xx);
    }
#endif

    if (!found)
        return EMMNOTFOUND;
    else
        return 0;
}

int get_mm_object(char *set, char *md5, void **buf, size_t *length)
{
	redisReply* reply = NULL;
    struct redisConnection *rc = NULL;
    int err = 0;

    if (!set || !md5 || !buf || !length)
        return EMMINVAL;
    
    rc = getRC_l2(set, 0);
    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed for %s@%s\n", set, md5);
        return EMMMETAERR;
    }
    
    reply = redisCMD(rc->rc, "hget %s %s", set, md5);

	if (reply == NULL) {
        hvfs_err(mmcc, "hget %s %s from MM Meta failed: %s\n",
                 set, md5, rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
	}

	if (reply->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s@%s does not exist on MM Meta server.\n", set, md5);
        err = EMMNOTFOUND;
        goto out_free;
	}
    
    if (reply->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "hget %s %s failed w/ %s\n", set, md5, reply->str);
        err = EMMMETAERR;
        goto out_free;
    }

    err = search_mm_object(reply->str, buf, length);
    if (err) {
        hvfs_err(mmcc, "search_mm_object failed w/ %d(%s)\n", 
                 err, mmcc_strerr(err));
        goto out_free;
    }

out_free:        
    freeReplyObject(reply);
out:
    putRC_l2(rc);

    return err;
}

struct key2info *get_k2i(char *set, int *nr)
{
    redisReply *rpy = NULL;
    struct key2info *ki = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l2(set, 0);

    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed for set %s\n", set);
        return NULL;
    }
    
    rpy = redisCMD(rc->rc, "hgetall %s", set);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }

    if (rpy->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s does not exist on MM Meta server.\n", set);
        err = EMMNOTFOUND;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i, j;

        *nr = rpy->elements / 2;
        ki = xzalloc(*nr * sizeof(*ki));
        if (!ki) {
            hvfs_err(mmcc, "alloc k2i failed, no memory.\n");
            err = EMMNOMEM;
            goto out_free;
        }
        
        for (i = 0, j = 0; i < rpy->elements; i+=2, j++) {
            ki[j].key = strdup(rpy->element[i]->str);
            ki[j].info = strdup(rpy->element[i + 1]->str);
        }
    }

out_free:
    freeReplyObject(rpy);
out:
    putRC_l2(rc);

    if (err)
        ki = NULL;
    
    return ki;
}

static int do_put(long sid, char *set, char *name, void *buffer, 
                  size_t len, char **out)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0;

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = -EINVAL;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed for Server ID=%ld.\n", sid);
        err = EMMCONNERR;
        goto out;
    }

    {
        int count = len, slen = strlen(set), nlen = strlen(name);
        char header[4] = {
            (char)SYNCSTORE,
            (char)slen,
            (char)nlen,
        };
//        storeos.write(header);
//        storeos.writeInt(content.length);
//        storeos.writeLong(fLen);
//        //文件名的长度
//        if(fn == null){
//            storeos.writeInt(0);
//        }else{
//            storeos.writeInt(fn.length());
//        }
//        // set,md5,content的实际内容写过去
//        storeos.write(set.getBytes());
//        storeos.write(md5.getBytes());
//        storeos.write(content);
//        //文件名
//        if(fn != null){
//            storeos.write(fn.getBytes());
//        }
//        storeos.flush();
        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_int(s->sock, count);
        if (err) {
            hvfs_err(mmcc, "send length failed, connection broken?\n");
            goto out_disconn;
        }
        /*没有含义，为了同包含大文件的java客户端的参数一致*/
        err = send_long(s->sock,count);
        if (err) {
            hvfs_err(mmcc, "send length failed, connection broken?\n");
            goto out_disconn;
        }
        /*同上*/
        err = send_int(s->sock,0);
        if (err) {
            hvfs_err(mmcc, "send length failed, connection broken?\n");
            goto out_disconn;
        }


        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, name, nlen);
        if (err) {
            hvfs_err(mmcc, "send name (or md5) failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, buffer, count);
        if (err) {
            hvfs_err(mmcc, "send data content failed, connection broken?\n");
            goto out_disconn;
        }

        /* ok, wait for reply now */
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMINVAL;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }

        if (!*out) {
            *out = xzalloc(count + 1);
            if (!*out) {
                hvfs_err(mmcc, "xmalloc() buffer %dB failed.", count + 1);
                err = EMMNOMEM;
                goto out_disconn;
            }
        }

        err = recv_bytes(s->sock, *out, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed, "
                     "connection broken?\n",
                     count);
            xfree(*out);
            goto out_disconn;
        }

        if (strncmp(*out, "#FAIL:", 6) == 0) {
            err = EMMMETAERR;
            goto out_put;
        }
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

static int do_put_iov(long sid, char *set, char *name, struct iovec *iov,
                      int iovlen, char **out)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0, tlen = 0, i;

    for (i = 0; i < iovlen; i++) {
        tlen += iov[i].iov_len;
    }

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = EMMINVAL;
        goto out;
 }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        int count = tlen, slen = strlen(set), nlen = strlen(name);
        char header[4] = {
            (char)SYNCSTORE,
            (char)slen,
            (char)nlen,
        };

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_int(s->sock, count);
        if (err) {
            hvfs_err(mmcc, "send length failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, name, nlen);
        if (err) {
            hvfs_err(mmcc, "send name (or md5) failed.\n");
            goto out_disconn;
        }

        for (i = 0; i < iovlen; i++) {
            err = send_bytes(s->sock, iov[i].iov_base, iov[i].iov_len);
            if (err) {
                hvfs_err(mmcc, "send content iov[%d] len=%ld failed, "
                         "connection broken?\n",
                         i, (u64)iov[i].iov_len);
                goto out_disconn;
            }
        }

        /* ok, wait for reply now */
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMINVAL;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }

        if (!*out) {
            *out = xzalloc(count + 1);
            if (!*out) {
                hvfs_err(mmcc, "xmalloc() buffer %dB failed.", count + 1);
                err = EMMNOMEM;
                goto out_disconn;
            }
        }

        err = recv_bytes(s->sock, *out, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed, "
                     "connection broken?\n",
                     count);
            xfree(*out);
            goto out_disconn;
        }

        if (strncmp(*out, "#FAIL:", 6) == 0) {
            err = EMMMETAERR;
            goto out_put;
        }
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

static void __random_sort_array(int *a, int nr)
{
    int swap, i, j;

    if (nr <= 1) return;

    for (i = 0; i < (nr + 1) / 2; i++) {
        j = random() % nr;
        if (i != j) {
            swap = a[i];
            a[i] = a[j];
            a[j] = swap;
        }
    }
}

static int fratio_compare(const void *a, const void *b)
{
    struct MMSConnection *x = (struct MMSConnection *)a;
    struct MMSConnection *y = (struct MMSConnection *)b;

    if ((double)x->lb.free / x->lb.total > (double)y->lb.free / y->lb.total)
        return 1;
    else if ((double)x->lb.free / x->lb.total < (double)y->lb.free / y->lb.total)
        return -1;
    else
        return 0;
}

static void __sort_array_by_fratio(struct MMSConnection **a, int nr)
{
    return qsort(a, nr, sizeof(struct MMSConnection *), fratio_compare);
}

static void __sort_array_by_rr(struct MMSConnection **a, int nr)
{
    struct MMSConnection *_t = NULL;
    static int last_idx = 0;

    if (last_idx >= nr) last_idx = 0;
    /* swap 0 and last_idx */
    _t = a[0];
    a[0] = a[last_idx];
    a[last_idx] = _t;
    last_idx++;
}

static int __select_mms(int *ids, int dupnum)
{
    time_t cur = time(NULL);
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *n;
    int i, c = 0, k = 0, total = 0, err = 0;
    struct MMSConnection **tids = NULL;

    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist) {
            if (cur - n->ttl < 300)
                total++;
        }
        xlock_unlock(&rh->lock);
    }
    if (total == 0) {
        hvfs_warning(mmcc, "No valid MMServer to write before select?\n");
        return EMMNOMMS;
    }

    if (dupnum > total)
        dupnum = total;

    tids = alloca(sizeof(struct MMSConnection *) * total);

    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist)
        {
            if (cur - n->ttl < 120 && n->lb.free > 0) {
                /* save it  */
                tids[c] = n;
                c++;
            }
        }
        xlock_unlock(&rh->lock);
        if (c >= total) break;
    }

    /* sort the array by free ratio as needed */
    if (c > dupnum) {
        switch (g_conf.alloc_policy) {
        default:
        case MMS_ALLOC_RR:
            __sort_array_by_rr(tids, c);
            break;
        case MMS_ALLOC_FR:
            __sort_array_by_fratio(tids, c);
        }
    }

    /* copy top dupnum id to result array */
    for (i = 0; i < c; i++) {
        ids[k] = tids[i]->sid;
        k++;
        if (k >= dupnum) break;
    }

    /* random sort the ids array */
    __random_sort_array(ids, k);

    hvfs_debug(mmcc, "total %d c %d k %d dupnum %d\n", total, c, k, dupnum);

    err = k;

    return err;
}

int __mmcc_put(char *set, char *name, void *buffer, size_t len,
               int dupnum, char **info)
{
    int i, c = 0, *ids = NULL, err = 0;

    ids = alloca(sizeof(int) * dupnum);

    err = __select_mms(ids, dupnum);
    if (err < 0) {
        hvfs_err(mmcc, "__select_mm() for %s@%s dupnum=%d failed w/ %d\n",
                 set, name, dupnum, err);
        goto out;
    }
    c = err;
    if (c <= 0) {
        hvfs_warning(mmcc, "No valid MMServer to write after select?\n");
        return EMMNOMMS;
    }

    hvfs_debug(mmcc, "Got active servers = %d to put for %s@%s\n", 
               c, set, name);

    /* ok, finally call do_put() */
    for (i = 0; i < c; i++) {
        char *out = NULL;
        int lerr = 0;
        
        lerr = do_put(ids[i], set, name, buffer, len, &out);
        if (lerr) {
            hvfs_err(mmcc, "do_put() on Server %d failed w/ %d(%s),"
                     " ignore\n",
                     ids[i], lerr, out);
            if (!err)
                err = lerr;
        } else {
            err = -1;
        }
        if (out) {
            xfree(*info);
            *info = out;
        }
    }
    if (err == -1)
        err = 0;

    /* get the info now */
    if (*info == NULL) {
        redisReply *rpy = NULL;
        struct redisConnection *rc = getRC_l2(set, 0);

        if (!rc) {
            hvfs_err(mmcc, "getRC_l2() failed for %s@%s\n", set, name);
            goto out;
        }
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            goto local_out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "%s/%s does not exist or internal error.\n",
                     set, name);
            goto local_out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            char *_t = NULL;
            int ilen = strlen(rpy->str) + 1;
            
            _t = xzalloc(ilen);
            if (!_t) {
                hvfs_err(mmcc, "alloc %dB failed for info, no memory.\n",
                         ilen);
            } else {
                xfree(*info);
                *info = _t;
                memcpy(_t, rpy->str, ilen - 1);
            }
            
        }
    local_out_free:
        freeReplyObject(rpy);
    local_out:
        putRC_l2(rc);
    }

out:
    return err;
}

int __mmcc_put_iov(char *set, char *name, struct iovec *iov, int iovlen,
                   int dupnum, char **info)
{
    int i, c = 0, *ids = NULL, err = 0;

    ids = alloca(sizeof(int) * dupnum);

    err = __select_mms(ids, dupnum);
    if (err < 0) {
        hvfs_err(mmcc, "__select_mm() for %s@%s dupnum=%d failed w/ %d\n",
                 set, name, dupnum, err);
        goto out;
    }
    c = err;
    if (c <= 0) {
        hvfs_warning(mmcc, "No valid MMServer to write after select?\n");
        return EMMNOMMS;
    }

    hvfs_debug(mmcc, "Got active servers = %d to put for %s@%s\n", 
               c, set, name);

    /* ok, finally call do_put_iov() */
    for (i = 0; i < c; i++) {
        char *out = NULL;
        int lerr = 0;
        
        lerr = do_put_iov(ids[i], set, name, iov, iovlen, &out);
        if (lerr) {
            hvfs_err(mmcc, "do_put() on Server %d failed w/ %d, ignore\n",
                     ids[i], lerr);
            if (!err)
                err = lerr;
        } else {
            err = -1;
        }
        if (*info == NULL)
            *info = out;
        else
            xfree(out);
    }
    if (err == -1)
        err = 0;

    /* get the info now */
    {
        redisReply *rpy = NULL;
        struct redisConnection *rc = getRC_l2(set, 0);

        if (!rc) {
            hvfs_err(mmcc, "getRC_l2() failed for %s@%s\n", set, name);
            goto out;
        }
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            goto local_out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "%s@%s does not exist or internal error(%s).\n",
                     set, name, rpy->str);
            goto local_out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            char *_t = NULL;
            int ilen = strlen(rpy->str) + 1;
            
            _t = xzalloc(ilen);
            if (!_t) {
                hvfs_err(mmcc, "alloc %dB failed for info, no memory.\n",
                         ilen);
            } else {
                xfree(*info);
                *info = _t;
                memcpy(_t, rpy->str, ilen - 1);
            }
            
        }
    local_out_free:
        freeReplyObject(rpy);
    local_out:
        putRC_l2(rc);
    }

out:
    return err;
}

/* Return value: 
 * 0   -> not duplicated; 
 * > 0 -> duplicated N times;
 * < 0 -> error
 */
static int __dup_detect(char *set, char *name, char **info)
{
    redisReply *rpy = NULL;
    struct redisConnection *rc = NULL;
    int err = 0;

    if (g_conf.mode & MMSCONF_NODUP) {
        return 0;
    }
    rc = getRC_l2(set, 1);
    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed on set %s\n", set);
        err = EMMMETAERR;
        goto out;
    }
    if ((g_conf.mode & MMSCONF_DUPSET) &&
        (g_conf.mode & MMSCONF_DEDUP))
        rpy = redisCMD(rc->rc, "evalsha %s 1 %s %s",
                       g_ops[__CLIENT_R_OP_DUP_DETECT].sha,
                       set, 
                       name);
    else
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMINVAL;
        freeRC(rc);
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_NIL) {
        /* no existing key, it is ok */
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_warning(mmcc, "%s@%s does not exist or internal error(%s).\n",
                     set, name, rpy->str);
        if (rpy->str != NULL && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __client_load_scripts(rc, __CLIENT_R_OP_DUP_DETECT);
            if (err) {
                hvfs_err(mmcc, "try to load script %s failed: %d\n",
                         g_ops[__CLIENT_R_OP_DUP_DETECT].opname, err);
            }
        }
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        char *p = rpy->str;
        int dupnum = 0;

        /* count # now */
        while (*p != '\0') {
            if (*p == '#')
                dupnum++;
            p++;
        }
        err = dupnum + 1;
        *info = strdup(rpy->str);
    }
    
out_free:
    freeReplyObject(rpy);
out_put:
    putRC_l2(rc);
out:
    return err;
}

/* Return value:
 *
 * 1     ->  deleted
 * 0     ->  no ref, not exist
 * <0    ->  still has other reference
 */
int mmcc_del(char *key)
{
    char *set = NULL, *name = NULL;
    redisReply *rpy = NULL;
    struct redisConnection *rc = NULL;
    int err = 0;

    /* split by @ */
    {
        char *p = strdup(key), *n = NULL;
        char *q = p;
        int i = 0;

        for (i = 0; i < 2; i++, p = NULL) {
            p = strtok_r(p, "@", &n);
            if (p != NULL) {
                switch (i) {
                case 0:
                    set = strdup(p);
                    break;
                case 1:
                    name = strdup(p);
                    break;
                }
            } else {
                goto out_free_q;
            }
        }
    out_free_q:
        xfree(q);
    }

    rc = getRC_l2(set, 1);
    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed on set %s\n", set);
        err = EMMMETAERR;
        goto out;
    }

    if ((g_conf.mode & MMSCONF_DUPSET) &&
        (g_conf.mode & MMSCONF_DEDUP)) {
        /* detect with deref */
        rpy = redisCMD(rc->rc, "evalsha %s 1 %s %s",
                       g_ops[__CLIENT_R_OP_DEREF].sha,
                       set, name);
    } else {
        /* simaple hdel */
        rpy = redisCMD(rc->rc, "hdel %s %s", set, name);
    }
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMINVAL;
        freeRC(rc);
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_warning(mmcc, "%s@%s does not exist or internal error(%s).\n",
                     set, name, rpy->str);
        if (rpy->str != NULL && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __client_load_scripts(rc, __CLIENT_R_OP_DEREF);
            if (err) {
                hvfs_err(mmcc, "try to load script %s failed: %d\n",
                         g_ops[__CLIENT_R_OP_DEREF].opname, err);
            } else {
                err = -EAGAIN;
            }
        }
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        err = rpy->integer;
        hvfs_debug(mmcc, "delete %s@%s w/ %d\n", set, name, err);
    }

out_free:
    freeReplyObject(rpy);
out_put:
    putRC_l2(rc);
out:
    return err;
}

static int __log_dupinfo(char *set, char *name)
{
    redisReply *rpy = NULL;
    struct redisConnection *rc = getRC_l2(set, 0);
    int err = 0;

    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        err = EMMMETAERR;
        goto out;
    }
    rpy = redisCMD(rc->rc, "hincrby mm.dedup.info %s@%s 1",
                   set, name);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMINVAL;
        freeRC(rc);
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "%s@%s does not exist or internal error.\n",
                 set, name);
        err = EMMINVAL;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        /* it is ok, ignore result */
        ;
    }

out_free:
    freeReplyObject(rpy);
out_put:
    putRC_l2(rc);
out:
    return err;
}

void mmcc_put_R(char *key, void *content, size_t len, struct mres *mr)
{
    char *info = NULL, *set = NULL, *name = NULL;
    int err = 0;

    memset(mr, 0, sizeof(*mr));
    if (unlikely(!key || !content || len <= 0))
        return;

    /* split by @ */
    {
        char *p = strdup(key), *n = NULL;
        char *q = p;
        int i = 0;

        for (i = 0; i < 2; i++, p = NULL) {
            p = strtok_r(p, "@", &n);
            if (p != NULL) {
                switch (i) {
                case 0:
                    set = strdup(p);
                    break;
                case 1:
                    name = strdup(p);
                    break;
                }
            } else {
                goto out_free;
            }
        }
    out_free:
        xfree(q);
    }

    /* dup detection */
    err = __dup_detect(set, name, &info);
    if (err > 0) {
        mr->flag |= MR_FLAG_DUPED;
        /* do logging */
        if (g_conf.logdupinfo) {
            __log_dupinfo(set, name);
        }
        /* if larger than dupnum, then do NOT put */
        if (err >= g_conf.dupnum) {
            hvfs_debug(mmcc, "DETECT dupnum=%d >= %d, do not put actually for "
                       "%s@%s.\n",
                       err, g_conf.dupnum, set, name);
            goto out_free2;
        }
    }

    err = __mmcc_put(set, name, content, len, g_conf.dupnum, &info);
    if (err) {
        hvfs_err(mmcc, "__mmcc_put() %s %s failed w/ %d\n",
                 set, name, err);
        goto out_free2;
    }
    hvfs_debug(mmcc, "mmcc_put() key=%s info=%s\n",
               key, info);

out_free2:
    xfree(set);
    xfree(name);

    mr->info = info;
}

char *mmcc_put(char *key, void *content, size_t len)
{
    struct mres mr;

    mmcc_put_R(key, content, len, &mr);

    return mr.info;
}

void mmcc_put_iov_R(char *key, struct iovec *iov, int iovlen, struct mres *mr)
{
    char *info = NULL, *set = NULL, *name = NULL;
    int err = 0;

    memset(mr, 0, sizeof(*mr));
    if (!key || !iov || iovlen <= 0)
        return;

    /* split by @ */
    {
        char *p = strdup(key), *n = NULL;
        char *q = p;
        int i = 0;

        for (i = 0; i < 2; i++, p = NULL) {
            p = strtok_r(p, "@", &n);
            if (p != NULL) {
                switch (i) {
                case 0:
                    set = strdup(p);
                    break;
                case 1:
                    name = strdup(p);
                    break;
                }
            } else {
                goto out_free;
            }
        }
    out_free:
        xfree(q);
    }

    /* dup detection */
    err = __dup_detect(set, name, &info);
    if (err > 0) {
        mr->flag |= MR_FLAG_DUPED;
        /* do logging */
        if (g_conf.logdupinfo) {
            __log_dupinfo(set, name);
        }
        /* if larger than dupnum, then do NOT put */
        if (err >= g_conf.dupnum) {
            hvfs_debug(mmcc, "DETECT dupnum=%d >= %d, do not put actually.\n",
                       err, g_conf.dupnum);
            goto out_free2;
        }
    }

    err = __mmcc_put_iov(set, name, iov, iovlen, g_conf.dupnum, &info);
    if (err) {
        hvfs_err(mmcc, "__mmcc_put() %s %s failed w/ %d\n",
                 set, name, err);
        goto out_free2;
    }
    hvfs_debug(mmcc, "mmcc_put() key=%s info=%s\n",
               key, info);

out_free2:
    xfree(set);
    xfree(name);

    mr->info = info;
}

char *mmcc_put_iov(char *key, struct iovec *iov, int iovlen)
{
    struct mres mr;

    mmcc_put_iov_R(key, iov, iovlen, &mr);

    return mr.info;
}

int __del_mm_set(char *host, int port, char *set)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0;

    c = __mmsc_lookup_byname(host, port);
    if (!c) {
        hvfs_err(mmcc, "lookup Server %s:%d failed.\n", host, port);
        err = -EINVAL;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed for Server %s:%d.\n",
                 host, port);
        err = EMMCONNERR;
        goto out;
    }

    {
        int slen = strlen(set);
        char header[4] = {
            (char)DELSET,
            (char)slen,
        };
        char r = 0;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed, connection broken?\n");
            goto out_disconn;
        }

        /* ok, wait for reply now */
        err = recv_bytes(s->sock, &r, 1);
        if (err) {
            hvfs_err(mmcc, "recv_bytes result byte failed, "
                     "connection broken?\n");
            goto out_disconn;
        }

        if (r != 1) {
            hvfs_err(mmcc, "delete set %s failed on server %s:%d\n",
                     set, host, port);
            err = -EFAULT;
            goto out_put;
        }
    }
out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

int del_mm_set_on_l2(char *set)
{
    redisReply *rpy = NULL;
    char **keys = NULL;
    int err = 0, knr = 0, i;

    struct redisConnection *rc = getRC_l2(set, 0);

    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed for set %s\n", set);
        return EMMNOTFOUND;
    }

    /* Step 1: get keys of set.* */
    {
        rpy = redisCMD(rc->rc, "keys %s*", set);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out_put;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "MM Meta server error %s\n", rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            knr = rpy->elements;
            keys = alloca(sizeof(char *) * rpy->elements);

            for (i = 0; i < rpy->elements; i++) {
                keys[i] = strdup(rpy->element[i]->str);
            }
        }
    out_free:
        freeReplyObject(rpy);
    }

    /* Step 2: del all keys related this set */
    for (i = 0; i < knr; i++) {
        rpy = redisCMD(rc->rc, "del %s", keys[i]);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "MM Meta server error %s\n", rpy->str);
            err = -EINVAL;
            goto out_free2;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                hvfs_debug(mmcc, "delete set %s ok.\n", keys[i]);
            } else {
                hvfs_debug(mmcc, "delete set %s failed, not exist.\n", keys[i]);
            }
        }
    out_free2:
        freeReplyObject(rpy);
    }

out:
    for (i = 0; i < knr; i++) {
        xfree(keys[i]);
    }
out_put:
    putRC_l2(rc);

    return err;
}

int del_mm_set_on_l1(char *set)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmcc, "getRC_l1() failed for set %s\n", set);
        return EMMMETAERR;
    }

    rpy = redisCMD(rc->rc, "del `%s", set);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "MM Meta server error %s\n", rpy->str);
        err = -EINVAL;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer == 1) {
            hvfs_debug(mmcc, "delete L1 pool set %s ok.\n", set);
        } else {
            hvfs_debug(mmcc, "delete L1 pool set %s failed, not exist.\n", set);
        }
    }
out_free:
    freeReplyObject(rpy);
out_put:
    putRC_l1(rc);

    return err;
}

int del_mm_set_on_mms(char *set)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l2(set, 0);

    if (!rc) {
        hvfs_err(mmcc, "getRC_l2() failed for set %s.\n", set);
        return EMMNOTFOUND;
    }

    rpy = redisCMD(rc->rc, "smembers %s.srvs", set);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }

    if (rpy->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s does not exist on MM Meta server.\n", set);
        err = EMMNOTFOUND;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i;

        for (i = 0; i < rpy->elements; i++) {
            char *p = strdup(rpy->element[i]->str), *n = NULL;
            char *q = p;
            char *host = NULL;
            int j = 0, port = -1;

            for (j = 0; j < 2; j++, p = NULL) {
                p = strtok_r(p, ":", &n);
                if (p != NULL) {
                    switch (j) {
                    case 0:
                        host = strdup(p);
                        break;
                    case 1:
                        port = atoi(p);
                        break;
                    }
                } else {
                    goto out_freep;
                }
            }
            /* send DEL to MMServer */
            __del_mm_set(host, port, set);

            xfree(host);
        out_freep:
            xfree(q);
        }
    }
    
out_free:
    freeReplyObject(rpy);
out:
    putRC_l2(rc);
    
    return err;
}

int del_mm_set(char *set)
{
    int err = 0;

    err = del_mm_set_on_mms(set);
    if (err) {
        hvfs_err(mmcc, "delete set %s on MMServer failed w/ %d\n",
                 set, err);
        goto out;
    }
    err = del_mm_set_on_l2(set);
    if (err) {
        hvfs_err(mmcc, "delete set %s on l2 pool failed w/ %d\n",
                 set, err);
        goto out;
    }
    err = del_mm_set_on_l1(set);
    if (err) {
        hvfs_err(mmcc, "delete set %s on l1 pool failed w/ %d\n",
                 set, err);
        goto out;
    }
    
out:
    return err;
}

/*
 * Return value: 0 -> not in; >0 -> is in; <0 -> error
 */
int __is_in_redis(char *set)
{
    long this_ts = LONG_MAX;
    char *t = set;

    if (!isdigit(set[0]))
        t = set + 1;
    this_ts = atol(t);

    if (this_ts > g_ckpt_ts)
        return 1;
    else
        return 0;
}

int get_ss_object(char *set, char *md5, void **buf, size_t *length)
{
    struct MMSConnection *c ;
    struct MMSCSock *s;
    int err = 0;

    if (!set || !md5 || !buf || !length)
        return EMMINVAL;

    if (g_ssid < 0)
        return EMMINVAL;

    c = __mmsc_lookup(g_ssid);
    if (!c) {
        hvfs_err(mmcc, "lookup SS Server ID=%ld failed.\n", g_ssid);
        err = EMMINVAL;
        goto out;
    }
    if (c->ttl <= 0) {
        err = EMMCONNERR;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        char setlen = (char)strlen(set);
        char md5len = (char)strlen(md5);
        char header[4] = {
            (char)XSEARCH,
            (char)setlen,
            (char)md5len,
        };
        int count, xerr = 0;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed, connection broken?\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, setlen);
        if (err) {
            hvfs_err(mmcc, "send set failed, connection broken?\n");
            goto out_disconn;
        }
        err = send_bytes(s->sock, md5, md5len);
        if (err) {
            hvfs_err(mmcc, "send md5 failed, connection broken?.\n");
            goto out_disconn;
        }

        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMNOTFOUND;
            goto out_put;
        } else if (count < 0) {
            /* oo, we got redirect_info */
            count = -count;
            xerr = EREDIRECT;
        }

        *buf = xmalloc(count);
        if (!*buf) {
            hvfs_err(mmcc, "xmalloc() buffer %dB failed.\n", count);
            err = EMMNOMEM;
            goto out_disconn;
        }
        *length = (size_t)count;

        err = recv_bytes(s->sock, *buf, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed, "
                     "connection broken?\n", count);
            xfree(*buf);
            goto out_disconn;
        }
        if (xerr) err = xerr;
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}
