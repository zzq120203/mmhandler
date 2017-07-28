/*
 * Client API for high level client
 */
#include ".h"

extern unsigned int hvfs_mmcc_tracing_flags;

int client_config(mmcc_config_t *);
int __client_load_scripts(struct redisConnection *rc, int idx);
int client_fina();
int update_mmserver2();
void update_g_info(struct redisConnection *rc);
int get_ss_object(char *set, char *md5, void **buf, size_t *length);
int __is_in_redis(char *set);
int get_mm_object(char* set, char* md5, void **buf, size_t* length);
int search_mm_object(char *infos, void **buf, size_t *length);
int del_mm_set(char *set);

static char *g_err_str[] = {
    "Meta Service Error",
    "Metadata or Data Not Found",
    "MMS Connection Failure",
    "Invalid MM Argument",
    "No Memory in MMSC",
    "Request Need to Redirect",
    "No Valid MMS",
    "MMS Internal Error",
};

char *mmcc_strerr(int err)
{
    if (err <= EMMERR_MAX && err >= EMMERR_MIN)
        return g_err_str[EMMERR_MAX - err];
    else
        return "Invalid Error Number Input";
}

/* URIs:
 * standalone redis server -> STA://host:port
 * sentinel   redis server -> STL://host:port;host:port
 */
int mmcc_init(char *uris)
{
    struct redis_pool_config rpc = {
        .uri = NULL,
        .master_name = NULL,
        .ptype = RP_CONF_STL,
        .max_conn = RP_CONF_MAX_CONN,
        .min_conn = RP_CONF_MIN_CONN,
    };
    int err = 0;

    if (!uris)
        return EMMINVAL;
    
    g_client_tick = time(NULL);

    err = client_init();
    if (err) {
        hvfs_err(mmcc, "client_init() failed w/ %d\n", err);
        goto out;
    }

    if (strstr(uris, "STL://")) {
        struct redisConnection *rc = NULL;

        rpc.uri = uris + 6;
        rpc.master_name = "l1.master";

        /* init l1 pool */
        err = rpool_init_l1(rpc);
        if (err) {
            hvfs_err(mmcc, "rpool_init_l1() failed w/ %d\n", err);
            goto out;
        }
        
        /* do client auto config from l1 */
        rc = getRC_l1();
        if (rc == NULL) {
            hvfs_err(mmcc, "get rc from L1 pool failed, check config\n");
            err = -EINVAL;
            goto out_l1;
        } else {
            update_g_info(rc);
            update_mmserver2(rc);
            putRC_l1(rc);
        }

        /* init l2 pools */
        err = rpool_init_l2();
        if (err) {
            hvfs_err(mmcc, "rpool_init_l2() failed w/ %d\n", err);
            goto out_l1;
        }

        /* load scripts to l2 pools */
        {
            struct redis_pool **rps = NULL;
            int pnr = 0, i;

            err = get_l2(&rps, &pnr);
            if (err) {
                hvfs_err(mmcc, "get_l2() failed w/ %d\n", err);
                goto out_l2;
            }

            for (i = 0; i < pnr; i++) {
                rc = getRC(rps[i]);
                if (rc != NULL) {
                    err = __client_load_scripts(rc, -1);
                    if (err) {
                        hvfs_err(mmcc, "load client scripts failed w/ %d\n", err);
                        goto out_put;
                    }
                } else {
                    hvfs_err(mmcc, "load scripts on L2 pool %ld failed.\n",
                             rps[i]->pid);
                }
            out_put:
                putRC(rps[i], rc);
            }
            if (rps != NULL) {
                xfree(rps);
            }
        }
        /* reset errno to ZERO */
        err = 0;
    } else if (strstr(uris, "STA://")) {
        rpc.ptype = RP_CONF_STA;
        rpc.uri = uris + 6;
        rpc.master_name = "nomaster";

        /* init l1 pool */
        err = rpool_init_l1(rpc);
        if (err) {
            hvfs_err(mmcc, "rpool_init_l1() failed w/ %d\n", err);
            goto out;
        }
        /* load scripts to l1 pool */
        {
            struct redisConnection *rc = getRC_l1();

            if (rc != NULL) {
                err = __client_load_scripts(rc, -1);
                if (err) {
                    hvfs_err(mmcc, "load client scripts failed w/ %d\n", err);
                    /* reset errno to ZERO */
                    err = 0;
                }
                update_mmserver2(rc);
                putRC_l1(rc);
            }
        }
    } else {
        hvfs_err(mmcc, "Invalid RP type: uri=%s\n", uris);
        err = -EINVAL;
    }

out:
    return err;
out_l2:
    rpool_fina_l2();
out_l1:
    rpool_fina_l1();
    goto out;
}

int mmcc_fina()
{
    int err = 0;

    err = client_fina();
    if (err) {
        hvfs_err(mmcc, "client_fina() failed w/ %d\n", err);
        goto out;
    }

    /* close l1/l2 pool */
    rpool_fina_l2();
    rpool_fina_l1();

out:
    return err;
}


/************
 2017/4/5
 **************/
char *mmcc_put(char *key, void *content, size_t len){

}

static inline void __parse_token(char *key, int *m, int *n)
{
    *m = *n = 0;
    
    while (*key != '\0') {
        if (*key == '#') {
            *m += 1;
        } else if (*key == '@') {
            *n += 1;
        }
        key++;
    }
}

int mmcc_get(char *key, void **buffer, size_t *len)
{
    char *dup = strdup(key);
    char *p = dup, *n = NULL;
    int err = 0, sharpnr, atnr;

    __parse_token(key, &sharpnr, &atnr);
    if (unlikely(sharpnr > 0)) {
        err = search_mm_object(dup, buffer, len);
    } else if (atnr == 1) {
        char *set = strtok_r(p, "@", &n);
        char *md5 = strtok_r(NULL, "@", &n);

        if (__is_in_redis(set)) {
            err = get_mm_object(set, md5, buffer, len);
        } else {
            err = get_ss_object(set, md5, buffer, len);
            if (err == EREDIRECT) {
                char infos[*len + 1];

                memcpy(infos, *buffer, *len);
                infos[*len] = '\0';
                xfree(*buffer);
                *buffer = NULL;
                err = search_mm_object(infos, buffer, len);
            }
        }
    } else {
        err = search_mm_object(dup, buffer, len);
    }
    
    xfree(dup);

    return err;
}

int mmcc_del_set(char *set)
{
    int err = 0;

    if (set == NULL || strlen(set) == 0) {
        err = -EINVAL;
        goto out;
    }
    
    err = del_mm_set(set);
    if (err) {
        hvfs_err(mmcc, "del_mm_set(%s) failed w/ %d\n",
                 set, err);
        goto out;
    }
out:
    return err;
}

void __master_connect_fail_check(struct redis_pool *rp)
{
#define MASTER_FAIL_CHECK       10
    atomic_inc(&rp->master_connect_err);
    if (atomic_read(&rp->master_connect_err) >= MASTER_FAIL_CHECK) {
        __fetch_from_sentinel(rp);
        atomic_set(&rp->master_connect_err, MASTER_FAIL_CHECK >> 2);
    }
}

int mmcc_config(mmcc_config_t *mc)
{
    return client_config(mc);
}

