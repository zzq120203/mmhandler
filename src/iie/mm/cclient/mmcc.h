/*
 *
 * Header file for MM C Client.
 *
 * Any problems please contact macan/zhaoyang @ IIE
 *
 */

#ifndef __MMCC_H__
#define __MMCC_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/uio.h>
#include <unistd.h>
/*
 * Provide url list, splited by ';'
 */
int mmcc_init(char *uris);

/*
 * finalize mmcc client
 */
int mmcc_fina();
    
/*
 * Return the fast lookup info, it can be used as 'key' in get()
 */
char *mmcc_put(char *key, void *content, size_t len);

char *mmcc_put_iov(char *key, struct iovec *iov, int iovlen);

struct mres
{
    char *info;
#define MR_FLAG_DUPED   0x01
    int flag;
};

void mmcc_put_R(char *key, void *content, size_t len, struct mres *mr);

void mmcc_put_iov_R(char *key, struct iovec *iov, int iovlen, struct mres *mr);


/*
 * Caller should free the allocated memory by 'free'
 */
int mmcc_get(char *key, void **buffer, size_t *len);


/*
 * Caller must prepare the buffer for [len] bytes, return result length that
 * might be less than [len] bytes (but >= 0).
 *
 * On any error: return <0 errno.
 */
int mmcc_get_range(char *key, void *buffer, off_t offset, size_t len);

/*
 * Delete the specified key (and might value)
 */
int mmcc_del(char *key);

/*
 * Delete the whole set, release storage space
 */
int mmcc_del_set(char *set);


/*
 * ERROR numbers
 */
#define EMMERR_MAX              -1025

#define EMMMETAERR              -1025
#define EMMNOTFOUND             -1026
#define EMMCONNERR              -1027
#define EMMINVAL                -1028
#define EMMNOMEM                -1029
#define EREDIRECT               -1030
#define EMMNOMMS                -1031
#define EMMMMSERR               -1032

#define EMMERR_MIN              -1032

char *mmcc_strerr(int errno);

/* Advanced Using, use only if you know it
 */
struct key2info
{
    char *key;
    char *info;
};

struct key2info *get_k2i(char *set, int *nr);

void mmcc_debug_mode(int enable);

extern time_t g_client_tick;

typedef void *(*__timer_cb)(void *);

typedef struct
{
    __timer_cb tcb;
    int ti;
    int mode;
    int dupnum;
    int sockperserver;
} mmcc_config_t;

int mmcc_config(mmcc_config_t *);

struct redis_pool_config
{
    char *uri;
    char *master_name;

#define RP_CONF_STA             0
#define RP_CONF_STL             1
#define RP_CONF_CLUSTER         2
    int ptype;
#define RP_CONF_MAX_CONN        50
#define RP_CONF_MIN_CONN        5
    int max_conn;
    int min_conn;
};

#define redisCMD(rc, a...) ({                    \
            struct redisReply *__rr = NULL;      \
            __rr = redisCommand(rc, ## a);       \
            __rr;                                \
        })

#ifdef __cplusplus
}
#endif 

#endif
