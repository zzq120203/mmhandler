
#include "mmcc.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <semaphore.h>
#include <pthread.h>

#ifndef HVFS_TRACING
#define HVFS_TRACING
#endif

#include "tracing.h"
#include <getopt.h>

HVFS_TRACING_DEFINE_FILE();

static int g_tget_stop = 0;
static sem_t tget_sem;
static int g_tput_stop = 0;
static sem_t tput_sem;

struct tget_args
{
    char *set;
    struct key2info *ki;
    double begin, end;
    int gnr, id;
    size_t len;                 /* length we get */
};

struct tput_args
{
    char *set;
    double begin, end;
    int gnr, id;
    size_t wlen;                /* length to write for each object */
    size_t len;                 /* total length written */
};

static void *__tget(void *args)
{
    struct tget_args *ta = (struct tget_args *)args;
    struct timeval tv;
    char *buffer;
    int i, err;
    size_t len, tlen = 0;-

    gettimeofday(&tv, NULL);
    ta->begin = (double)(tv.tv_sec * 1000000 + tv.tv_usec);

    for (i = 0; i < ta->gnr; i++) {
        char key[256];

        sprintf(key, "%s@%s", ta->set, ta->ki[i].key);
        err = mmcc_get(key, (void **)&buffer, &len);
        if (err) {
            printf("get(%s) failed w/ %d\n", key, err);
        } else {
            tlen += len;
            free(buffer);
        }
    }
    gettimeofday(&tv, NULL);
    ta->end = (double)(tv.tv_sec * 1000000 + tv.tv_usec);
    
    printf("TGID=%d gnr %d %.4f us, GPS %.4f, tlen %ld.\n", 
           ta->id, ta->gnr, (ta->end - ta->begin),
           (ta->gnr * 1000000.0 / (ta->end - ta->begin)), (long)tlen);
    ta->len = tlen;

    sem_post(&tget_sem);
    
    pthread_exit(NULL);
}

int thread_get(char *set, int gnr, int tnr)
{
    pthread_t threads[tnr];
    struct tget_args ta[tnr];
    struct key2info *ki;
    struct timeval tv;
    double begin, end;
    int i, anr = 0, err;
    size_t tlen = 0;

    sem_init(&tget_sem, 0, 0);
    
    /* get valid keys */
    ki = get_k2i(set, &anr);
    if (!ki) {
        printf("get_k2i(%s) failed.\n", set);
        return -1;
    }
    if (gnr > anr)
        gnr = anr;

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec + tv.tv_usec / 1000000.0;
    for (i = 0; i < tnr; i++) {
        ta[i].id = i;
        ta[i].set = set;
        ta[i].ki = ki + i * (gnr / tnr);
        ta[i].gnr = gnr / tnr;

        err = pthread_create(&threads[i], NULL, __tget, &ta[i]);
        if (err) {
            perror("pthread_create");
            g_tget_stop = 1;
            break;
        }
    }

    if (g_tget_stop)
        return err;
    
    for (i = 0; i < tnr; i++) {
        sem_wait(&tget_sem);
    }
    
    for (i = 0; i < tnr; i++) {
        pthread_join(threads[i], NULL);
        tlen += ta[i].len;
    }
    gettimeofday(&tv, NULL);
    end = tv.tv_sec + tv.tv_usec / 1000000.0;
    
    for (i = 0; i < anr; i++) {
        free(ki[i].key);
        free(ki[i].info);
    }
    free(ki);

    printf("TGET tlen %ld B tlat %lf s, avg lat %lf ms, avg BW %lf bps\n",
           (long)tlen, (end - begin), (end - begin) / anr * 1000.0,
           tlen / (end - begin));

    return 0;
}

static void __gen_content(char *buffer, int len)
{
    long seed = random();
    int i;

    if (len < 8) {
        seed %= 256;
        for (i = 0; i < len; i++) {
            buffer[i] = (seed + i) & 0xff;
        }
    } else {
        long *f = (long *)buffer;

        for (i = 0; i < len / sizeof(long); i++) {
            f[i] = seed + i;
        }
    }
}

static void *__tput(void *args)
{
    struct tput_args *ta = (struct tput_args *)args;
    struct timeval tv;
    int i, err;
    size_t len, tlen = 0;

    gettimeofday(&tv, NULL);
    ta->begin = (double)(tv.tv_sec * 1000000 + tv.tv_usec);

    for (i = 0; i < ta->gnr; i++) {
        char key[256], *info = NULL;
        char buffer[ta->wlen];

        __gen_content(buffer, ta->wlen);
        sprintf(key, "%s@%d__%d", ta->set, ta->id, i);
        len = ta->wlen;
        info = mmcc_put(key, buffer, ta->wlen);
        if (info == NULL) {
            printf("put(%s) failed w/ %d\n", key, err);
        } else {
            tlen += len;
            free(info);
        }
    }
    gettimeofday(&tv, NULL);
    ta->end = (double)(tv.tv_sec * 1000000 + tv.tv_usec);

    printf("TGID=%d gnr %d %.4f us, PPS %.4f, tlen %ld.\n",
           ta->id, ta->gnr, (ta->end - ta->begin),
           (ta->gnr * 1000000.0 / (ta->end - ta->begin)), (long)tlen);
    ta->len = tlen;

    sem_post(&tput_sem);

    pthread_exit(NULL);
}

int thread_put(char *set, int gnr, int tnr, long len)
{
    pthread_t threads[tnr];
    struct tput_args ta[tnr];
    struct timeval tv;
    double begin, end;
    int i, anr = 0, err;
    size_t tlen = 0;

    sem_init(&tput_sem, 0, 0);

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec + tv.tv_usec / 1000000.0;
    for (i = 0; i < tnr; i++) {
        ta[i].id = i;
        ta[i].set = set;
        ta[i].gnr = gnr / tnr;
        ta[i].wlen = len;

        err = pthread_create(&threads[i], NULL, __tput, &ta[i]);
        if (err) {
            perror("pthread_create");
            g_tput_stop = 1;
            break;
        }
    }

    if (g_tput_stop)
        return err;

    for (i = 0; i < tnr; i++) {
        sem_wait(&tput_sem);
    }

    for (i = 0; i < tnr; i++) {
        pthread_join(threads[i], NULL);
        tlen += ta[i].len;
        anr += ta[i].gnr;
    }
    gettimeofday(&tv, NULL);
    end = tv.tv_sec + tv.tv_usec / 1000000.0;

    printf("TPUT tlen %ld B tlat %lf s, avg lat %lf ms, avg BW %lf bps\n",
           (long)tlen,(end - begin), (end - begin) / anr * 1000.0,
           tlen / (end - begin));

    return 0;
}

int kvtest()
{
    int err = 0;
#if 0
    char *buffer = NULL, *key = "default@f07405a864130580916bba658929bf30";
    size_t len = 0;

    err = mmcc_get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);

    key = "1@default@2@0@0@150@a";

    err = mmcc_get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);

    key = "1@default@1@0@0@193247@c#1@default@1@0@0@193247@d";

#if 0
    {
        int n = 120;
        do {
            n = sleep(n);
        } while (n--);
    }
#endif

    err = mmcc_get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);
#endif

#if 0
    if (0) {
        int x = 0;
        char key[256], *info = NULL;

        for (x = 0; x < 100000; x++) {
            sprintf(key, "%s@__%d", "default", x);
            info = mmcc_put(key, key, strlen(key));
            printf("mmcc_put(%s) info=%s\n", key, info);
            free(info);
        }
    }

    err = thread_get("default", 100000, 200);
    if (err) {
        printf("thread_get() failed w/ %d\n", err);
    }
#endif
    
    return err;
}

void do_help()
{
    printf("Version 1.0.0a\n"
           "Arguments:\n"
           "-h, -? --help       print this help.\n"
           "-u, --url           URL to connecto to.\n"
           "-i, --iter          number of iterations.\n"
           "-n, --len           length for each object.\n"
           "-t, --nth           number of threads.\n"
        );
}

int main(int argc, char *argv[])
{
    char *url = NULL, *set = NULL;
    char *shortflags = "h?n:i:t:u:s:r:o:S:";
    struct l longflags[] = {
        {"help", no_argument, 0, 'h'},
        {"url", required_argument, 0, 'u'},
        {"iter", required_argument, 0, 'i'},
        {"len", required_argument, 0, 'n'},
        {"nth", required_argument, 0, 't'},
        {"seed", required_argument, 0, 's'},
        {"repnr", required_argument, 0, 'r'},
        {"op", required_argument, 0, 'o'},
        {"set", required_argument, 0, 'S'},
    };
    long len = 1024, iter = 1000, nth = 10;
    int err = 0, seed = time(NULL), op = 0;
    mmcc_config_t mc;
    
    memset(&mc, 0, sizeof(mc));
    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
        default:
        case '?':
        case 'h':
            do_help();
            return 0;
        case 'u':
            url = strdup(optarg);
            break;
        case 'i':
            iter = atol(optarg);
            break;
        case 'n':
            len = atol(optarg);
            break;
        case 't':
            nth = atol(optarg);
            break;
        case 's':
            seed = atoi(optarg);
            break;
        case 'r':
            mc.dupnum = atoi(optarg);
            break;
        case 'o':
            if (strcmp(optarg, "put") == 0) {
                op = 1;
            } else if (strcmp(optarg, "get") == 0) {
                op = 0;
            }
            break;
        case 'S':
            set = strdup(optarg);
            break;
        }
    }
    if (!url) {
        printf("no URL provided.\n");
        do_help();
        goto out;
    }
    srandom(seed);
    printf("Use seed %d\n", seed);

    mc.sockperserver = 10;
    mmcc_config(&mc);

    err = mmcc_init(url);
    if (err) {
        printf("init() failed w/ %d\n", err);
        goto out_free;
    }

    if (op == 1) {
        err = thread_put(set ? set : "default", iter, nth, len);
        if (err) {
            printf("thread_put() faield w/ %d\n", err);
            goto out_fina;
        }
    }
    
    if (op == 0) {
        err = thread_get(set ? set : "default", iter, nth);
        if (err) {
            printf("thread_get() failed w/ %d\n", err);
            goto out_fina;
        }
    }

out_fina:
    mmcc_fina();
out_free:
    free(url);
    free(set);
out:
    return err;
}
