/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-01-25 09:43:30 macan>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#ifndef __LIB_RING_H__
#define __LIB_RING_H__

#include "lib.h"

#define VID_MAX_DEFAULT         (1)

/* point on the consistent hash ring */
struct chp 
{
    u64 point;                  /* point on the ring */
    u32 vid;                    /* virtual id of the machine */
#define CHP_AUTO        0x00
#define CHP_MANUAL      0x01
    u32 type;                   /* auto or manual */
    u64 site_id;
    char *node;
    int port;
    void *private;
};

struct chring 
{
    u32 alloc;                  /* point allocated */
    u32 used;                   /* point used */
    u32 group;
    xrwlock_t rwlock;           /* protect the array */
    struct chp *array;          /* array of struct chp, sorted by `point' */
};

struct chring_tx
{
    u32 group;
    u32 nr;                     /* # of points in the ring */
    struct chp array[0];
};

struct ring_range
{
    u64 start;                  /* range start */
    u64 end;                    /* range end */
    u64 dist;                   /* distance of the range */
};

#define RING_ALLOC_FACTOR       32
#define RING_ALLOC_FACTOR_SHIFT 5

/* Allocate a ring */
struct chring *ring_alloc(int alloc, u32 gid);

/* Free a ring */
void ring_free(struct chring *r);

void ring_resort_nolock(struct chring *r);
void ring_resort_locked(struct chring *r);
int ring_add_point(struct chp *p, struct chring *r);
int ring_add_point_nosort(struct chp *p, struct chring *r);

/* Get the point in the ring */
struct chp *ring_get_point(char *key, int klen, struct chring *r);
struct chp *ring_get_point2(u64 point, struct chring *r);

/* Dump the consistent hash ring */
void ring_dump(struct chring *r);
void ring_stat(struct chring *r, int nr);

/* Ring Hash function, using what? */
u64 ring_hash(u64 key, u64 salt);

int ring_topn_range(int, struct chring *, struct ring_range *);
int ring_find_site(struct chring *, u64, void **);
int ring_del_point(struct chp *, struct chring *);

/* fast init ring, the caller should provide the lock */
#define ring_mem_prepare(r, newsize, ret) do {                          \
        if (r->alloc < newsize) {                                       \
            r->array = xrealloc(r->array, newsize * sizeof(struct chp)); \
            if (!r->array) {                                            \
                hvfs_debug(lib, "xrealloc failed\n");                   \
                ret = -ENOMEM;                                          \
                break;                                                  \
            }                                                           \
            r->alloc = newsize;                                         \
        }                                                               \
    } while (0)

#define ring_add_blob(r, i, p) do { \
        r->array[i] = *(p);         \
        r->used++;                  \
    } while (0)

int ring_add(struct chring *r, char *node, int port, int id, int vid_max);
struct xnet_group *__get_active_site(struct chring *r);
struct xnet_group_entry *find_site(struct xnet_group *xg, int site_id);
void xnet_group_sort(struct xnet_group *xg);
int xnet_group_add(struct xnet_group **xg, u64 site_id, char *node, int port);

#endif
