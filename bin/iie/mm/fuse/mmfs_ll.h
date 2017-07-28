/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-10-12 17:16:55 macan>
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

#ifndef __MMFS_LL_H__
#define __MMFS_LL_H__

#ifndef HVFS_TRACING
#define HVFS_TRACING
#endif

#include "lib.h"
#include "mmcc_ll.h"
#include "hiredis.h"
#include <netdb.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "xlist.h"
#include "xhash.h"
#include "atomic.h"
#include "md5.h"
#include "version.h"
#include <assert.h>
#include "err.h"

#include "tracing.h"

#include <limits.h>
#include <time.h>
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

#define EHOLE   1050
#define ESCAN   1051

static char *mmfs_ccolor[] __attribute__((unused)) = 
{
    "\033[0;40;31m",            /* red */
    "\033[0;40;32m",            /* green */
    "\033[0;40;33m",            /* yellow */
    "\033[0;40;34m",            /* blue */
    "\033[0;40;35m",            /* pink */
    "\033[0;40;36m",            /* yank */
    "\033[0;40;37m",            /* white */
    "\033[0m",                  /* end */
};
#define MMFS_COLOR(x)   (mmfs_ccolor[x])
#define MMFS_COLOR_RED  (mmfs_ccolor[0])
#define MMFS_COLOR_GREEN        (mmfs_ccolor[1])
#define MMFS_COLOR_YELLOW       (mmfs_ccolor[2])
#define MMFS_COLOR_BLUE         (mmfs_ccolor[3])
#define MMFS_COLOR_PINK         (mmfs_ccolor[4])
#define MMFS_COLOR_YANK         (mmfs_ccolor[5])
#define MMFS_COLOR_WHITE        (mmfs_ccolor[6])
#define MMFS_COLOR_END          (mmfs_ccolor[7])

#define PAGE_ROUNDUP(x, p) ((((size_t) (x)) + (p) - 1) & ~((p) - 1))

#define MMFS_ROOT_INO           1

#define MMFS_I_XSIZE            10
#define MMFS_INODE_NAME         "__name+$@^"
#define MMFS_INODE_MD           "__md#$@^-+"
#define MMFS_INODE_SYMNAME      "__sym*&#$@"
#define MMFS_INODE_VERSION      "__ver^&$F#"
#define MMFS_INODE_BLOCK        "__blk%$*(^"
#define MMFS_INODE_CHUNKNR      "__chk**(^%"

#define MMFS_DEFAULT_UMASK      0644
#define MMFS_DEFAULT_DIR_UMASK  0755

struct mdu
{
    u64 ino;
#define MMFS_MDU_LINK           0x00000001
#define MMFS_MDU_SYMLINK        0x00000002
#define MMFS_MDU_DIR            0x00000004
    u64 flags;
    u32 uid;
    u32 gid;
    u32 dev;
    u16 mode;
    u16 nlink;
    u64 size;

    u64 atime;
    u64 ctime;
    u64 mtime;

    u64 blknr;

    u32 version;
};

struct mdu_update
{
    /* flags for valid */
#define MU_MODE         (1 << 0)
#define MU_UID          (1 << 1)
#define MU_GID          (1 << 2)
#define MU_FLAG_ADD     (1 << 3)
#define MU_ATIME        (1 << 4)
#define MU_MTIME        (1 << 5)
#define MU_CTIME        (1 << 6)
#define MU_VERSION      (1 << 7)
#define MU_SIZE         (1 << 8)
#define MU_FLAG_CLR     (1 << 9)
#define MU_NLINK        (1 << 10)
#define MU_SYMNAME      (1 << 11)
#define MU_NLINK_DELTA  (1 << 12)
#define MU_DEV          (1 << 13)
#define MU_BLKNR        (1 << 14)

    u64 atime;
    u64 mtime;
    u64 ctime;
    u64 size;

    /* NOTE: refer to struct bhhead mu_valid */
    u32 valid;

    u32 uid;
    u32 gid;

    u32 flags;

    u32 version;
    s32 nlink;
    u32 dev;
    u16 mode;

    u64 blknr;
};

struct dentry_info
{
    u64 ino;                    /* dentry ino */
    u16 mode;
    u16 namelen;
    u32 __padding;
    char name[0];               /* then dentry name */
};

struct dup_detector
{
#define MMFS_DD_HSIZE_DEFAULT (8192)
    struct regular_hash *ht;
    u64 id;                     /* id for this detector */
    u32 hsize;
    atomic_t nr;
};

#define DUP_DETECTOR(name, xid)                                     \
    struct dup_detector name = {.ht = NULL, .id = xid, .hsize = 0,}

#define INIT_DETECTOR(name, xid) do {                   \
        memset(name, 0, sizeof(struct dup_detector));   \
        (name)->id = xid;                               \
        __dd_init(name, 0);                             \
    } while (0)

#define DESTROY_DETECTOR(name) do {             \
        __dd_destroy(name);                     \
    } while (0)

typedef struct __mmfs_dir
{
    u64 dino;                   /* current directory inode number */
    struct dup_detector dd;
    u64 goffset, loffset;
    char *cursor;               /* hscan cursor */
    struct dentry_info *di;
    int csize;
} mmfs_dir_t;

struct mstat
{
    char *name;
    u64 pino;                   /* parent inode no */
    u64 ino;                    /* self inode no */
    struct mdu mdu;             /* self mdu, self ino in it */
    void *arg;
};

struct __sb_delta
{
    u64 space_quota;
    u64 space_used;
    u64 inode_quota;
    u64 inode_used;
};

struct mmfs_sb
{
    char *name;                 /* file system name */
    u64 root_ino;               /* root inode number */
    u64 version;

    u64 space_quota;
    u64 space_used;
    u64 inode_quota;
    u64 inode_used;

    struct __sb_delta d;

#define MMFS_SB_DIRTY   0x01
    int flags;

#define MMFS_SB_U_INR   0x01
#define MMFS_SB_U_SPACE 0x02

    xlock_t lock;

    u64 chunk_size;
};

struct __mmfs_fuse_mgr
{
    u32 inited:1;
    u32 sync_write:1;
    u32 noatime:1;
    u32 nodiratime:1;
    u32 ismkfs:1;
    u32 perm:1;
    u32 cached_chunk:1;
    u32 useltc:1;

    u32 ttl:8;                  /* lru translate cache ttl */

    time_t tick;                /* this tick should update by itimer */

    char *uris;
    char *namespace;
    char *rootdir;

#define MMFS_LARGE_FILE_CHUNK   (10 * 1024 * 1024)
    u64 chunk_size;
};

struct __mmfs_op_stat
{
#define OP_NONE         0
#define OP_GETATTR      1
#define OP_READLINK     2
#define OP_MKNOD        3
#define OP_MKDIR        4
#define OP_UNLINK       5
#define OP_RMDIR        6
#define OP_SYMLINK      7
#define OP_RENAME       8
#define OP_LINK         9
#define OP_CHMOD        10
#define OP_CHOWN        11
#define OP_TRUNCATE     12
#define OP_UTIME        13
#define OP_OPEN         14
#define OP_READ         15
#define OP_WRITE        16
#define OP_STATFS_PLUS  17
#define OP_RELEASE      18
#define OP_FSYNC        19
#define OP_OPENDIR      20
#define OP_READDIR_PLUS 21
#define OP_RELEASE_DIR  22
#define OP_CREATE_PLUS  23
#define OP_FTRUNCATE    24
#define OP_A_FSYNC      25

    atomic64_t getattr;
    atomic64_t readlink;
    atomic64_t mknod;
    atomic64_t mkdir;
    atomic64_t unlink;
    atomic64_t rmdir;
    atomic64_t symlink;
    atomic64_t rename;
    atomic64_t link;
    atomic64_t chmod;
    atomic64_t chown;
    atomic64_t truncate;
    atomic64_t utime;
    atomic64_t open;
    atomic64_t read;
    atomic64_t write;
    atomic64_t statfs_plus;
    atomic64_t release;
    atomic64_t fsync;
    atomic64_t opendir;
    atomic64_t readdir_plus;
    atomic64_t release_dir;
    atomic64_t create_plus;
    atomic64_t ftruncate;
    atomic64_t a_fsync;
};

struct __mmfs_client_info
{
    char *hostname;
    char *namespace;
    char *ip;
    char *md5;                  /* self client binary md5sum */
    time_t born;                /* mount time */
    u32 used_pages;             /* ODC used pages */
    u32 free_pages;             /* ODC free pages */
    u32 dirty_pages;            /* ODC dirty pages */
    struct __mmfs_op_stat os;
};

/* Regin for Internal APIs */
extern u32 hvfs_mmll_tracing_flags;
extern u32 hvfs_mmcc_tracing_flags;

static inline void __mmfs_gset(u64 ino, char *out)
{
    snprintf(out, 31, "o%ld", ino);
}

int __mmfs_fill_root(struct mstat *ms);

int __mmfs_load_scripts();

void __mmfs_unload_scripts();

int __mmfs_stat(u64 pino, struct mstat *ms);

#define __MMFS_CREATE_SYMLINK   0x01
#define __MMFS_CREATE_DIR       0x02
#define __MMFS_CREATE_LINK      0x04
#define __MMFS_CREATE_DENTRY    0x10
#define __MMFS_CREATE_INODE     0x20
#define __MMFS_CREATE_ALL       0xf0
int __mmfs_create(u64 pino, struct mstat *ms, struct mdu_update *mu, u32 flags);

#define __MMFS_UNLINK_DENTRY    0x01
#define __MMFS_UNLINK_INODE     0x02
#define __MMFS_UNLINK_ALL       0xff
int __mmfs_unlink(u64 pino, struct mstat *ms, u32 flags);

int __mmfs_is_empty_dir(u64 dino);

int __mmfs_linkadd(struct mstat *ms, s32 nlink, u32 flags);

int __mmfs_update_inode(struct mstat *ms, struct mdu_update *mu);

int __mmfs_fread(struct mstat *ms, void *data, u64 off, u64 size);

int __mmfs_fwrite(struct mstat *ms, u32 flag, void *data, u64 size, u64 chkid);

int __mmfs_fwritev(struct mstat *ms, u32 flag, struct iovec *iov, int iovlen, u64 chkid);

int __mmfs_clr_block(struct mstat *ms, u64 chkid);

int __mmfs_readdir(mmfs_dir_t *dir);

int __mmfs_readlink(u64 pino, struct mstat *ms);

int __mmfs_create_root(struct mstat *ms, struct mdu_update *mu);

int __mmfs_create_sb(struct mmfs_sb *msb);

int __mmfs_update_sb(struct mmfs_sb *msb);

int __mmfs_get_sb(struct mmfs_sb *msb);

void __update_msb(int flag, s64 delta);

int __mmfs_inc_shadow_dir(u64 dino);

int __mmfs_dec_shadow_dir(u64 dino);

int __mmfs_is_shadow_dir(u64 dino);

int __mmfs_rename_log(u64 ino, u64 opino, u64 npino);

int __mmfs_rename_fix(u64 ino);

int __mmfs_renew_ci(struct __mmfs_client_info *ci, int type);

int __mmfs_client_info(struct __mmfs_client_info *ci);

int __dd_init(struct dup_detector *dd, int hsize);

void __dd_destroy(struct dup_detector *dd);

#endif
