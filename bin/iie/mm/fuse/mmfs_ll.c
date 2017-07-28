/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-10-27 13:29:16 macan>
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

#include "mmfs.h"

u32 hvfs_mmll_tracing_flags = HVFS_DEFAULT_LEVEL;

struct __mmfs_r_op
{
    char *opname;
    char *script;
    char *sha;
};

#define __MMFS_R_OP_CREATE_INODE        0
#define __MMFS_R_OP_CREATE_DENTRY       1
#define __MMFS_R_OP_DELETE_DENTRY       2
#define __MMFS_R_OP_UPDATE_INODE        3
#define __MMFS_R_OP_UPDATE_SB           4
#define __MMFS_R_OP_UPDATE_BLOCK        5
#define __MMFS_R_OP_CLEAR_BLOCK         6

struct __mmfs_r_op g_ops[] = {
    {
        "create_inode",
        "local t = redis.call('incr', 'INO_CUR'); if t then local t0 = t; t = t..\",\"..ARGV[2]; local v = redis.call('hget', KEYS[1]..t0, ARGV[3]); if v == false then v = '0' end; if v == ARGV[4] then redis.call('hset', KEYS[1]..t0, ARGV[1], t); return t; else return ARGV[2] end; else return ARGV[2] end",
    },
    {
        "create_dentry",
        "local t = redis.call('hexists', KEYS[1], ARGV[1]); if t == 1 then return 1; else redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); return 0; end"
    },
    {
        "delete_dentry",
        "local t = redis.call('hexists', KEYS[1], ARGV[1]); if t == 0 then return 0; else t = redis.call('hget', KEYS[1], ARGV[1]); redis.call('hdel', KEYS[1], ARGV[1]); return t; end",
    },
    {
        "update_inode",
        "local x = redis.call('exists', KEYS[1]); if x == 0 then return -1 end; local v = redis.call('hget', KEYS[1], ARGV[3]); if v == false then v = '0' end; if v == ARGV[4] then redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); if not (ARGV[6] == 'nil') then redis.call('hset', KEYS[1], ARGV[5], ARGV[6]) end; redis.call('hset', KEYS[1], ARGV[3], v+1); return 1; else return 0 end",
    },
    {
        "update_sb",
        "local x = redis.call('exists', KEYS[1]); if x == 0 then return nil end; local v = redis.call('hget', KEYS[1], 'version'); if v == false then v = '0' end; local v1 = redis.call('hget', KEYS[1], 'space_quota'); local v2 = redis.call('hget', KEYS[1], 'space_used'); local v3 = redis.call('hget', KEYS[1], 'inode_quota'); local v4 = redis.call('hget', KEYS[1], 'inode_used'); if v == ARGV[1] then redis.call('hmset', KEYS[1], 'root_ino',ARGV[2], 'space_quota', v1+ARGV[3], 'space_used', v2+ARGV[4], 'inode_quota', v3+ARGV[5], 'inode_used', v4+ARGV[6], 'version', v+1); return redis.call('hgetall', KEYS[1]); else return nil end"
    },
    {
        "update_block",
        "local r = {}; local x = redis.call('hexists', KEYS[1], ARGV[1]); local y = nil; if x == 1 then y=redis.call('hget', KEYS[1], ARGV[1]); if y == ARGV[2] then return {2,y}; end; end; x=redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); return {x,y};",
    },
    {
        "clear_block",
        "local r = {}; local x = redis.call('hexists', KEYS[1], ARGV[1]); if x == 1 then local y=redis.call('hget', KEYS[1], ARGV[1]); local b=redis.call('hexists', '_DUPSET_', y); local c=0; if b == 1 then c=redis.call('hincrby', '_DUPSET_', y, -1); if c < 0 then redis.call('hdel', '_DUPSET_', y); end; end; redis.call('hdel', KEYS[1], ARGV[1]); if c < 0 then r = {2, y}; else r = {1, y}; end; else r = {0}; end; return r;"
    },
};

int __mmfs_load_scripts_4each(struct redisConnection *rc, int idx)
{
    redisReply *rpy = NULL;
    int err = 0, i;

    for (i = 0; i < sizeof(g_ops) / sizeof(struct __mmfs_r_op); i++) {
        if (idx == -1 || i == idx) {
            rpy = redisCommand(rc->rc, "script load %s", g_ops[i].script);
            if (rpy == NULL) {
                hvfs_err(mmll, "read from MM Meta failed: %s(pid: %ld)\n", 
                         rc->rc->errstr, rc->pid);
                freeRC(rc);
                err = -EMMMETAERR;
                goto out;
            }
            if (rpy->type == REDIS_REPLY_ERROR) {
                hvfs_err(mmll, "script %d load failed w/ \n%s(pid: %ld).\n", 
                         i, rpy->str, rc->pid);
                err = -EINVAL;
                goto out_free;
            }
            if (rpy->type == REDIS_REPLY_STRING) {
                hvfs_info(mmll, "[%ld] Script %d %s \tloaded as '%s'.\n",
                          rc->pid, i, g_ops[i].opname, rpy->str);
                xfree(g_ops[i].sha);
                g_ops[i].sha = strdup(rpy->str);
            } else {
                xfree(g_ops[i].sha);
                g_ops[i].sha = NULL;
            }
        out_free:
            freeReplyObject(rpy);
        }
    }

out:
    return err;
}

int __mmfs_load_scripts(int idx)
{
    struct redisConnection *rc = getRC_l1();
    int err = 0;

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for load scripts.\n");
        return -EINVAL;
    }

    err = __mmfs_load_scripts_4each(rc, idx);
    if (err) {
        hvfs_err(mmll, "do scripts loads on pool %ld failed w/ %d\n",
                 rc->pid, err);
        putRC_l1(rc);
        goto out;
    }
    putRC_l1(rc);

    {
        struct redis_pool **rps = NULL;
        int pnr = 0, i;

        err = get_l2(&rps, &pnr);
        if (err) {
            hvfs_err(mmll, "get_l2() failed w/ %d\n", err);
            goto out;
        }

        for (i = 0; i < pnr; i++) {
            struct redisConnection *rc0 = getRC(rps[i]);
            if (rc0 != NULL) {
                err = __mmfs_load_scripts_4each(rc0, idx);
                if (err) {
                    hvfs_err(mmll, "load scripts on pool %ld failed w/ %d\n",
                             rc0->pid, err);
                    goto out_put;
                }
            } else {
                hvfs_err(mmll, "load scripts on L2 pool %ld failed.\n",
                         rps[i]->pid);
            }
        out_put:
            putRC(rps[i], rc0);
        }
        xfree(rps);
    }

out:
    return err;
}

void __mmfs_unload_scripts()
{
    int i;

    for (i = 0; i < sizeof(g_ops) / sizeof(struct __mmfs_r_op); i++) {
        xfree(g_ops[i].sha);
    }
}

int __mmfs_fill_root(struct mstat *ms)
{
    int err = 0;

    memset(ms, 0, sizeof(*ms));
    ms->ino = g_msb.root_ino;
    err = __mmfs_stat(ms->ino, ms);
    if (err) {
        hvfs_err(mmll, "do internal ROOT stat failed w/ %d\n",
                 err);
    }

    return err;
}

int __str2mdu(char *mstr, struct mdu *mdu)
{
    char *p = NULL, *n, *s = NULL;
    char *dup = strdup(mstr);
    int err = 0, i = 0;

    n = dup;
    do {
        p = strtok_r(n, ",", &s);
        if (!p) {
            break;
        }
        hvfs_verbose(mmll, "token: %s\n", p);
        switch (i) {
        case 0:
            mdu->ino = (u64)atol(p);
            break;
        case 1:
            mdu->flags = (u64)atol(p);
            break;
        case 2:
            mdu->uid = (u32)atoi(p);
            break;
        case 3:
            mdu->gid = (u32)atoi(p);
            break;
        case 4:
            mdu->dev = (u32)atoi(p);
            break;
        case 5:
            mdu->mode = (u16)atoi(p);
            break;
        case 6:
            mdu->nlink = (u16)atoi(p);
            break;
        case 7:
            mdu->size = (u64)atol(p);
            break;
        case 8:
            mdu->atime = (u64)atol(p);
            break;
        case 9:
            mdu->ctime = (u64)atol(p);
            break;
        case 10:
            mdu->mtime = (u64)atol(p);
            break;
        case 11:
            mdu->blknr = (u64)atol(p);
            break;
        case 12:
            mdu->version = (u32)atoi(p);
            break;
        }
        i++;
    } while (!(n = NULL));

    xfree(dup);

    if (i < 13) {
        err = -EINVAL;
    }

    return err;
}

void __mdu2str(struct mdu *mdu, char *ostr, int ignore_ino)
{
    char *p = ostr;
    
    if (ostr == NULL)
        return;

    if (!ignore_ino)
        p += sprintf(p, "%ld,", mdu->ino);
    p += sprintf(p, "%ld,", mdu->flags);
    p += sprintf(p, "%d,", mdu->uid);
    p += sprintf(p, "%d,", mdu->gid);
    p += sprintf(p, "%d,", mdu->dev);
    p += sprintf(p, "%d,", mdu->mode);
    p += sprintf(p, "%d,", mdu->nlink);
    p += sprintf(p, "%ld,", mdu->size);
    p += sprintf(p, "%ld,", mdu->atime);
    p += sprintf(p, "%ld,", mdu->ctime);
    p += sprintf(p, "%ld,", mdu->mtime);
    p += sprintf(p, "%ld,", mdu->blknr);
    p += sprintf(p, "%d", mdu->version);
}

void __init_mdu(struct mdu *mdu, int is_dir)
{
    memset(mdu, 0, sizeof(*mdu));
    mdu->nlink = 1;
    if (is_dir) {
        mdu->nlink = 2;
    }
}

void __pack_mdu(struct mdu *mdu, struct mdu_update *mu)
{
    if (mu->valid & MU_MODE)
        mdu->mode = mu->mode;
    if (mu->valid & MU_UID)
        mdu->uid = mu->uid;
    if (mu->valid & MU_GID)
        mdu->gid = mu->gid;
    if (mu->valid & MU_FLAG_ADD)
        mdu->flags |= mu->flags;
    if (mu->valid & MU_ATIME)
        mdu->atime = mu->atime;
    if (mu->valid & MU_MTIME)
        mdu->mtime = mu->mtime;
    if (mu->valid & MU_CTIME)
        mdu->ctime = mu->ctime;
    if (mu->valid & MU_VERSION)
        mdu->version = mu->version;
    if (mu->valid & MU_SIZE)
        mdu->size = mu->size;
    if (mu->valid & MU_FLAG_CLR)
        mdu->flags &= ~(mu->flags);
    if (mu->valid & MU_NLINK)
        mdu->nlink = mu->nlink;
    if (mu->valid & MU_NLINK_DELTA)
        mdu->nlink += mu->nlink;
    if (mu->valid & MU_DEV)
        mdu->dev = mu->dev;
    if (mu->valid & MU_BLKNR)
        mdu->blknr = mu->blknr;
}

void __update_msb(int flag, s64 delta)
{
    if (delta == 0) return;

    hvfs_debug(mmll, "Update MSB:{0x%x -> %ld}\n", flag, delta);
    xlock_lock(&g_msb.lock);
    switch (flag) {
    case MMFS_SB_U_INR:
        g_msb.inode_used += delta;
        g_msb.flags |= MMFS_SB_DIRTY;
        break;
    case MMFS_SB_U_SPACE:
        g_msb.space_used += delta;
        g_msb.flags |= MMFS_SB_DIRTY;
        break;
    }
    xlock_unlock(&g_msb.lock);
}

/*
 * ms->name : dentry name to stat
 * ms->ino  : dentry ino to stat (if ino > 0, use it; otherwise use ->name)
 */
int __mmfs_stat(u64 pino, struct mstat *ms)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for pino %ld "
                 "(ino=%ld,name=%s)\n", pino, ms->ino,
                 (ms->ino == 0 ? ms->name : "----"));
        return -EINVAL;
    }

    if (ms->ino == 0) {
        if (!ms->name) {
            hvfs_err(mmll, "invalid argument for null name.\n");
            err = -EINVAL;
            goto out;
        }
        /* stat by self name, use pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", pino, ms->name);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_%ld / '%s' does not exist.\n",
                     pino, ms->name);
            err = -ENOENT;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* convert to ino */
            ms->ino = atol(rpy->str);
        }
        /* release resouce */
    out_free:
        freeReplyObject(rpy);
    }
    if (ms->ino > 0) {
        /* stat by self ino, ignore pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", ms->ino, MMFS_INODE_MD);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_%ld '_MD_' does not exist.\n", 
                     ms->ino);
            err = -ENOENT;
            goto out_free2;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(mmll, "_IN_%ld mdu: %s\n", ms->ino, rpy->str);
            /* convert mdu_string to mdu */
            ms->pino = pino;
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(mmll, "Invalid MDU for %ld (%s)\n", ms->ino, rpy->str);
            }
        }
    out_free2:
        freeReplyObject(rpy);
    } else {
        err = -ENOENT;
    }
    
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_readlink(u64 pino, struct mstat *ms)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for pino %ld "
                 "(ino=%ld,name=%s)\n", pino, ms->ino,
                 (ms->ino == 0 ? ms->name : "----"));
        return -EINVAL;
    }

    if (ms->ino > 0) {
        /* stat by self ino, ignore pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", ms->ino, MMFS_INODE_SYMNAME);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_%ld '_SYMNAME_' does not exist.\n",
                     ms->ino);
            err = -ENOENT;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(mmll, "_IN_%ld metastore: %s\n", ms->ino, rpy->str);
            ms->arg = strdup(rpy->str);
        }
    out_free:
        freeReplyObject(rpy);
    } else {
        hvfs_err(mmll, "read link from %ld(%s) is not allowed, please convert "
                 " it to ms->ino.\n",
                 pino, ms->name);
        err = -EINVAL;
    }
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_create(u64 pino, struct mstat *ms, struct mdu_update *mu, u32 flags)
{
    char mstr[512];
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for pino %ld name %s\n",
                 pino, ms->name);
        return -EINVAL;
    }

    __init_mdu(&ms->mdu, flags & __MMFS_CREATE_DIR);
    __pack_mdu(&ms->mdu, mu);
    __mdu2str(&ms->mdu, mstr, 1);
    hvfs_debug(mmll, "CREATE MDU: (pino %ld)/%s -> ?,%s\n",
               pino, ms->name, mstr);

    /* Step 1: create a new inode hash table for self */
    if (flags & __MMFS_CREATE_INODE) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_ %s %s %s %d",
                           g_ops[__MMFS_R_OP_CREATE_INODE].sha,
                           MMFS_INODE_MD,
                           mstr,
                           MMFS_INODE_VERSION,
                           0);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_* create for (pino %ld)/%s failed w/\n%s\n",
                     pino, ms->name, rpy->str);
            if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                err = __mmfs_load_scripts(__MMFS_R_OP_CREATE_INODE);
                if (err) {
                    hvfs_err(mmll, "try to reload script %s failed: %d\n",
                             g_ops[__MMFS_R_OP_CREATE_INODE].opname, err);
                }
            }
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* parse the return mdu info */
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(mmll, "_IN_* create for (pino %ld)/%s failed.\n",
                         pino, ms->name);
                err = -EINVAL;
                goto out_free;
            }
            hvfs_debug(mmll, "_IN_%ld created ok (pino %ld)/%s.\n",
                       ms->mdu.ino, pino, ms->name);
            ms->ino = ms->mdu.ino;
            ms->pino = pino;
            __update_msb(MMFS_SB_U_INR, 1);
        }
    out_free:
        freeReplyObject(rpy);
    }
    if (err)
        goto out;

    if (flags & __MMFS_CREATE_SYMLINK) {
        hvfs_debug(mmll, "CREATE SYMLINK from _IN_%ld to %s\n",
                   ms->ino, (char *)ms->arg);
        rpy = redisCommand(rc->rc, "hsetnx _IN_%ld %s %s",
                           ms->ino,
                           MMFS_INODE_SYMNAME,
                           (char *)ms->arg);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_%ld add SYMNAME %s failed w/ %s\n",
                     ms->ino, (char *)ms->arg, rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out_clear;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 0) {
                err = -EEXIST;
                freeReplyObject(rpy);
                goto out_clear;
            }
        }
        freeReplyObject(rpy);
    }
    /* Step 2: insert a new entry to parent ino's hash table */
    if (flags & __MMFS_CREATE_DENTRY) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %ld",
                           g_ops[__MMFS_R_OP_CREATE_DENTRY].sha,
                           pino, ms->name, ms->ino);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s (_IN_%ld leaks)\n", 
                     rc->rc->errstr, ms->ino);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "create dentry for (pino %ld)/%s failed.\n",
                     pino, ms->name);
            if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                err = __mmfs_load_scripts(__MMFS_R_OP_CREATE_DENTRY);
                if (err) {
                    hvfs_err(mmll, "try to reload script %s failed: %d\n",
                             g_ops[__MMFS_R_OP_CREATE_DENTRY].opname, err);
                }
            }
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out_clear;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                /* this means target file name exists */
                err = -EEXIST;
                freeReplyObject(rpy);
                goto out_clear;
            }
        }
        freeReplyObject(rpy);
    }

out:
    putRC_l1(rc);

    return err;
out_clear:
    {
        rpy = redisCommand(rc->rc, "del _IN_%ld", ms->ino);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "del _IN_%ld failed w/ %s\n",
                     ms->ino, rpy->str);
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                hvfs_debug(mmll, "del _IN_%ld ok.\n", ms->ino);
            } else {
                hvfs_warning(mmll, "del _IN_%ld failed, no exists?\n", ms->ino);
            }
        }
        freeReplyObject(rpy);
    }
    goto out;
}

int __mmfs_create_root(struct mstat *ms, struct mdu_update *mu)
{
    char mstr[512];
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed\n");
        return -EINVAL;
    }

    __init_mdu(&ms->mdu, 1);
    __pack_mdu(&ms->mdu, mu);
    __mdu2str(&ms->mdu, mstr, 1);

    hvfs_info(mmll, "CREATE ROOT MDU: (pino %d)/%s -> ?,%s\n",
              MMFS_ROOT_INO, ms->name, mstr);

    {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_ %s %s %s %d",
                           g_ops[__MMFS_R_OP_CREATE_INODE].sha,
                           MMFS_INODE_MD,
                           mstr,
                           MMFS_INODE_VERSION,
                           0);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_* create for (pino %d)/%s failed w/\n%s\n",
                     MMFS_ROOT_INO, ms->name, rpy->str);
            if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                err = __mmfs_load_scripts(__MMFS_R_OP_CREATE_INODE);
                if (err) {
                    hvfs_err(mmll, "try to reload script %s failed: %d\n",
                             g_ops[__MMFS_R_OP_CREATE_INODE].opname, err);
                }
            }
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* parse the return mdu info */
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(mmll, "_IN_* create for (pino %d)/%s failed.\n",
                         MMFS_ROOT_INO, ms->name);
                err = -EINVAL;
                goto out_free;
            }
            hvfs_debug(mmll, "_IN_%ld created ok (pino %d)/%s.\n",
                       ms->mdu.ino, MMFS_ROOT_INO, ms->name);
            ms->ino = ms->mdu.ino;
            ms->pino = MMFS_ROOT_INO;
        }
    out_free:
        freeReplyObject(rpy);
    }

out:
    putRC_l1(rc);

    return err;
}

int __mmfs_create_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err = 0;
    struct redisConnection *rc = getRC_l1();
    
    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(mmll, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }
    
    hvfs_info(mmll, "Begin create new file system named as '%s'.\n", 
              msb->name);

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for create MSB\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "exists _MMFS_SB_%s", msb->name);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (!(rpy->type == REDIS_REPLY_INTEGER && rpy->integer == 0)) {
            hvfs_err(mmll, "ERROR STATE of key '_MMFS_SB_%s', reject "
                     "create file system superblock.\n", msb->name);
            err = -EEXIST;
            goto out_free1;
        }
    out_free1:
        freeReplyObject(rpy);
        if (err)
            goto out;
    }
    {
        rpy = redisCommand(rc->rc, "hset _MMFS_SB_%s name %s", 
                           msb->name, msb->name);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (!(rpy->type == REDIS_REPLY_INTEGER && rpy->integer == 1)) {
            hvfs_err(mmll, "ERROR STATE of key '_MMFS_SB_%s', reject "
                     "create file system superblock.\n", msb->name);
            err = -EEXIST;
            goto out_free2;
        }
    out_free2:
        freeReplyObject(rpy);
        if (err)
            goto out;
    }
    {
        rpy = redisCommand(rc->rc, "hmset _MMFS_SB_%s "
                           "space_quota %ld "
                           "space_used %ld "
                           "inode_quota %ld "
                           "inode_used %ld",
                           msb->name, 
                           msb->space_quota, msb->space_used,
                           msb->inode_quota, msb->inode_used);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STATUS && 
            strcmp(rpy->str, "OK") == 0) {
            err = 0;
        } else {
            err = 1;
        }
        freeReplyObject(rpy);
    }

out:
    putRC_l1(rc);

    return err;
}

int __mmfs_update_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err = 0;
    struct redisConnection *rc = getRC_l1();

    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(mmll, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for update MSB\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _MMFS_SB_%s %ld "
                           "%ld %ld %ld %ld %ld",
                           g_ops[__MMFS_R_OP_UPDATE_SB].sha,
                           msb->name,
                           msb->version,
                           msb->root_ino,
                           (s64)msb->d.space_quota,
                           (s64)msb->d.space_used,
                           (s64)msb->d.inode_quota,
                           (s64)msb->d.inode_used);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            err = EMMMETAERR;
            freeRC(rc);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL) {
            hvfs_err(mmll, "invalid arguments or version for update_sb, "
                     "this version %ld.\n", msb->version);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "invalid redis response: %s.\n", rpy->str);
            if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                err = __mmfs_load_scripts(__MMFS_R_OP_UPDATE_SB);
                if (err) {
                    hvfs_err(mmll, "try to reload script %s failed: %d\n",
                             g_ops[__MMFS_R_OP_UPDATE_SB].opname, err);
                }
            }
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            int i;
            
            for (i = 0; i < rpy->elements; i += 2) {
                char *f = rpy->element[i]->str;
                char *v = rpy->element[i + 1]->str;

                if (f != NULL && v != NULL) {
                    int l = strlen(f);

                    if (strncmp(f, "version", l > 7 ? 7 : l) == 0)
                        msb->version = atol(v);
                    else if (strncmp(f, "root_ino", l > 8 ? 8 : l) == 0)
                        msb->root_ino = atol(v);
                    else if (strncmp(f, "space_quota", l > 11 ? 11 : l) == 0)
                        msb->space_quota = atol(v);
                    else if (strncmp(f, "space_used", l > 10 ? 10 : l) == 0)
                        msb->space_used = atol(v);
                    else if (strncmp(f, "inode_quota", l > 11 ? 11 : l) == 0)
                        msb->inode_quota = atol(v);
                    else if (strncmp(f, "inode_used", l > 10 ? 10 : l) == 0)
                        msb->inode_used = atol(v);
                } else {
                    hvfs_err(mmll, "get bad MMFS SB: f %p v %p\n", f, v);
                    err = -EINVAL;
                    break;
                }
            }
            if (!err) {
                memset(&msb->d, 0, sizeof(msb->d));
                msb->d.space_used = msb->space_used;
                msb->d.inode_used = msb->inode_used;
            }
        } else {
            err = -EINVAL;
        }
        freeReplyObject(rpy);
    }
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_get_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err= 0;
    struct redisConnection *rc = getRC_l1();

    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(mmll, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for get MSB\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "hgetall _MMFS_SB_%s",
                           msb->name);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            err = EMMMETAERR;
            freeRC(rc);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            int i;

            if (rpy->elements <= 10) {
                err = -EINVAL;
            } else {
                for (i = 0; i < rpy->elements; i += 2) {
                    char *f = rpy->element[i]->str;
                    char *v = rpy->element[i + 1]->str;

                    if (f != NULL && v != NULL) {
                        int l = strlen(f);

                        if (strncmp(f, "version", l > 7 ? 7 : l) == 0)
                            msb->version = atol(v);
                        else if (strncmp(f, "root_ino", l > 8 ? 8 : l) == 0)
                            msb->root_ino = atol(v);
                        else if (strncmp(f, "space_quota", l > 11 ? 11 : l) == 0)
                            msb->space_quota = atol(v);
                        else if (strncmp(f, "space_used", l > 10 ? 10 : l) == 0)
                            msb->space_used = atol(v);
                        else if (strncmp(f, "inode_quota", l > 11 ? 11 : l) == 0)
                            msb->inode_quota = atol(v);
                        else if (strncmp(f, "inode_used", l > 10 ? 10 : l) == 0)
                            msb->inode_used = atol(v);
                    } else {
                        hvfs_err(mmll, "get bad MMFS SB: f %p v %p\n", f, v);
                        err = -EINVAL;
                        break;
                    }
                }
                if (!err) {
                    msb->flags = 0;
                    memset(&msb->d, 0, sizeof(msb->d));
                    msb->d.space_used = msb->space_used;
                    msb->d.inode_used = msb->inode_used;
                }
            }
        } else {
            err = -EINVAL;
        }
        freeReplyObject(rpy);
    }

out:
    putRC_l1(rc);

    return err;
}

static inline int __update_inode(struct redisConnection *rc, redisReply *rpy, 
                                 struct mstat *ms)
{
    char mstr[512];
    struct mdu s = ms->mdu;
    int err = 0;
    
    if (!ms->ino || ms->ino != ms->mdu.ino) {
        hvfs_err(mmll, "Invalid ino %ld (mdu %ld)\n",
                 ms->ino, ms->mdu.ino);
        err = -EINVAL;
        goto out;
    }

    s.version++;
    __mdu2str(&s, mstr, 0);

    /* Step 1: find by ino to update existing inode */
    rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %s %s %ld %s %s",
                       g_ops[__MMFS_R_OP_UPDATE_INODE].sha,
                       ms->ino,
                       MMFS_INODE_MD,
                       mstr,
                       MMFS_INODE_VERSION,
                       (u64)ms->mdu.version,
                       MMFS_INODE_BLOCK,
                       (ms->arg ? ms->arg : "nil"));
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMMETAERR;
        freeRC(rc);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "find _IN_%ld failed w/ %s\n",
                 ms->ino, rpy->str);
        if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __mmfs_load_scripts(__MMFS_R_OP_UPDATE_INODE);
            if (err) {
                hvfs_err(mmll, "try to reload script %s failed: %d\n",
                         g_ops[__MMFS_R_OP_UPDATE_INODE].opname, err);
            }
        }
        err = -EINVAL;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        switch (rpy->integer) {
        case 1:
        {
            /* updated */
            char mstr[512];

            __mdu2str(&ms->mdu, mstr, 0);
            hvfs_debug(mmll, "update inode _IN_%ld ok, version=%ld, %s\n",
                       ms->ino, (u64)s.version, mstr);
            ms->mdu.version = s.version;
            break;
        }
        case 0:
            /* failed */
            hvfs_warning(mmll, "update inode _IN_%ld failed, "
                         "version mismatch (expect %ld).\n",
                         ms->ino, (u64)ms->mdu.version);
            err = -EAGAIN;
            break;
        default:
            /* failed */
            hvfs_warning(mmll, "update inode _IN_%ld failed, key not "
                         "exists.\n ", ms->ino);
            err = -EINVAL;
        }
    }
    freeReplyObject(rpy);

out:
    return err;
}

/* Return value: 1 -> not deleted
 */
int __mmfs_unlink(u64 pino, struct mstat *ms, u32 flags)
{
    redisReply *rpy = NULL;
    int err = 0, j;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for pino %ld name %s\n",
                 pino, ms->name);
        return -EINVAL;
    }

    hvfs_debug(mmll, "DELETE dentry: (pino %ld)/%s\n",
               pino, ms->name);

    /* Step 1: delete the dentry in parent ino's hash table */
    if (flags & __MMFS_UNLINK_DENTRY) {
        /* Step 1.1 delete dentry in parent _IN_ */
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s",
                           g_ops[__MMFS_R_OP_DELETE_DENTRY].sha,
                           pino, ms->name);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "delete dentry for (pino %ld)/%s failed w/ %s\n",
                     pino, ms->name, rpy->str);
            if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                err = __mmfs_load_scripts(__MMFS_R_OP_DELETE_DENTRY);
                if (err) {
                    hvfs_err(mmll, "try to reload script %s failed: %d\n",
                             g_ops[__MMFS_R_OP_DELETE_DENTRY].opname, err);
                }
            }
            freeReplyObject(rpy);
            err = -EINVAL;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 0) {
                /* this means we can NOT find the dentry */
                hvfs_err(mmll, "find dentry for (pino %ld)/%s failed"
                         " to unlink (not exist)\n",
                         pino, ms->name);
                err = -ENOENT;
            } else {
                hvfs_err(mmll, "find dentry for (pino %ld)/%s failed"
                         " to unlink (invalid reply %ld)\n",
                         pino, ms->name, (u64)rpy->integer);
                err = -EINVAL;
            }
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(mmll, "delete dentry for (pino %ld)/%s -> ino %s\n",
                       pino, ms->name, rpy->str);
            ms->mdu.ino = atol(rpy->str);
        }

        freeReplyObject(rpy);

        if (!ms->mdu.ino || ms->mdu.ino == 1) {
            hvfs_err(mmll, "invalid ino %ld for (pino %ld)/%s\n",
                     ms->mdu.ino, pino, ms->name);
            err = -EINVAL;
            goto out;
        }
    }

    /* Step 2: delete the inode hash table for self */
    if (flags & __MMFS_UNLINK_INODE) {
        /* Step 2.1 read in the inode mdu to check nlink value */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s",
                           ms->mdu.ino, MMFS_INODE_MD);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "_IN_%ld '_MD_' does not exist.\n",
                     ms->mdu.ino);
            err = -ENOENT;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(mmll, "_IN_%ld mdu: %s\n", ms->mdu.ino, rpy->str);
            /* convert mdu_string to mdu */
            ms->ino = ms->mdu.ino;
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err || ms->ino != ms->mdu.ino) {
                hvfs_err(mmll, "Invalid MDU fro %ld (%s)\n", ms->ino, rpy->str);
                err = -EINVAL;
                freeReplyObject(rpy);
                goto out;
            }
        }

        if ((S_ISDIR(ms->mdu.mode) && ms->mdu.nlink > 2) ||
            (!S_ISDIR(ms->mdu.mode) && ms->mdu.nlink > 1)) {
            /* Step 2.2 call update inode to do nlink--*/
            ms->mdu.nlink--;
            err = __update_inode(rc, rpy, ms);
            if (err) {
                hvfs_err(mmll, "call __update_inode _IN_%ld to nlink-- "
                         "failed w/ %d\n",
                         ms->ino, err);
            } else {
                /* this means we have not delete the inode actually */
                err = 1;
            }
        } else {
            freeReplyObject(rpy);

            /* Step 2.2.pre: clear blocks if exists */
            hvfs_debug(mmll, "clear block info for _IN_%ld, blknr=%ld\n",
                       ms->mdu.ino, ms->mdu.blknr);

            for (j = 0; j < ms->mdu.blknr; j++) {
                if (j == 0) {
                    rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s",
                                       g_ops[__MMFS_R_OP_CLEAR_BLOCK].sha,
                                       ms->mdu.ino,
                                       MMFS_INODE_BLOCK);
                } else {
                    rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s_%ld",
                                       g_ops[__MMFS_R_OP_CLEAR_BLOCK].sha,
                                       ms->mdu.ino,
                                       MMFS_INODE_BLOCK,
                                       j);
                }
                if (rpy == NULL) {
                    hvfs_err(mmll, "read from MM Meta failed: %s\n",
                             rc->rc->errstr);
                    break;
                }
                if (rpy->type == REDIS_REPLY_ERROR) {
                    hvfs_err(mmll, "clear blocks for (pino %ld)/%s _IN_%ld failed w/ %s\n",
                             pino, ms->name, ms->mdu.ino, rpy->str);
                    if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
                        err = __mmfs_load_scripts(__MMFS_R_OP_CLEAR_BLOCK);
                        if (err) {
                            hvfs_err(mmll, "try to reload script %s failed: %d\n",
                                     g_ops[__MMFS_R_OP_CLEAR_BLOCK].opname, err);
                        }
                    }
                }
                if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements >= 1 &&
                    rpy->element[0]->type == REDIS_REPLY_INTEGER) {
                    switch (rpy->element[0]->integer) {
                    case 0:
                        /* not exist */
                        hvfs_err(mmll, "clear _IN_%ld block CHK=%d not exist.\n", 
                                 ms->mdu.ino, j);
                        break;
                    case 1:
                    {
                        int retried = 0;

                        /* deleted, clean MMCC ref */
                        hvfs_err(mmll, "clear _IN_%ld block CHK=%d with ref %s\n",
                                 ms->mdu.ino, j, rpy->element[1]->str);
                    retry:
                        err = mmcc_del(rpy->element[1]->str);
                        if (err < 0) {
                            if (err == -EAGAIN && !retried) {
                                retried = 1;
                                goto retry;
                            }
                            if (err == EMMMETAERR)
                                hvfs_err(mmll, "MMCC delete %s failed w/ %d\n",
                                         rpy->element[1]->str, err);
                            else
                                hvfs_debug(mmll, "MMCC delete %s still has ref %d\n",
                                           rpy->element[1]->str, -err);
                            err = 0;
                        } else if (err == 0) {
                            hvfs_debug(mmll, "MMCC delete %s no ref, not exist\n",
                                       rpy->element[1]->str);
                        } else {
                            hvfs_debug(mmll, "MMCC delete %s, deleted (%d)\n",
                                       rpy->element[1]->str, err);
                            err = 0;
                        }
                        break;
                    }
                    }
                }
                freeReplyObject(rpy);
            }

            /* Step 2.2 do truely delete now */
            rpy = redisCommand(rc->rc, "del _IN_%ld",
                               ms->mdu.ino);
            if (rpy == NULL) {
                hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
                freeRC(rc);
                goto out;
            }
            if (rpy->type == REDIS_REPLY_ERROR) {
                hvfs_err(mmll, "delete inode for (pino %ld)/%s _IN_%ld failed w/ %s\n",
                         pino, ms->name, ms->mdu.ino, rpy->str);
                freeReplyObject(rpy);
                goto out;
            }
            if (rpy->type == REDIS_REPLY_INTEGER) {
                if (rpy->integer == 1) {
                    hvfs_debug(mmll, "delete inode for (pino %ld)/%s _IN_%ld ok\n",
                               pino, ms->name, ms->mdu.ino);
                    __update_msb(MMFS_SB_U_INR, -1);
                } else {
                    hvfs_err(mmll, "delete inode for (pino %ld)/%s _IN_%ld bad (ignore)\n",
                             pino, ms->name, ms->mdu.ino);
                }
            }

            /* Step 2.3 rename fix for non DIR */
            if (!S_ISDIR(ms->mdu.mode)) {
                err = __mmfs_rename_fix(ms->mdu.ino);
                if (err) {
                    hvfs_err(mmll, "rename/link fix: ino=%ld\n", ms->mdu.ino);
                }
            }
        }

        freeReplyObject(rpy);
    }
out:
    putRC_l1(rc);

    return err;
}

/* Return: 1 -> ok, 0 -> not empty or error
 */
int __mmfs_is_empty_dir(u64 dino)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld\n", dino);
        return -EINVAL;
    }

    /* Step 1: find by ino to check any existing dentries */
    rpy = redisCommand(rc->rc, "hlen _IN_%ld", dino);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "find _IN_%ld failed w/ %s\n",
                 dino, rpy->str);
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer == 0) {
            /* invalid INODE or not exist INODE */
        } else if (rpy->integer == 1) {
            /* this must be MMFS_MDU */
            err = 1;
        } else if (rpy->integer > 1) {
            /* not empty dir? need recheck */
            char *cursor = strdup("0");
            int xnr = 0, stop = 0;

            do {
                freeReplyObject(rpy);
                rpy = redisCommand(rc->rc, "hscan _IN_%ld %s count 500",
                                   dino, cursor);
                if (rpy == NULL) {
                    hvfs_err(mmll, "read from MM Meta failed: %s\n",
                             rc->rc->errstr);
                    freeRC(rc);
                    xfree(cursor);
                    goto out;
                }
                if (rpy->type == REDIS_REPLY_ERROR) {
                    hvfs_err(mmll, "find _IN_%ld failed w/ %s\n",
                             dino, rpy->str);
                    freeReplyObject(rpy);
                    xfree(cursor);
                    goto out;
                }
                if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements == 2 &&
                    rpy->element[1]->type == REDIS_REPLY_ARRAY) {
                    int i = 0;

                    xfree(cursor);
                    cursor = strdup(rpy->element[0]->str);
                    if (strcmp(cursor, "0") == 0) {
                        stop = 1;
                    }

                    for (i = 0; i < rpy->element[1]->elements; i+= 2) {
                        char *f = rpy->element[1]->element[i]->str;
                        int l = strlen(f);

                        if (l == MMFS_I_XSIZE && strncmp(f, "__", 2) == 0) {
                            /* do more check */
                            if (strncmp(f, MMFS_INODE_NAME, l) == 0) {
                                continue;
                            } else if (strncmp(f, MMFS_INODE_MD, l) == 0) {
                                continue;
                            } else if (strncmp(f, MMFS_INODE_SYMNAME, l) == 0) {
                                continue;
                            } else if (strncmp(f, MMFS_INODE_VERSION, l) == 0) {
                                continue;
                            } else if (strncmp(f, MMFS_INODE_BLOCK, l) == 0) {
                                continue;
                            } else if (strncmp(f, MMFS_INODE_CHUNKNR, l) == 0) {
                                continue;
                            } else {
                                xnr++;
                            }
                        } else
                            xnr++;
                    }
                }
                if (xnr || stop)
                    break;
            } while (1);

            xfree(cursor);
            if (!xnr) {
                /* is empty */
                err = 1;
            }
        } else {
            /* invalid reply */
        }
    }
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_update_inode(struct mstat *ms, struct mdu_update *mu)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld\n", ms->ino);
        return -EINVAL;
    }

    if (mu != NULL) {
        __pack_mdu(&ms->mdu, mu);
    }

    err = __update_inode(rc, rpy, ms);

    putRC_l1(rc);

    return err;
}

int __mmfs_linkadd(struct mstat *ms, s32 nlink, u32 flags)
{
    struct mdu_update mu = {.valid = 0,};
    int err = 0;

    ms->mdu.nlink += nlink;
    if (flags > 0) {
        if (flags & MU_CTIME) {
            mu.valid |= MU_CTIME;
            mu.ctime = time(NULL);
        }
    }

    err = __mmfs_update_inode(ms, &mu);
    if (err) {
        hvfs_err(mmll, "call __mmfs_update_inode on _IN_%ld failed w/ %d\n",
                 ms->ino, err);
        goto out;
    }
out:
    return err;
}

/* Note that, for each read, caller must make sure it is read from ONE chunk,
 * otherwise, it will be rejected.
 *
 * Note that offset should be in-chunk offset.
 */
int __mmfs_fread_chunk(struct mstat *ms, void *data, u64 off, u64 size,
                       u64 chkid)
{
    redisReply *rpy = NULL;
    size_t rlen = 0;
    u64 chk_begin, chk_end;
    struct redisConnection *rc = NULL;
    int err = 0;

    /* validate chunk */
    chk_begin = g_msb.chunk_size * chkid;
    chk_end = g_msb.chunk_size * (chkid + 1);

    if (!data) {
        hvfs_err(mmll, "NULL buffer to read in.\n");
        return -EINVAL;
    }

    rc = getRC_l1();
    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld chkikd=%ld\n",
                 ms->ino, chkid);
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }

    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(mmll, "directory (ino %ld) has no blocks to read\n", ms->ino);
        err = -EISDIR;
        goto out;
    }

    if (chk_begin + off + size > ms->mdu.size) {
        if (chk_begin + off >= ms->mdu.size) {
            hvfs_debug(mmll, "Read offset across the boundary (%ld vs %ld)\n",
                       chk_begin + off, ms->mdu.size);
            err = -EFBIG;
            goto out;
        } else {
            /* Convention: for fuse client, it always read for some pages, we
             * should truncate the size to validate range */
            if (chk_end > ms->mdu.size)
                size = ms->mdu.size - chk_begin - off;
            else
                size = chk_end - chk_begin - off;
        }
    }

    hvfs_debug(mmll, "__mmfs_fread_chunk(%ld) off=%ld, size=%ld, mdu.size=%ld, "
               "chk(%ld)=[%ld,%ld)\n",
               ms->ino, (u64)off, (u64)size, ms->mdu.size,
               chkid, chk_begin, chk_end);

    if (likely(chkid == 0)) {
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", 
                           ms->ino, MMFS_INODE_BLOCK);
    } else {
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s_%ld", 
                           ms->ino, MMFS_INODE_BLOCK, chkid);
    }
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
        hvfs_warning(mmll, "_IN_%ld does not exist or MM error: %s\n",
                     ms->ino, rpy->str);
        if (ms->mdu.size > 0) {
            err = -EHOLE;
        } else {
            err = -EFBIG;
        }
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        void *buf = NULL;
        
        hvfs_debug(mmll, "Call mmcc_get to read _IN_%ld CHK=%ld %s\n", 
                   ms->ino, chkid, rpy->str);
        err = mmcc_get(rpy->str, &buf, &rlen);
        if (err) {
            hvfs_err(mmll, "_IN_%ld block get(%s) failed w/ %d\n",
                     ms->ino, rpy->str, err);
            goto out_free;
        }
        if (rlen < off) {
            hvfs_warning(mmll, "_IN_%ld block size %ld < off %ld\n",
                         ms->ino, rlen, off);
            err = 0;
        } else if (off + size > rlen) {
            hvfs_warning(mmll, "_IN_%ld block size %ld < request size %ld\n",
                         ms->ino, rlen, size + off);
            memcpy(data, buf + off, rlen - off);
            err = rlen - off;
        } else {
            memcpy(data, buf + off, size);
            err = size;
        }
        xfree(buf);
    }

out_free:
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_fread(struct mstat *ms, void *data, u64 off, u64 size)
{
    u64 chkid, endchk;
    s64 loff, lsize, end = size + off;
    s64 rsize, trsize = 0;
    int j;
    
    /* use [off, off+size) to identify how many chunks we need to read */
    chkid = off / g_msb.chunk_size;
    endchk = (off + size) / g_msb.chunk_size;
    endchk -= (off + size) % g_msb.chunk_size == 0 ? 1 : 0;

    hvfs_debug(mmll, "__mmfs_fread(%ld) [%ld,%ld) CHK %ld to %ld.\n",
               ms->ino, off, off + size, chkid, endchk);


    for (j = 0; chkid <= endchk; chkid++, j++) {
        loff = off - (chkid + j) * g_msb.chunk_size;
        if (loff < 0) loff = 0;
        lsize = min(g_msb.chunk_size - loff, 
                    end - (chkid + j) * g_msb.chunk_size - loff);

        rsize = __mmfs_fread_chunk(ms, data + trsize, loff, lsize, chkid);
        if (rsize < 0) {
            /* chunk read failed */
            hvfs_err(mmll, "fread(%ld) chunk(%ld) faild w/ %ld\n",
                     ms->ino, chkid, rsize);
            trsize = rsize;
            goto out;
        } else if (rsize == 0) {
            /* chunk read w/ EOF */
            break;
        } else if (rsize < lsize) {
            /* chunk read w/ partial region */
            hvfs_debug(mmll, "fread(%ld) chunk(%ld) partial rsize=%ld lsize=%ld\n",
                       ms->ino, chkid, rsize, lsize);
            trsize += rsize;
            break;
        } else if (rsize > lsize) {
            /* chunk read beyond region? */
            hvfs_err(mmll, "fread(%ld) chunk(%ld) beyond region expect %ld, "
                     "but got %ld.\n",
                     ms->ino, chkid, lsize, rsize);
            rsize = lsize;
        }
        off += rsize;
        trsize += rsize;
    }

out:
    return trsize;
}

int __mmfs_fwrite(struct mstat *ms, u32 flag, void *data, u64 size, u64 chkid)
{
    redisReply *rpy = NULL;
    char set[64], *p, name[64], key[256];
    struct mres mr;
    MD5_CTX mdContext;
    int err = 0, i;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld chkid=%ld\n",
                 ms->ino, chkid);
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(mmll, "directory (ino %ld) has no blocks to write\n", ms->ino);
        err = -EISDIR;
        goto out;
    }
    memset(&mr, 0, sizeof(mr));

    /* write the content to MMServer, and generate key */
    __mmfs_gset(ms->pino, set);

    MD5Init(&mdContext);
    MD5Update(&mdContext, (unsigned char *)data, size);
    MD5Final(&mdContext);
    
    for (i = 0, p = name; i < 16; i++) {
        p += sprintf(p, "%02x", mdContext.digest[i]);
    }
    
    snprintf(key, 255, "%s@%s", set, name);

    mmcc_put_R(key, data, size, &mr);
    if (!mr.info) {
        hvfs_err(mmll, "_IN_%ld block put failed w/ %d\n",
                 ms->ino, err);
        err = EMMMMSERR;
        goto out;
    }
    hvfs_debug(mmll, "_IN_%ld block put key=%s info=%s flag=%d pid=%ld\n",
               ms->ino, key, mr.info, mr.flag, rc->pid);

    if (likely(chkid == 0)) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %s",
                           g_ops[__MMFS_R_OP_UPDATE_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK,
                           key);
    } else {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s_%ld %s",
                           g_ops[__MMFS_R_OP_UPDATE_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK,
                           chkid,
                           key);
    }
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n",
                 ms->ino, rpy->str);
        if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __mmfs_load_scripts(__MMFS_R_OP_UPDATE_BLOCK);
            if (err) {
                hvfs_err(mmll, "try to reload script %s failed: %d\n",
                         g_ops[__MMFS_R_OP_UPDATE_BLOCK].opname, err);
            }
        }
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements == 2 &&
        rpy->element[0]->type == REDIS_REPLY_INTEGER) {
        switch (rpy->element[0]->integer) {
        case 2:
            /* update, but equal? */
            hvfs_debug(mmll, "_IN_%ld block not update %s\n",
                       ms->ino, key);
            break;
        case 1:
            /* not exist yet */
            hvfs_debug(mmll, "_IN_%ld block set    to %s\n",
                       ms->ino, key);
            break;
        case 0:
        default:
        {
            int retried = 0;

            /* updated */
            hvfs_debug(mmll, "_IN_%ld block update to %s (old %s)\n",
                       ms->ino, key, rpy->element[1]->str);
        retry:
            err = mmcc_del(rpy->element[1]->str);
            if (err < 0) {
                if (err == -EAGAIN && !retried) {
                    retried = 1;
                    goto retry;
                }
                if (err == EMMMETAERR)
                    hvfs_err(mmll, "MMCC delete %s failed w/ %d\n",
                             rpy->element[1]->str, err);
                else
                    hvfs_debug(mmll, "MMCC delete %s still has ref %d\n",
                               rpy->element[1]->str, -err);
                err = 0;
            } else if (err == 0) {
                hvfs_debug(mmll, "MMCC delete %s no ref, not exist\n",
                           rpy->element[1]->str);
            } else {
                hvfs_debug(mmll, "MMCC delete %s, deleted (%d)\n",
                           rpy->element[1]->str, err);
                err = 0;
            }
            break;
        }
        }
    }
    freeReplyObject(rpy);
    
out_free2:
    xfree(mr.info);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_fwritev(struct mstat *ms, u32 flag, struct iovec *iov, int iovlen,
                   u64 chkid)
{
    struct redisConnection *rc = NULL;
    redisReply *rpy = NULL;
    char set[64], *p, name[64], key[256];
    struct mres mr;
    MD5_CTX mdContext;
    int err = 0, i;

    if (iovlen <= 0) return 0;

    rc = getRC_l1();
    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld chkid=%ld\n",
                 ms->ino, chkid);
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(mmll, "directory (ino %ld) has no blocks to write\n", ms->ino);
        err = -EISDIR;
        goto out;
    }
    memset(&mr, 0, sizeof(mr));

    /* write the content to MMServer, and generate key */
    __mmfs_gset(ms->pino, set);

    MD5Init(&mdContext);
    for (i = 0; i < iovlen; i++) {
        MD5Update(&mdContext, (unsigned char *)iov[i].iov_base, iov[i].iov_len);
    }
    MD5Final(&mdContext);
    
    for (i = 0, p = name; i < 16; i++) {
        p += sprintf(p, "%02x", mdContext.digest[i]);
    }
    
    snprintf(key, 255, "%s@%s", set, name);

    mmcc_put_iov_R(key, iov, iovlen, &mr);
    if (!mr.info) {
        err = EMMMMSERR;
        hvfs_err(mmll, "_IN_%ld block put failed w/ %d\n",
                 ms->ino, err);
        goto out;
    }
    hvfs_debug(mmll, "_IN_%ld block put key=%s info=%s flag=%d pid=%ld\n",
               ms->ino, key, mr.info, mr.flag, rc->pid);

    if (likely(chkid == 0)) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %s",
                           g_ops[__MMFS_R_OP_UPDATE_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK,
                           key);
    } else {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s_%ld %s",
                           g_ops[__MMFS_R_OP_UPDATE_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK,
                           chkid,
                           key);
    }
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n",
                 ms->ino, rpy->str);
        if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __mmfs_load_scripts(__MMFS_R_OP_UPDATE_BLOCK);
            if (err) {
                hvfs_err(mmll, "try to reload script %s failed: %d\n",
                         g_ops[__MMFS_R_OP_UPDATE_BLOCK].opname, err);
            }
        }
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        switch (rpy->integer) {
        case 2:
            /* update, but equal? */
            hvfs_debug(mmll, "_IN_%ld block not update %s\n",
                       ms->ino, key);
            break;
        case 1:
            /* not exist yet */
            hvfs_debug(mmll, "_IN_%ld block set    to %s\n",
                       ms->ino, key);
            break;
        case 0:
        default:
        {
            int retried = 0;

            /* updated */
            hvfs_debug(mmll, "_IN_%ld block update to %s (old %s)\n",
                       ms->ino, key, rpy->element[1]->str);
        retry:
            err = mmcc_del(rpy->element[1]->str);
            if (err < 0) {
                if (err == -EAGAIN && !retried) {
                    retried = 1;
                    goto retry;
                }
                if (err == EMMMETAERR)
                    hvfs_err(mmll, "MMCC delete %s failed w/ %d\n",
                             rpy->element[1]->str, err);
                else
                    hvfs_debug(mmll, "MMCC delete %s still has ref %d\n",
                               rpy->element[1]->str, -err);
                err = 0;
            } else if (err == 0) {
                hvfs_debug(mmll, "MMCC delete %s no ref, no exist\n",
                           rpy->element[1]->str);
            } else {
                hvfs_debug(mmll, "MMCC delete %s, deleted (%d)\n",
                           rpy->element[1]->str, err);
                err = 0;
            }
            break;
        }
        }
    }
    freeReplyObject(rpy);
    
out_free2:
    xfree(mr.info);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_clr_block(struct mstat *ms, u64 chkid)
{
    struct redisConnection *rc = NULL;
    redisReply *rpy = NULL;
    int err = 0;

    rc = getRC_l1();
    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld chkid=%ld\n",
                 ms->ino, chkid);
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(mmll, "directory (ino %ld) has no blocks to write\n", ms->ino);
        err = -EISDIR;
        goto out;
    }

    hvfs_debug(mmll, "_IN_%ld block clear for CHK=%ld\n",
               ms->ino, chkid);

    if (likely(chkid == 0)) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s",
                           g_ops[__MMFS_R_OP_CLEAR_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK);
    } else {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s_%ld",
                           g_ops[__MMFS_R_OP_CLEAR_BLOCK].sha,
                           ms->ino,
                           MMFS_INODE_BLOCK,
                           chkid);
    }
    if (rpy == NULL) {
        hvfs_err(mmll, "HDEL from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n",
                 ms->ino, rpy->str);
        if (rpy->str && strncmp(rpy->str, "NOSCRIPT", 8) == 0) {
            err = __mmfs_load_scripts(__MMFS_R_OP_CLEAR_BLOCK);
            if (err) {
                hvfs_err(mmll, "try to reload script %s failed: %d\n",
                         g_ops[__MMFS_R_OP_CLEAR_BLOCK].opname, err);
            }
        }
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements >= 1 &&
        rpy->element[0]->type == REDIS_REPLY_INTEGER) {
        switch (rpy->element[0]->integer) {
        case 0:
            /* not exist */
            hvfs_debug(mmll, "_IN_%ld block CHK=%ld not exist to deleted\n",
                       ms->ino, chkid);
            break;
        case 1:
        {
            int retried = 0;

            /* deleted, clean MMCC ref */
            hvfs_debug(mmll, "_IN_%ld block CHK=%ld with ref %s\n",
                       ms->ino, chkid, rpy->element[1]->str);
        retry:
            err = mmcc_del(rpy->element[1]->str);
            if (err < 0) {
                if (err == -EAGAIN && !retried) {
                    retried = 1;
                    goto retry;
                }
                if (err == EMMMETAERR)
                    hvfs_err(mmll, "MMCC delete %s failed w/ %d\n",
                             rpy->element[1]->str, err);
                else
                    hvfs_debug(mmll, "MMCC delete %s still has ref %d\n",
                               rpy->element[1]->str, -err);
                err = 0;
            } else if (err == 0) {
                hvfs_debug(mmll, "MMCC delete %s no ref, not exist\n",
                           rpy->element[1]->str);
            } else {
                hvfs_debug(mmll, "MMCC delete %s, deleted (%d)\n",
                           rpy->element[1]->str, err);
                err = 0;
            }
            break;
        }
        }
    }
    freeReplyObject(rpy);
    
out:
    putRC_l1(rc);

    return err;
}

/* Duplication detector for directory listing
 *
 */
struct dd_entry
{
    struct hlist_node hlist;
    char *key;
};

int __dd_init(struct dup_detector *dd, int hsize)
{
    int i;

    if (hsize)
        dd->hsize = hsize;
    else
        dd->hsize = MMFS_DD_HSIZE_DEFAULT;

    dd->ht = xmalloc(dd->hsize * sizeof(struct regular_hash));
    if (!dd->ht) {
        hvfs_err(mmll, "Dup entry detector (%ld) hash table init failed\n",
                 dd->id);
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < dd->hsize; i++) {
        INIT_HLIST_HEAD(&dd->ht[i].h);
        xlock_init(&dd->ht[i].lock);
    }
    atomic_set(&dd->nr, 0);

    return 0;
}

void __dd_destroy(struct dup_detector *dd)
{
    struct regular_hash *rh;
    struct dd_entry *de;
    struct hlist_node *pos, *n;
    int i;

    hvfs_debug(mmll, "DD _IN_%ld destroy w/ %d entries.\n",
               dd->id, atomic_read(&dd->nr));

    /* need to free every DD entry */
    for (i = 0; i < dd->hsize; i++) {
        rh = dd->ht + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(de, pos, n, &rh->h, hlist) {
            hlist_del(&de->hlist);
            xfree(de->key);
            xfree(de);
        }
        xlock_unlock(&rh->lock);
    }
    xfree(dd->ht);
}

static inline
int __dd_hash(struct dup_detector *dd, const char *key)
{
    return __murmurhash2_64a(key, strlen(key), 0xe23f21f779) % dd->hsize;
}

static inline
struct dd_entry *__dde_alloc(const char *key)
{
    struct dd_entry *de;

    de = xmalloc(sizeof(*de));
    if (!de) {
        hvfs_err(mmll, "xzalloc() dd_entry failed\n");
        return NULL;
    }
    INIT_HLIST_NODE(&de->hlist);
    de->key = strdup(key);

    return de;
}

static inline
struct dd_entry *__dd_insert(struct dup_detector *dd, struct dd_entry *new)
{
    struct regular_hash *rh;
    struct dd_entry *de;
    struct hlist_node *pos, *n;
    int idx, found = 0;

    idx = __dd_hash(dd, new->key);
    rh = dd->ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(de, pos, n, &rh->h, hlist) {
        if (strcmp(new->key, de->key) == 0) {
            /* already exist */
            found = 1;
            break;
        }
    }
    if (!found) {
        hlist_add_head(&new->hlist, &rh->h);
        atomic_inc(&dd->nr);
        de = new;
    }
    xlock_unlock(&rh->lock);

    return de;
}

static inline
struct dd_entry *__dd_lookup(struct dup_detector *dd, const char *key)
{
    struct regular_hash *rh;
    struct dd_entry *de;
    struct hlist_node *pos, *n;
    int idx, found = 0;

    idx = __dd_hash(dd, key);
    rh = dd->ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(de, pos, n, &rh->h, hlist) {
        if (strcmp(de->key, key) == 0) {
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found)
        return de;
    else
        return NULL;
}

/* Return: 1 -> removed; 0 -> not removed
 */
static inline
int __dd_remove(struct dup_detector *dd, const char *key)
{
    struct regular_hash *rh;
    struct dd_entry *de;
    struct hlist_node *pos, *n;
    int idx, removed = 0;

    idx = __dd_hash(dd, key);
    rh = dd->ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(de, pos, n, &rh->h, hlist) {
        if (strcmp(de->key, key) == 0) {
            hlist_del(&de->hlist);
            atomic_dec(&dd->nr);
            xfree(de->key);
            xfree(de);
            removed = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    return removed;
}

int __mmfs_readdir(mmfs_dir_t *dir)
{
    struct mstat ms = {0,};
    redisReply *rpy = NULL;
    int err = 0, idx;

    if (dir->dino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided\n", dir->dino);
        return -EINVAL;
    }

    ms.ino = dir->dino;
    err = __mmfs_stat(0, &ms);
    if (err) {
        hvfs_err(mmll, "__mmfs_stat(%ld) failed w/ %d\n",
                 dir->dino, err);
        return err;
    }

    if (!S_ISDIR(ms.mdu.mode)) {
        hvfs_err(mmll, "_IN_%ld is not a directory\n", dir->dino);
        return -ENOTDIR;
    }

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for ino %ld\n", dir->dino);
        return -EINVAL;
    }

    /* hscan the dentries */
    rpy = redisCommand(rc->rc, "hscan _IN_%ld %s count 500",
                       dir->dino, dir->cursor == NULL ? "0" : dir->cursor);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error\n",
                 dir->dino);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements == 2 &&
        rpy->element[1]->type == REDIS_REPLY_ARRAY) {
        int i = 0, j = 0, tlen = 0, clen = 0;

        xfree(dir->cursor);
        dir->cursor = strdup(rpy->element[0]->str);
        
        for (i = 0; i < rpy->element[1]->elements; i += 2) {
            char *f = rpy->element[1]->element[i]->str;
            char *v = rpy->element[1]->element[i + 1]->str;
            int l = strlen(f);

            if (l == MMFS_I_XSIZE && strncmp(f, "__", 2) == 0) {
                /* do more check */
                if (strncmp(f, MMFS_INODE_NAME, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_MD, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_SYMNAME, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_VERSION, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_BLOCK, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_CHUNKNR, l) == 0) {
                    continue;
                }
            }
            /* BUG-XXX: ignore .fuse_hiddenXXXX dentries, these entries are
             * generated by rename when the source files are unlinked under
             * openning.
             *
             * Under file openning, if file's dir entry is unlinked, it
             * actually is renamed.
             */
            if (l > 12 && strncmp(f, ".fuse_hidden", 12) == 0) {
                continue;
            }
            /* BUG-XXX: detect duplications here.
             *
             * Redis might return duplicate entries on modify hash table
             * during scanning.
             */
            {
                struct dd_entry *de = __dde_alloc(f);

                if (de) {
                    if (__dd_insert(&dir->dd, de) != de) {
                        xfree(de->key);
                        xfree(de);
                        continue;
                    }
                }
            }
            /* ok, do record now */
            clen = sizeof(struct dentry_info) + l;
            void *t = xrealloc(dir->di, tlen + clen);
            if (!t) {
                hvfs_err(mmll, "xrealloc(%d) failed\n", tlen + clen);
                err = -ENOMEM;
                freeReplyObject(rpy);
                xfree(dir->di);
                dir->di = NULL;
                goto out;
            }
            dir->di = t;
            struct dentry_info *di = (t + tlen);

            di->ino = atoi(v);
            di->mode = 0;
            di->namelen = l;
            memcpy(di->name, f, l);
            tlen += clen;
            j++;
        }
        dir->csize = j;
    }
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    /* get inode mode for each dentry now */
    struct dentry_info *di = dir->di;

    for (idx = 0; idx < dir->csize; idx++) {
        ms.ino = di->ino;
        err = __mmfs_stat(0, &ms);
        if (err) {
            hvfs_err(mmll, "__mmfs_stat() _IN_%ld to get mode failed w/ %d\n",
                     ms.ino, err);
        } else
            di->mode = ms.mdu.mode;
        di = (void *)di + sizeof(*di) + di->namelen;
    }

    return err;
}

int __mmfs_inc_shadow_dir(u64 dino)
{
    redisReply *rpy = NULL;
    int err = 0;

    if (dino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided.\n", dino);
        return -EINVAL;
    }

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for ino %ld\n", dino);
        return -EINVAL;
    }

    /* inc the shadow dir count */
    rpy = redisCommand(rc->rc, "hincrby _SD_%s %ld 1",
                       g_msb.name, dino);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n", 
                 dino, rpy->str);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_dec_shadow_dir(u64 dino)
{
    redisReply *rpy = NULL;
    int err = 0, deleted = 0;

    if (dino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided.\n", dino);
        return -EINVAL;
    }

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld\n", dino);
        return -EINVAL;
    }

    hvfs_debug(mmll, "dec shadow dino %ld\n", dino);

    /* dec the shadow dir count */
    rpy = redisCommand(rc->rc, "hincrby _SD_%s %ld -1",
                       g_msb.name, dino);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n", 
                 dino, rpy->str);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer <= 0) {
            deleted = 1;
        }
    }
    freeReplyObject(rpy);

    if (deleted){
        rpy = redisCommand(rc->rc, "hdel _SD_%s %ld",
                           g_msb.name, dino);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "delete inode %ld in _SD_ failed w/ %s\n",
                     dino, rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        freeReplyObject(rpy);
        
        /* BUG-XXX: check if this inode exists, otherwise delete set.
         *
         * Detected by xfstests generic 013, fsstress.2, data missing and
         * system locked up.
         */
        rpy = redisCommand(rc->rc, "exists _IN_%ld", dino);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "check inode %ld exists failed w/ %s\n",
                     dino, rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 0) {
                /* ok, free to recycle space now */
                char set[256];
                
                sprintf(set, "o%ld", dino);
                err = mmcc_del_set(set);
                if (err) {
                    hvfs_err(mmll, "do MMCC set %s delete failed, manual delete.\n",
                             set);
                }
                hvfs_debug(mmll, "MMCC set %s deleted (not shadow).\n", set);
            }
        }
        freeReplyObject(rpy);
    }
out:
    putRC_l1(rc);

    return err;
}

/* Return value: 0->not shadow; 1->is shadow
 */
int __mmfs_is_shadow_dir(u64 dino)
{
    redisReply *rpy = NULL;
    int err = 0;

    if (dino <= 0) {
        hvfs_err(mmll, "invalid ino %ld provided.\n", dino);
        return -EINVAL;
    }

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld\n", dino);
        return -EINVAL;
    }

    /* check if it is a shadow dir */
    rpy = redisCommand(rc->rc, "hget _SD_%s %ld", g_msb.name, dino);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "_IN_%ld does not exist or MM error: %s\n", 
                 dino, rpy->str);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_NIL) {
        /* ok, there is no dino exist, return 0 */
        err = 0;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        err = atoi(rpy->str);
        if (err < 0) err = 0;
    }
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_rename_log(u64 ino, u64 opino, u64 npino)
{
    redisReply *rpy = NULL;
    time_t ts = time(NULL);
    char entry[256];
    int err = 0;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld "
                 "opino %ld npino %ld\n",
                 ino, opino, npino);
        return -EINVAL;
    }

    hvfs_debug(mmll, "rename log: ino %ld rename from P(%ld) to P(%ld).\n",
               ino, opino, npino);

    snprintf(entry, 256, "%ld|%ld|%ld|%ld", ino, (u64)ts, opino, npino);

    rpy = redisCommand(rc->rc, "hset _RL_%s %s 1",
                       g_msb.name, entry);
    if (rpy == NULL) {
        hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmll, "set rename log failed w %s\n", rpy->str);
        err = -EINVAL;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer != 1) {
            hvfs_err(mmll, "deuplicate rename log entry?!\n");
        }
    }
    freeReplyObject(rpy);
out:
    putRC_l1(rc);

    return err;
}

int __mmfs_rename_fix(u64 ino)
{
    redisReply *rpy = NULL;
    char entry[256], *cursor = NULL;
    char **fields = NULL;
    int err = 0, nr = 0, i, j;

    struct redisConnection *rc = getRC_l1();

    if (!rc) {
        hvfs_err(mmll, "getRC_l1() failed for _IN_%ld\n", ino);
        return -EINVAL;
    }

    hvfs_debug(mmll, "rename fix: ino %ld.\n", ino);

    snprintf(entry, 256, "%ld|*", ino);

    do {
        rpy = redisCommand(rc->rc, "hscan _RL_%s %s match %s",
                           g_msb.name, 
                           (cursor == NULL ? "0" : cursor), entry);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "scan rename fix failed w/ %s\n", rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements == 2 &&
            rpy->element[1]->type == REDIS_REPLY_ARRAY) {
            int alloc = 1;
            char **__t;

            xfree(cursor);
            cursor = strdup(rpy->element[0]->str);

            if (rpy->element[1]->elements < 2) {
                goto free;
            }
            __t = xrealloc(fields, sizeof(char *) * 
                           (rpy->element[1]->elements / 2 + nr));
            if (!__t) {
                hvfs_err(mmll, "xrealloc() fields' pointer array failed, "
                         "elems=%ld, nr=%d.\n",
                         rpy->element[1]->elements / 2, nr);
                alloc = 0;
            } else {
                fields = __t;
            }

            for (i = 0; i < rpy->element[1]->elements; i+= 2) {
                char *n = strdup(rpy->element[1]->element[i]->str);
                char *p = NULL, *q = n, *s = NULL;

                if (alloc)
                    fields[nr++] = strdup(n);

                /* parse the rename log */
                for (j = 0; j < 4; j++, n = NULL) {
                    p = strtok_r(n, "|", &s);
                    if (!p) {
                        break;
                    }
                    switch (j) {
                    case 2:
                        /* old pino */
                        err = __mmfs_dec_shadow_dir(atol(p));
                        if (err) {
                            hvfs_err(mmll, "dec shadow dir %s faild w/ %d\n",
                                     p, err);
                        }
                        break;
                    default:
                        break;
                    }
                }
                xfree(q);
            }
        }
    free:
        freeReplyObject(rpy);
        if (cursor == NULL || strcmp(cursor, "0") == 0)
            break;
    } while (1);

    for (i = 0; i < nr; i++) {
        hvfs_err(mmll, "Try to del %s\n", fields[i]);
        rpy = redisCommand(rc->rc, "hdel _RL_%s %s",
                           g_msb.name, fields[i]);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "hdel _RL_ field %s failed: %s\n",
                     fields[i], rpy->str);
        }
        freeReplyObject(rpy);
    }
    for (i = 0; i < nr; i++) {
        xfree(fields[i]);
    }
out:
    if (cursor)
        xfree(cursor);
    putRC_l1(rc);

    return err;
out_free:
    for (i = 0; i < nr; i++) {
        xfree(fields[i]);
    }
    goto out;
}

int __mmfs_ci_pack(struct __mmfs_client_info *ci, char *key, char *buf)
{
    int n = 0;

    sprintf(key, "%s.%s", ci->hostname, ci->namespace);
    
    n += sprintf(buf + n, "%ld,%s,%s,%s,%s,%ld,%dU,%dF,%dD,",
                 (u64)time(NULL),
                 ci->hostname, ci->namespace,
                 ci->ip, ci->md5, (u64)ci->born,
                 ci->used_pages, ci->free_pages,
                 ci->dirty_pages);
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.getattr));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.readlink));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.mknod));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.mkdir));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.unlink));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.rmdir));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.symlink));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.rename));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.link));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.chmod));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.chown));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.truncate));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.utime));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.open));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.read));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.write));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.statfs_plus));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.release));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.fsync));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.opendir));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.readdir_plus));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.release_dir));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.create_plus));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.ftruncate));
    n += sprintf(buf + n, "%ld,", atomic64_read(&ci->os.a_fsync));

    return n;
}

int __mmfs_client_info(struct __mmfs_client_info *ci)
{
    char key[256];
    char buf[1024];
    int err = 0;

    err = __mmfs_ci_pack(ci, key, buf);
    if (err < 0) {
        hvfs_err(mmll, "pack client info failed w/ %d\n", err);
        return err;
    }
    if (err > 0) {
        redisReply *rpy = NULL;
        struct redisConnection *rc = getRC_l1();

        if (!rc) {
            hvfs_err(mmll, "getRC_l1() failed for CI\n");
            return -EINVAL;
        }

        rpy = redisCommand(rc->rc, "hset mmfs.client.info %s %s",
                           key, buf);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "hset mmfs.client.info failed w/ %s\n", rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            /* it is ok */
        }
        freeReplyObject(rpy);

        /* publish this message to mm.info.mmfs.client */
        rpy = redisCommand(rc->rc, "publish mm.info.mmfs.client %s,%s",
                           key, buf);
        if (rpy == NULL) {
            hvfs_err(mmll, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmll, "hset mmfs.client.info failed w/ %s\n", rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            /* it is ok */
        }
        freeReplyObject(rpy);
    out:
        putRC_l1(rc);
        err = 0;
    }

    return err;
}

static void __convert_host_to_ip(char *host, char *ip)
{
    struct hostent *he;
    int copied = 0, i;

    he = gethostbyname(host);
    if (!he) {
        hvfs_err(mmll, "gethostbyname(%s) failed w/ %s(%d)\n",
                 host, strerror(errno), errno);
        goto out;
    }
    for (i = 0; i < he->h_length; i++) {
        struct sockaddr_in sa;

        sa.sin_addr.s_addr = *((unsigned long *)he->h_addr_list[i]);
        inet_ntop(AF_INET, &(sa.sin_addr), ip, NI_MAXHOST);
        if (strlen(ip) > 0) {
            /* ok, use this IP */
            copied = 1;
            break;
        }
    }

out:    
    if (!copied) {
        strcpy(ip, host);
    }
}

int __mmfs_renew_ci(struct __mmfs_client_info *ci, int type)
{
    char hostname[128];
    char ip[NI_MAXHOST];
    char buildinfo[256];
    int err = 0;
    
    if (!ci->hostname && !ci->namespace) {
        memset(ci, 0, sizeof(*ci));

        err = gethostname(hostname, 128);
        if (err) {
            hvfs_err(mmll, "gethostname() failed w/ %d(%s)\n", 
                     errno, strerror(errno));
            err = -errno;
            goto out;
        }
        __convert_host_to_ip(hostname, ip);
        sprintf(buildinfo, "SHA %s dirty %s build_id %s",
                MMFS_GIT_SHA1,
                MMFS_GIT_DIRTY,
                MMFS_BUILD_ID);

        ci->hostname = strdup(hostname);
        ci->namespace = mmfs_fuse_mgr.namespace;
        ci->ip = strdup(ip);
        ci->md5 = strdup(buildinfo);
        ci->born = time(NULL);
    } else {
        switch (type) {
        case OP_NONE:
        default:
            break;
        case OP_GETATTR:
            atomic64_inc(&ci->os.getattr);
            break;
        case OP_READLINK:
            atomic64_inc(&ci->os.readlink);
            break;
        case OP_MKNOD:
            atomic64_inc(&ci->os.mknod);
            break;
        case OP_MKDIR:
            atomic64_inc(&ci->os.mkdir);
            break;
        case OP_UNLINK:
            atomic64_inc(&ci->os.unlink);
            break;
        case OP_RMDIR:
            atomic64_inc(&ci->os.rmdir);
            break;
        case OP_SYMLINK:
            atomic64_inc(&ci->os.symlink);
            break;
        case OP_RENAME:
            atomic64_inc(&ci->os.rename);
            break;
        case OP_LINK:
            atomic64_inc(&ci->os.link);
            break;
        case OP_CHMOD:
            atomic64_inc(&ci->os.chmod);
            break;
        case OP_CHOWN:
            atomic64_inc(&ci->os.chown);
            break;
        case OP_TRUNCATE:
            atomic64_inc(&ci->os.truncate);
            break;
        case OP_UTIME:
            atomic64_inc(&ci->os.utime);
            break;
        case OP_OPEN:
            atomic64_inc(&ci->os.open);
            break;
        case OP_READ:
            atomic64_inc(&ci->os.read);
            break;
        case OP_WRITE:
            atomic64_inc(&ci->os.write);
            break;
        case OP_STATFS_PLUS:
            atomic64_inc(&ci->os.statfs_plus);
            break;
        case OP_RELEASE:
            atomic64_inc(&ci->os.release);
            break;
        case OP_FSYNC:
            atomic64_inc(&ci->os.fsync);
            break;
        case OP_OPENDIR:
            atomic64_inc(&ci->os.opendir);
            break;
        case OP_READDIR_PLUS:
            atomic64_inc(&ci->os.readdir_plus);
            break;
        case OP_RELEASE_DIR:
            atomic64_inc(&ci->os.release_dir);
            break;
        case OP_CREATE_PLUS:
            atomic64_inc(&ci->os.create_plus);
            break;
        case OP_FTRUNCATE:
            atomic64_inc(&ci->os.ftruncate);
            break;
        case OP_A_FSYNC:
            atomic64_inc(&ci->os.a_fsync);
            break;
        }
    }

out:
    return err;
}
