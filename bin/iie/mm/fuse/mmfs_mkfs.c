/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-08-04 20:13:43 macan>
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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <pthread.h>
#include <getopt.h>

#include "mmfs.c"

HVFS_TRACING_DEFINE_FILE();

static int mmfs_mkfs(struct mmfs_sb *msb)
{
    struct mstat ms = {0,};
    struct mdu_update mu;
    int err = 0;

    err = __mmfs_create_sb(msb);
    if (err) {
        hvfs_err(lib, "do internal create superblock '%s' failed w/ %d\n",
                 msb->name, err);
        goto out;
    }
    
    ms.name = "_ROOT_INO_";
    ms.ino = 0;
    mu.valid = MU_MODE | MU_ATIME | MU_CTIME | MU_MTIME;
    mu.mode = MMFS_DEFAULT_DIR_UMASK | S_IFDIR;
    mu.atime = mu.ctime = mu.mtime = time(NULL);
    ms.mdu.flags |= MMFS_MDU_DIR;

    err = __mmfs_create_root(&ms, &mu);
    if (err) {
        hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                 ms.name, err);
        goto out;
    }

    msb->root_ino = ms.ino;
    memset(&msb->d, 0, sizeof(msb->d));
    err = __mmfs_update_sb(msb);
    if (err) {
        hvfs_err(lib, "do internal update superblock '%s' failed w/ %d\n",
                 msb->name, err);
        goto out;
    }

out:
    return err;
}

void do_help()
{
    hvfs_plain(lib,
               "Version 1.0.0a\n"
               "Copyright (c) 2015 IIE and Ma Can <ml.macana@gmail.com>\n\n"
               "Arguments:\n"
               "-h, -? --help           print this help.\n"
               "-n, --name              Set file system name.\n"
               "-T, --tune              Set tune flag.\n"
               "-S, --space_quota       Set file system space quota, in bytes.\n"
               "-I, --inode_quota       Set file system inode quota, in #.\n"
        );
}

int main(int argc, char *argv[])
{
    struct mmfs_sb msb = {0,};
    char *shortflags = "h?n:S:I:T";
    char *value, *uris = NULL;
    struct option longflags[] = {
        {"help", no_argument, 0, 'h'},
        {"tune", no_argument, 0, 'T'},
        {"name", required_argument, 0, 'n'},
        {"space_quota", required_argument, 0, 'S'},
        {"inode_quota", required_argument, 0, 'I'},
    };
    int is_tune = 0, err= 0;
    
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
            break;
        case 'T':
            is_tune = 1;
            break;
        case 'n':
            msb.name = strdup(optarg);
            break;
        case 'S':
            msb.space_quota = atol(optarg);
            break;
        case 'I':
            msb.inode_quota = atol(optarg);
            break;
        }
    }

    mmfs_debug_mode(1);

    value = getenv("uris");
    if (value) {
        uris = strdup(value);
    }

    if (msb.name == NULL || strlen(msb.name) == 0) {
        msb.name = strdup("default");
    }
    mmfs_fuse_mgr.namespace = msb.name;
    mmfs_fuse_mgr.uris = uris;

    if (!is_tune)
        mmfs_fuse_mgr.ismkfs = 1;

    mmfs_init(NULL);

    if (!is_tune) {
        err = mmfs_mkfs(&msb);
        if (err) {
            switch (err) {
            case -EEXIST:
                hvfs_err(lib, "File System Already Exists?\n");
                break;
            case EMMMETAERR:
                hvfs_err(lib, "Redis connection failed.\n");
                break;
            case -EINVAL:
                hvfs_err(lib, "Invalid arguments.\n");
                break;
            }
            hvfs_err(lib, "mmfs_mkfs(/) failed w/ %d\n", err);
            goto out;
        }

        hvfs_info(lib, "Create file system '%s' success.\n"
                  "\tSpace Quota: %ld B\n"
                  "\tInode Quota: %ld\n",
                  msb.name,
                  msb.space_quota,
                  msb.inode_quota);
    } else {
        /* tune mmfs */
        u64 space_quota = msb.space_quota;
        u64 inode_quota = msb.inode_quota;
        
        err = __mmfs_get_sb(&msb);
        if (err) {
            hvfs_err(lib, "File System '%s' does not exist or error=%d.\n",
                     msb.name, err);
            goto out;
        }
        memset(&msb.d, 0, sizeof(msb.d));
        msb.d.space_quota = space_quota - msb.space_quota;
        msb.d.inode_quota = inode_quota - msb.inode_quota;
        err = __mmfs_update_sb(&msb);
        if (err) {
            hvfs_err(lib, "Update file system '%s' superblock failed w/ %d\n",
                     msb.name, err);
            goto out;
        }
    }
    
out:
    xfree(msb.name);
    mmfs_destroy(NULL);
    
    return err;
}
