/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-10-30 18:27:14 macan>
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

#ifndef __MMFS_H__
#define __MMFS_H__

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "mmfs_ll.h"

/* which fuse version should we use? */
#warning "We need FUSE version 2.9"
#define FUSE_USE_VERSION 29
#include <fuse.h>
#include <fuse/fuse_lowlevel.h>

extern struct fuse_operations mmfs_ops;
extern size_t g_pagesize;
extern struct __mmfs_fuse_mgr mmfs_fuse_mgr;
extern struct mmfs_sb g_msb;

void mmfs_debug_mode(int enable);

#endif
