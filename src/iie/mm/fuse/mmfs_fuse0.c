#include "mmfs.c"

HVFS_TRACING_DEFINE_FILE();

int main(int argc, char *argv[])
{
    int err = 0, i, j;

    mmfs_init(NULL);
            
    hvfs_lib_tracing_flags = 0xf0000001;

    for (i = 0; i < 5; i++) {
        struct stat stbuf;
        
        err = mmfs_getattr("/", &stbuf);
        if (err) {
            hvfs_err(lib, "mmfs_getattr() failed w/ %d\n", err);
            goto out;
        }
    }

    for (i = 0; i < 1; i++) {
        char buf[1024];
        struct stat stbuf;

        err = mmfs_getattr("/hello", &stbuf);
        if (err == -ENOENT) {
            err = mmfs_symlink("/world", "/hello");
            if (err) {
                hvfs_err(lib, "mmfs_symlink() failed w/ %d\n", err);
                goto out;
            }
        }
        err = mmfs_link("/hello", "/hello_hardlink");
        if (err) {
            hvfs_err(lib, "mmfs_link() failed w/ %d\n", err);
            goto out;
        }
        err = mmfs_readlink("/hello", buf, sizeof(buf));
        if (err) {
            hvfs_err(lib, "mmfs_readlink() failed w/ %d\n", err);
            goto out;
        }
        hvfs_info(lib, "GOT linkname '%s'\n", buf);
        err = mmfs_unlink("/hello");
        if (err) {
            hvfs_err(lib, "mmfs_unlink() failed w/ %d\n", err);
            goto out;
        }
        err = mmfs_chmod("/hello_hardlink", 9999);
        if (err) {
            hvfs_err(lib, "mmfs_chmod() failed w/ %d\n", err);
            goto out;
        }
        err = mmfs_unlink("/hello_hardlink");
        if (err) {
            hvfs_err(lib, "mmfs_unlink() failed w/ %d\n", err);
            goto out;
        }
    }

    for (i = 0; i < 1; i++) {
        struct stat stbuf;

        err = mmfs_getattr("/world", &stbuf);
        if (err == -ENOENT) {
            err = mmfs_mknod("/world", 999, 100);
            if (err) {
                hvfs_err(lib, "mmfs_mknod() failed w/ %d\n", err);
                goto out;
            }
        
            err = mmfs_getattr("/world", &stbuf);
            if (err) {
                hvfs_err(lib, "mmfs_getattr() failed w/ %d\n", err);
                goto out;
            }
        }
        err = mmfs_chmod("/world", 6666);
        if (err) {
            hvfs_err(lib, "mmfs_chmod() failed w/ %d\n", err);
            goto out;
        }
        err = mmfs_unlink("/world");
        if (err) {
            hvfs_err(lib, "mmfs_unlink() failed w/ %d\n", err);
            goto out;
        }
    }

    hvfs_info(lib, "... do  MKDIR test now ...\n");
    
    for (i = 0; i < 1; i++) {
        struct stat stbuf;

        err = mmfs_getattr("/world_dir", &stbuf);
        if (err == -ENOENT) {
            err = mmfs_mkdir("/world_dir", 666);
            if (err) {
                hvfs_err(lib, "mmfs_mknod() failed w/ %d\n", err);
                goto out;
            }
        
            err = mmfs_getattr("/world_dir", &stbuf);
            if (err) {
                hvfs_err(lib, "mmfs_getattr() failed w/ %d\n", err);
                goto out;
            }
        }

        struct fuse_file_info fi = {0,};

        for (j = 0; j < 10; j++) {
            char pathname[256];

            sprintf(pathname, "/world_dir/test-%d", j);

            /* touch one file in this directory */
            err = mmfs_create_plus(pathname, 0666, &fi);
            if (err && err != -EEXIST) {
                hvfs_err(lib, "mmfs_create_plus() failed w/ %d\n", err);
                goto out;
            }
        }
        
        /* list the directory */
        err = mmfs_opendir("/world_dir", &fi);
        if (err) {
            hvfs_err(lib, "mmfs_opendir() failed w/ %d\n", err);
            goto out;
        }

        char *buf[1024];
        err = mmfs_readdir_plus("/world_dir", buf, NULL, 0, &fi);
        if (err) {
            hvfs_err(lib, "mmfs_readdir_plus() failed w/ %d\n", err);
            goto out;
        }

        err = mmfs_release_dir("/world_dir", &fi);
        if (err) {
            hvfs_err(lib, "mmfs_release_dir() failed w/ %d\n", err);
            goto out;
        }

        for (j = 0; j < 10; j++) {
            char pathname[256];

            sprintf(pathname, "/world_dir/test-%d", j);

            /* delete the file */
            err = mmfs_unlink(pathname);
            if (err) {
                hvfs_err(lib, "mmfs_unlink() failed w/ %d\n", err);
                goto out;
            }
        }

        err = mmfs_rmdir("/world_dir");
        if (err) {
            hvfs_err(lib, "mmfs_rmdir() failed w/ %d\n", err);
            goto out;
        }
    }

    hvfs_info(lib, "... end MKDIR test now ...\n");

    hvfs_info(lib, "... begin RENAME test now ...\n");
    {
        struct fuse_file_info fi = {0,};
        struct stat stbuf;
        struct timespec tv[2];
        char buf[1024];
        size_t size = 10;
        off_t offset = 0;

        err = mmfs_getattr("/rename_source", &stbuf);
        if (err == -ENOENT) {
            err = mmfs_create_plus("/rename_source", 0664, &fi);
            if (err && err != -EEXIST) {
                hvfs_err(lib, "mmfs_create_plus() failed w/ %d\n", err);
                goto out;
            }
        }
        err = mmfs_chown("/rename_source", 0, 0);
        if (err) {
            hvfs_err(lib, "mmfs_chown() failed w/ %d\n", err);
            goto out;
        }

        tv[0].tv_sec = time(NULL);
        tv[1].tv_sec = time(NULL);
        err = mmfs_utimens("/rename_source", tv);
        if (err) {
            hvfs_err(lib, "mmfs_utime() failed w/ %d\n", err);
            goto out;
        }

        err = mmfs_open("/rename_source", &fi);
        if (err) {
            hvfs_err(lib, "mmfs_open() failed w/ %d\n", err);
            goto out;
        }

#if 0
        sprintf(buf, "hello, mmfs2!\n");
        size = strlen(buf);
        offset = 8100;
        err = mmfs_write("/rename_source", buf, size, offset, &fi);
        if (err < 0) {
            hvfs_err(lib, "mmfs_write() failed w/ %d\n", err);
            goto out;
        } else {
            hvfs_info(lib, "mmfs_write(/rename_source) = %d DATA.\n", err);
        }
#endif

        memset(buf, 0, sizeof(buf));
        err = mmfs_read("/rename_source", buf, size, offset, &fi);
        if (err < 0) {
            hvfs_err(lib, "mmfs_read() failed w/ %d\n", err);
            goto out;
        } else {
            hvfs_info(lib, "mmfs_read(/rename_source) = %d DATA(%s).\n", 
                      err, buf);
        }
        
        memset(buf, 0, sizeof(buf));
        size = 15;
        offset = 6;
        err = mmfs_read("/rename_source", buf, size, offset, &fi);
        if (err < 0) {
            hvfs_err(lib, "mmfs_read() failed w/ %d\n", err);
            goto out;
        } else {
            hvfs_info(lib, "mmfs_read(/rename_source) = %d DATA(%s).\n", 
                      err, buf);
        }
        
        memset(buf, 0, sizeof(buf));
        size = 100;
        offset = 4100;
        err = mmfs_read("/rename_source", buf, size, offset, &fi);
        if (err < 0) {
            hvfs_err(lib, "mmfs_read() failed w/ %d\n", err);
            goto out;
        } else {
            hvfs_info(lib, "mmfs_read(/rename_source) = %d DATA(%s).\n", 
                      err, buf);
        }

        err = mmfs_release("/rename_source", &fi);
        if (err) {
            hvfs_err(lib, "mmfs_release() failed w/ %d\n", err);
            goto out;
        }

        hvfs_info(lib, ".......... BEGIN RENAME .......\n");
        err = mmfs_rename("/rename_source", "/rename_target");
        if (err) {
            hvfs_err(lib, "mmfs_rename() failed w/ %d\n", err);
            goto out;
        }
    }

out:
    mmfs_destroy(NULL);
    
    return err;
}

