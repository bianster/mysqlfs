/*
  MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>
#include <fuse.h>
#include <mysql.h>
#include "query.h"

static char *host = NULL;
static char *user = NULL;
static char *passwd = NULL;
static char *db = NULL;

static int mysqlfs_getattr(const char *path, struct stat *stbuf)
{
    int ret;
    MYSQL *mysql;
    MYSQL *conn;

    fprintf(stderr, "mysqlfs_getattr()\n");
    fprintf(stderr, "path=%s\n", path);

    memset(stbuf, 0, sizeof(struct stat));

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }

    ret = query_getattr(mysql, path, stbuf);
    if(ret){
        fprintf(stderr, "Error: query_getattr()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    stbuf->st_size = query_size(mysql, path);
    mysql_close(mysql);

    return 0;
}

static int mysqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi)
{
    (void) offset;
    (void) fi;
    int ret;
    MYSQL *mysql;
    MYSQL *conn;
    int inode;

    fprintf(stderr, "mysqlfs_readdir()\n");
    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }

    inode = query_inode(mysql, path);
    if(inode < 1){
        fprintf(stderr, "Error: query_inode()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    ret = query_readdir(mysql, inode, buf, filler);

    mysql_close(mysql);

    return 0;
}

static int mysqlfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int ret;
    MYSQL *mysql;
    MYSQL *conn;
    int inode;
    char *dir;

    fprintf(stderr, "mysqlfs_mknod()\n");

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }

    dir = strdup(path);
    if(!dir){
        fprintf(stderr, "Error: strdup()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    dirname(dir);
    inode = query_inode(mysql, dir);
    free(dir);

    ret = query_mknod(mysql, path, mode, rdev, inode);

    mysql_close(mysql);

    /*
      if (S_ISFIFO(mode))
      res = mkfifo(path, mode);
      else
      res = mknod(path, mode, rdev);
      if (res == -1)
      return -errno;
    */
    return 0;
}

static int mysqlfs_mkdir(const char *path, mode_t mode){
    int ret;
    
    MYSQL *mysql;
    MYSQL *conn;
    int inode;
    char* dir;

    fprintf(stderr, "mysqlfs_mkdir()\n");

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }

    printf("ping=%d\n", mysql_ping(mysql));

    dir = strdup(path);
    if(!dir){
        fprintf(stderr, "Error: strdup()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    dirname(dir);
    inode = query_inode(mysql, dir);
    free(dir);
    if(inode < 1){
        fprintf(stderr, "Error: query_inode()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    ret = query_mkdir(mysql, path, mode, inode);
    if(ret){
        fprintf(stderr, "Error: query_mkdir()\n");
        mysql_close(mysql);
        return -ENOENT;
    }

    mysql_close(mysql);

    return 0;
}

static int mysqlfs_unlink(const char *path){
    MYSQL *mysql;
    MYSQL *conn;

    fprintf(stderr, "mysqlfs_unlink()\n");
    mysql = mysql_init(NULL);

    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }

    query_delete(mysql, path);

    mysql_close(mysql);
    return 0;
}

static int mysqlfs_rmdir(const char *path){
    MYSQL *mysql;
    MYSQL *conn;

    fprintf(stderr, "mysqlfs_rmdir()\n");
    mysql = mysql_init(NULL);

    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }
    query_delete(mysql, path);
    mysql_close(mysql);
    return 0;
}



static int mysqlfs_flush(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "mysql_flush()\n");
    return 0;
}

static int mysqlfs_truncate(const char* path, off_t off)
{
    fprintf(stderr, "mysql_truncate()\n");
    printf("off=%lld\n", off);
    return 0;
}

static int mysqlfs_utime(const char *path, struct utimbuf *time){

    fprintf(stderr, "mysql_utime()\n");
    fprintf(stderr, "atime=%s", ctime(&time->actime));
    fprintf(stderr, "mtime=%s", ctime(&time->modtime));

    return 0;
}

static int mysqlfs_open(const char *path, struct fuse_file_info *fi)
{
    MYSQL *mysql;
    MYSQL *conn;
    int inode;

    fprintf(stderr, "mysqlfs_open()\n");

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }
    
    inode = query_inode(mysql, path);
    printf("inode=%d\n", inode);
    if(inode < 1){
        mysql_close(mysql);
        return -ENOENT;
    }

    mysql_close(mysql);
    
/*
    if((fi->flags & 3) != O_RDONLY)
        return -EACCES;
*/

    return 0;
}

static int mysqlfs_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi)
{
    int ret;
    MYSQL *mysql;
    MYSQL *conn;

    fprintf(stderr, "mysqlfs_read()\n");
    printf("size=%d\n", size);
    printf("offset=%lld\n", offset);

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }
    
    ret = query_read(mysql, path, buf, size, offset);

    mysql_close(mysql);

    return ret;
}

static int mysqlfs_write(const char *path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi)
{
    int ret;
    MYSQL *mysql;
    MYSQL *conn;

    fprintf(stderr, "mysqlfs_write()\n");
    printf("size=%d\n", size);
    printf("offset=%lld\n", offset);

    mysql = mysql_init(NULL);
    if(!mysql){
        fprintf(stderr, "ERROR: mysql_init()\n");
        return -ENOENT;
    }

    conn = mysql_real_connect(mysql, host, user, passwd, db, 0, NULL, 0);
    if(!conn){
        fprintf(stderr, "Error: mysql_real_connect()\n");
        fprintf(stderr, "mysql_error: %s\n", mysql_error(mysql));
        return -ENOENT;
    }
    
    ret = query_write(mysql, path, buf, size, offset);

    mysql_close(mysql);

    return ret;
}

/*
static int mysqlfs_release(const char* path, struct fuse_file_info* fi)
{
    fprintf(stderr, "mysqlfs_release()\n");
    return 0;
}
*/

static struct fuse_operations mysqlfs_oper = {
    .getattr = mysqlfs_getattr,
    .readdir = mysqlfs_readdir,
    .mknod	 = mysqlfs_mknod,
    .mkdir	 = mysqlfs_mkdir,
    .unlink  = mysqlfs_unlink,
    .rmdir   = mysqlfs_rmdir,
    .truncate= mysqlfs_truncate,
    .utime   = mysqlfs_utime,
    .open	 = mysqlfs_open,
    .read	 = mysqlfs_read,
    .write	 = mysqlfs_write,
    .flush   = mysqlfs_flush,
//    .release= mysqlfs_release,
};

void usage(){
    fprintf(stderr, "usage: mysqlfs -hhost -uuser -ppasswd database.table ./mountpoint\n");
}

char *mysql_fs_parse_arg(const char *arg, const char *key){
    char * ret;

    if(!strncmp(arg, key, strlen(key))){
        ret  = strdup(strchr(arg, '=') + 1);
        printf("hoge=%s\n", ret);
        printf("hoge=%p\n", ret);

        return ret;
    }

    return NULL;
}

static int mysqlfs_opt_proc(void *data, const char *arg, int key,
                            struct fuse_args *outargs){

    if(key != FUSE_OPT_KEY_OPT){
        fuse_opt_add_arg(outargs, arg);
        return 0;
    }

    if(!strncmp(arg, "host=", strlen("host="))){

        host = strchr(arg, '=') + 1;

        return 0;
    }

    if(!strncmp(arg, "user=", strlen("user="))){
        user = strchr(arg, '=') + 1;
        return 0;
    }

    if(!strncmp(arg, "password=", strlen("password="))){
        passwd = strchr(arg, '=') + 1;
        return 0;
    }

    if(!strncmp(arg, "database=", strlen("database="))){
        db = strchr(arg, '=') + 1;
        return 0;
    }

    fuse_opt_add_arg(outargs, arg);
    return 0;
}

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_opt_parse(&args, NULL, NULL, mysqlfs_opt_proc);

    if(!host || !user || !passwd || !db){
        usage();
        return EXIT_FAILURE;
    }

    fuse_main(args.argc, args.argv, &mysqlfs_oper);

    return EXIT_SUCCESS;
}
