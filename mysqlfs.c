/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: mysqlfs.c,v 1.9 2006/09/04 11:43:29 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <fuse.h>
#include <mysql/mysql.h>
#include <pthread.h>
#include <sys/stat.h>

#ifdef DEBUG
#include <mcheck.h>
#endif

#include "mysqlfs.h"
#include "query.h"
#include "pool.h"
#include "log.h"

#define PATH_MAX 1024

static MYSQL_POOL* mysql_pool;

static int mysqlfs_getattr(const char *path, struct stat *stbuf)
{
    int ret;
    MYSQL_CONN *conn;

    // This is called far too often
    //log_printf(LOG_D_CALL, "mysqlfs_getattr(\"%s\")\n", path);

    memset(stbuf, 0, sizeof(struct stat));

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_getattr(conn->mysql, path, stbuf);
    if(ret){
        if (ret != -ENOENT)
            log_printf(LOG_ERROR, "Error: query_getattr()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }else{
        stbuf->st_size = query_size(conn->mysql, path);
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi)
{
    (void) offset;
    (void) fi;
    int ret;
    MYSQL_CONN *conn;
    int inode;

    log_printf(LOG_D_CALL, "mysqlfs_readdir(\"%s\")\n", path);
    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    inode = query_inode(conn->mysql, path);
    if(inode < 1){
        log_printf(LOG_ERROR, "Error: query_inode()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    
    log_printf(LOG_D_OTHER, "inode2(\"%s\") = %d\n", path, inode);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    ret = query_readdir(conn->mysql, inode, buf, filler);
    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int ret;
    MYSQL_CONN *conn;
    int inode;
    char dir_path[PATH_MAX];

    log_printf(LOG_D_CALL, "mysqlfs_mknod(\"%s\", %o): %s\n", path, mode,
	       S_ISREG(mode) ? "file" :
	       S_ISDIR(mode) ? "directory" :
	       S_ISLNK(mode) ? "symlink" :
	       "other");

    if(!(strlen(path) < PATH_MAX)){
        log_printf(LOG_ERROR, "Error: Filename too long\n");
        return -ENAMETOOLONG;
    }
    strncpy(dir_path, path, PATH_MAX);
    dirname(dir_path);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    inode = query_inode(conn->mysql, dir_path);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_mknod(conn->mysql, path, mode, rdev, inode, S_ISREG(mode) || S_ISLNK(mode));
    if(ret){
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);
    return 0;
}

static int mysqlfs_mkdir(const char *path, mode_t mode){
    int ret;
    MYSQL_CONN *conn;
    int inode;
    char dir_path[PATH_MAX];

    log_printf(LOG_D_CALL, "mysqlfs_mkdir(\"%s\")\n", path);
    
    if(!(strlen(path) < PATH_MAX)){
        log_printf(LOG_ERROR, "Error: Filename too long\n");
        return -ENAMETOOLONG;
    }
    strncpy(dir_path, path, PATH_MAX);
    dirname(dir_path);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    inode = query_inode(conn->mysql, dir_path);
    if(inode < 1){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_mkdir(conn->mysql, path, mode, inode);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_mkdir()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);
    return 0;
}

static int mysqlfs_unlink(const char *path){
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_unlink(\"%s\")\n", path);
    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_delete(conn->mysql, path);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_unlink()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_rmdir(const char *path)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_rmdir(\"%s\")\n", path);
    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_delete(conn->mysql, path);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_rmdir()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_chmod(const char* path, mode_t mode)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_chmod(\"%s\")\n", path);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_chmod(conn->mysql, path, mode);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_chmod()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_truncate(const char* path, off_t length)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_truncate(\"%s\"): len=%lld\n", path, length);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_truncate(conn->mysql, path, length);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_length()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
    return 0;
}

static int mysqlfs_utime(const char *path, struct utimbuf *time)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_utime(\"%s\")\n", path);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_utime(conn->mysql, path, time);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_utime()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_open(const char *path, struct fuse_file_info *fi)
{
    MYSQL_CONN *conn;
    int inode;

    log_printf(LOG_D_CALL, "mysqlfs_open(\"%s\")\n", path);

    conn = mysqlfs_pool_get(mysql_pool);    
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    inode = query_inode(conn->mysql, path);
    if(inode < 1){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    /* Save inode for future use. Lets us skip path->inode translation.  */
    fi->fh = inode;

    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_read(\"%s\")\n", path);
    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_read(conn->mysql, path, buf, size, offset);
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_write(const char *path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_write(\"%s\")\n", path);
    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    ret = query_write(conn->mysql, path, buf, size, offset);
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_flush(const char *path, struct fuse_file_info *fi)
{
    log_printf(LOG_D_CALL, "mysqlfs_flush(\"%s\")\n", path);
    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);
    return 0;
}

static int mysqlfs_release(const char *path, struct fuse_file_info *fi)
{
    log_printf(LOG_D_CALL, "mysqlfs_release(\"%s\")\n", path);
    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);
    return 0;
}

static int mysqlfs_symlink(const char *from, const char *to)
{
    int ret;
    int inode;
    MYSQL_CONN *conn;

    ret = mysqlfs_mknod(to, S_IFLNK | 0755, 0);
    log_printf(LOG_DEBUG, "symlink(%s, %s): mknod=%d\n", from, to, ret);
    if (ret < 0)
      return ret;

    conn = mysqlfs_pool_get(mysql_pool);    
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    inode = query_inode(conn->mysql, to);
    if(inode < 1){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_write(conn->mysql, to, from, strlen(from), 0);
    if (ret > 0)
      ret = 0;
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_readlink(const char *path, char *buf, size_t size)
{
    int ret;
    MYSQL_CONN *conn;

    conn = mysqlfs_pool_get(mysql_pool);
    if(!conn){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_get()\n");
        return -EMFILE;
    }

    memset (buf, size, 0);
    ret = query_read(conn->mysql, path, buf, size, 0);
    log_printf(LOG_DEBUG, "readlink(%s): %s [%zd -> %d]\n", path, buf, size, ret);
    mysqlfs_pool_return(mysql_pool, conn);

    if (ret > 0) ret = 0;
    return ret;
}

static struct fuse_operations mysqlfs_oper = {
    .getattr	= mysqlfs_getattr,
    .readdir	= mysqlfs_readdir,
    .mknod	= mysqlfs_mknod,
    .mkdir	= mysqlfs_mkdir,
    .unlink	= mysqlfs_unlink,
    .rmdir	= mysqlfs_rmdir,
    .chmod	= mysqlfs_chmod,
    .truncate	= mysqlfs_truncate,
    .utime	= mysqlfs_utime,
    .open	= mysqlfs_open,
    .read	= mysqlfs_read,
    .write	= mysqlfs_write,
    .flush	= mysqlfs_flush,
    .release	= mysqlfs_release,
    .symlink	= mysqlfs_symlink,
    .readlink	= mysqlfs_readlink,
};

void usage(){
    fprintf(stderr,
            "usage: mysqlfs -ohost=host -ouser=user -opasswd=passwd "
            "-odatabase=database ./mountpoint\n");
}

/* MLUDVIG_BUILD ... temporary workaround, the following can't be
 * compiled on my workstation with libfuse 2.4.2-0ubuntu3 */
#ifndef MLUDVIG_BUILD
static int mysqlfs_opt_proc(void *data, const char *arg, int key,
                            struct fuse_args *outargs){
    MYSQLFS_OPT *opt = (MYSQLFS_OPT*)data;
    char *str;

    if(key != FUSE_OPT_KEY_OPT){
        fuse_opt_add_arg(outargs, arg);
        return 0;
    }

    if(!strncmp(arg, "host=", strlen("host="))){
        str = strchr(arg, '=') + 1;
        opt->host = str;
        return 0;
    }

    if(!strncmp(arg, "user=", strlen("user="))){
        str = strchr(arg, '=') + 1;
        opt->user = str;
        return 0;
    }

    if(!strncmp(arg, "password=", strlen("password="))){
        str = strchr(arg, '=') + 1;
        opt->passwd = str;
        return 0;
    }

    if(!strncmp(arg, "database=", strlen("database="))){
        str = strchr(arg, '=') + 1;
        opt->db = str;
        return 0;
    }

    if(!strncmp(arg, "connection=", strlen("connection="))){
        str = strchr(arg, '=') + 1;
        opt->connection = atoi(str);
        return 0;
    }

    fuse_opt_add_arg(outargs, arg);
    return 0;
}

/*
 * main
 */
int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    static MYSQLFS_OPT opt;

    /* default param */
    opt.connection = 5;

#ifdef DEBUG
    mtrace();
#endif

    fuse_opt_parse(&args, &opt, NULL, mysqlfs_opt_proc);

    if(!opt.host || !opt.user || !opt.passwd || !opt.db){
        usage();
        fuse_opt_free_args(&args);
        return EXIT_FAILURE;
    }

    mysql_pool = mysqlfs_pool_init(&opt);
    if(!mysql_pool){
        fprintf(stderr, "Error: mysqlfs_pool_init()\n");
        fuse_opt_free_args(&args);
        return EXIT_FAILURE;        
    }

    fuse_main(args.argc, args.argv, &mysqlfs_oper);
    fuse_opt_free_args(&args);

    mysqlfs_pool_print(mysql_pool);
    mysqlfs_pool_free(mysql_pool);

#ifdef DEBUG
    muntrace();
#endif
  
    return EXIT_SUCCESS;
}

#else

/*
 * main
 */
int main(int argc, char *argv[])
{
    static MYSQLFS_OPT opt;

    /* default param */
    opt.connection = 5;

    opt.host = "localhost";
    opt.user = "fuse";
    opt.passwd = "fuse";
    opt.db = "mysqlfs";

    log_file = log_init("mysqlfs.log", 1);

    mysql_pool = mysqlfs_pool_init(&opt);
    if(!mysql_pool){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_init()\n");
        return EXIT_FAILURE;        
    }

    fuse_main(argc, argv, &mysqlfs_oper);

    mysqlfs_pool_print(mysql_pool);
    mysqlfs_pool_free(mysql_pool);

    return EXIT_SUCCESS;
}
#endif /* MLUDVIG_BUILD */
