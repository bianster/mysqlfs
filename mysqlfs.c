/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: mysqlfs.c,v 1.17 2006/09/13 10:54:37 ludvigm Exp $

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
#include <fuse/fuse.h>
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
    log_printf(LOG_D_CALL, "mysqlfs_getattr(\"%s\")\n", path);

    memset(stbuf, 0, sizeof(struct stat));

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_getattr(conn->mysql, path, stbuf);

    if(ret){
        if (ret != -ENOENT)
            log_printf(LOG_ERROR, "Error: query_getattr()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }else{
        long inode = query_inode(conn->mysql, path);
        if(inode < 0){
            log_printf(LOG_ERROR, "Error: query_inode()\n");
            mysqlfs_pool_return(mysql_pool, conn);
            return inode;
        }

        stbuf->st_size = query_size(conn->mysql, inode);
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
    long inode;

    log_printf(LOG_D_CALL, "mysqlfs_readdir(\"%s\")\n", path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if(inode < 0){
        log_printf(LOG_ERROR, "Error: query_inode()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    
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
    long parent_inode;
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

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    parent_inode = query_inode(conn->mysql, dir_path);
    if(parent_inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_mknod(conn->mysql, path, mode, rdev, parent_inode, S_ISREG(mode) || S_ISLNK(mode));
    if(ret < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    mysqlfs_pool_return(mysql_pool, conn);
    return 0;
}

static int mysqlfs_mkdir(const char *path, mode_t mode){
    int ret;
    MYSQL_CONN *conn;
    long inode;
    char dir_path[PATH_MAX];

    log_printf(LOG_D_CALL, "mysqlfs_mkdir(\"%s\", 0%o)\n", path, mode);
    
    if(!(strlen(path) < PATH_MAX)){
        log_printf(LOG_ERROR, "Error: Filename too long\n");
        return -ENAMETOOLONG;
    }
    strncpy(dir_path, path, PATH_MAX);
    dirname(dir_path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, dir_path);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_mkdir(conn->mysql, path, mode, inode);
    if(ret < 0){
        log_printf(LOG_ERROR, "Error: query_mkdir()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    mysqlfs_pool_return(mysql_pool, conn);
    return 0;
}

static int mysqlfs_unlink(const char *path)
{
    int ret;
    long inode, parent, nlinks;
    char name[PATH_MAX];
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_unlink(\"%s\")\n", path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_inode_full(conn->mysql, path, name, sizeof(name),
			   &inode, &parent, &nlinks);
    if (ret < 0) {
        if (ret != -ENOENT)
            log_printf(LOG_ERROR, "Error: query_inode_full(%s): %s\n",
		       path, strerror(ret));
	goto err_out;
    }

    ret = query_rmdirentry(conn->mysql, name, parent);
    if (ret < 0) {
        log_printf(LOG_ERROR, "Error: query_rmdirentry()\n");
	goto err_out;
    }

    /* Only the last unlink() must set deleted flag. 
     * This is a shortcut - query_set_deleted() wouldn't
     * set the flag if there is still an existing direntry
     * anyway. But we'll save some DB processing here. */
    if (nlinks > 1)
        return 0;
    
    ret = query_set_deleted(conn->mysql, inode);
    if (ret < 0) {
        log_printf(LOG_ERROR, "Error: query_set_deleted()\n");
	goto err_out;
    }

    ret = query_purge_deleted(conn->mysql, inode);
    if (ret < 0) {
        log_printf(LOG_ERROR, "Error: query_purge_deleted()\n");
	goto err_out;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;

err_out:
    mysqlfs_pool_return(mysql_pool, conn);
    return ret;
}

static int mysqlfs_chmod(const char* path, mode_t mode)
{
    int ret;
    long inode;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_chmod(\"%s\", 0%3o)\n", path, mode);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if (inode < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    ret = query_chmod(conn->mysql, inode, mode);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_chmod()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_chown(const char *path, uid_t uid, gid_t gid)
{
    int ret;
    long inode;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_chown(\"%s\", %ld, %ld)\n", path, uid, gid);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if (inode < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    ret = query_chown(conn->mysql, inode, uid, gid);
    if(ret){
        log_printf(LOG_ERROR, "Error: query_chown()\n");
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

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_truncate(conn->mysql, path, length);
    if (ret < 0) {
        log_printf(LOG_ERROR, "Error: query_length()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_utime(const char *path, struct utimbuf *time)
{
    int ret;
    long inode;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysql_utime(\"%s\")\n", path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if (inode < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    ret = query_utime(conn->mysql, inode, time);
    if (ret < 0) {
        log_printf(LOG_ERROR, "Error: query_utime()\n");
        mysqlfs_pool_return(mysql_pool, conn);
        return -EIO;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_open(const char *path, struct fuse_file_info *fi)
{
    MYSQL_CONN *conn;
    long inode;
    int ret;

    log_printf(LOG_D_CALL, "mysqlfs_open(\"%s\")\n", path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    /* Save inode for future use. Lets us skip path->inode translation.  */
    fi->fh = inode;

    log_printf(LOG_D_OTHER, "inode(\"%s\") = %d\n", path, fi->fh);

    ret = query_inuse_inc(conn->mysql, inode, 1);
    if (ret < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_read(\"%s\" %zu@%llu)\n", path, size, offset);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_read(conn->mysql, fi->fh, buf, size, offset);
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_write(const char *path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_write(\"%s\" %zu@%lld)\n", path, size, offset);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_write(conn->mysql, fi->fh, buf, size, offset);
    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_release(const char *path, struct fuse_file_info *fi)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "mysqlfs_release(\"%s\")\n", path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_inuse_inc(conn->mysql, fi->fh, -1);
    if (ret < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    ret = query_purge_deleted(conn->mysql, fi->fh);
    if (ret < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_link(const char *from, const char *to)
{
    int ret;
    long inode, new_parent;
    MYSQL_CONN *conn;
    char *tmp, *name, esc_name[PATH_MAX * 2];

    log_printf(LOG_D_CALL, "link(%s, %s)\n", from, to);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, from);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return inode;
    }

    tmp = strdup(to);
    name = dirname(tmp);
    new_parent = query_inode(conn->mysql, name);
    free(tmp);
    if (new_parent < 0) {
        mysqlfs_pool_return(mysql_pool, conn);
        return new_parent;
    }

    tmp = strdup(to);
    name = basename(tmp);
    mysql_real_escape_string(conn->mysql, esc_name, name, strlen(name));
    free(tmp);

    ret = query_mkdirentry(conn->mysql, inode, esc_name, new_parent);
    if(ret < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return ret;
    }

    mysqlfs_pool_return(mysql_pool, conn);

    return 0;
}

static int mysqlfs_symlink(const char *from, const char *to)
{
    int ret;
    int inode;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "%s(\"%s\" -> \"%s\")\n", __func__, from, to);

    ret = mysqlfs_mknod(to, S_IFLNK | 0755, 0);
    if (ret < 0)
      return ret;

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, to);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    ret = query_write(conn->mysql, inode, from, strlen(from), 0);
    if (ret > 0) ret = 0;

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static int mysqlfs_readlink(const char *path, char *buf, size_t size)
{
    int ret;
    long inode;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "%s(\"%s\")\n", __func__, path);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    inode = query_inode(conn->mysql, path);
    if(inode < 0){
        mysqlfs_pool_return(mysql_pool, conn);
        return -ENOENT;
    }

    memset (buf, 0, size);
    ret = query_read(conn->mysql, inode, buf, size, 0);
    log_printf(LOG_DEBUG, "readlink(%s): %s [%zd -> %d]\n", path, buf, size, ret);
    mysqlfs_pool_return(mysql_pool, conn);

    if (ret > 0) ret = 0;
    return ret;
}

static int mysqlfs_rename(const char *from, const char *to)
{
    int ret;
    MYSQL_CONN *conn;

    log_printf(LOG_D_CALL, "%s(%s -> %s)\n", __func__, from, to);

    // FIXME: This should be wrapped in a transaction!!!
    mysqlfs_unlink(to);

    if ((conn = mysqlfs_pool_get(mysql_pool)) == NULL)
      return -EMFILE;

    ret = query_rename(conn->mysql, from, to);

    mysqlfs_pool_return(mysql_pool, conn);

    return ret;
}

static struct fuse_operations mysqlfs_oper = {
    .getattr	= mysqlfs_getattr,
    .readdir	= mysqlfs_readdir,
    .mknod	= mysqlfs_mknod,
    .mkdir	= mysqlfs_mkdir,
    .unlink	= mysqlfs_unlink,
    .rmdir	= mysqlfs_unlink,
    .chmod	= mysqlfs_chmod,
    .chown	= mysqlfs_chown,
    .truncate	= mysqlfs_truncate,
    .utime	= mysqlfs_utime,
    .open	= mysqlfs_open,
    .read	= mysqlfs_read,
    .write	= mysqlfs_write,
    .release	= mysqlfs_release,
    .link	= mysqlfs_link,
    .symlink	= mysqlfs_symlink,
    .readlink	= mysqlfs_readlink,
    .rename	= mysqlfs_rename,
};

void usage(){
    fprintf(stderr,
            "usage: mysqlfs -ohost=host -ouser=user -opasswd=passwd "
            "-odatabase=database ./mountpoint\n");
}

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

    if(!strncmp(arg, "port=", strlen("port="))){
        str = strchr(arg, '=') + 1;
	if (sscanf(str, "%u", &opt->port) != 1)
	    return -1;
        return 0;
    }

    if(!strncmp(arg, "socket=", strlen("socket="))){
        str = strchr(arg, '=') + 1;
        opt->socket = str;
        return 0;
    }

    /* Read defaults from specified group in my.cnf
     * Command line options still have precedence.  */
    if(!strncmp(arg, "mycnf_group=", strlen("mycnf_group="))){
        str = strchr(arg, '=') + 1;
        opt->mycnf_group = str;
        return 0;
    }

    if(!strncmp(arg, "connection=", strlen("connection="))){
        str = strchr(arg, '=') + 1;
        opt->connection = atoi(str);
        return 0;
    }

    if(!strncmp(arg, "logfile=", strlen("logfile="))){
        str = strchr(arg, '=') + 1;
        opt->logfile = str;
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
    opt.connection = 25;
    opt.logfile = "mysqlfs.log";

#ifdef DEBUG
    mtrace();
#endif

    fuse_opt_parse(&args, &opt, NULL, mysqlfs_opt_proc);

    log_file = log_init(opt.logfile, 1);

    mysql_pool = mysqlfs_pool_init(&opt);
    if(!mysql_pool){
        log_printf(LOG_ERROR, "Error: mysqlfs_pool_init()\n");
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
