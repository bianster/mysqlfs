/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: query.c,v 1.12 2006/09/13 02:58:23 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>
#include <fuse/fuse.h>
#include <mysql/mysql.h>

#include "mysqlfs.h"
#include "query.h"
#include "log.h"

#define SQL_MAX 10240

int query_getattr(MYSQL *mysql, const char *path, struct stat *stbuf)
{
    int ret;
    long inode, nlinks;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    ret = query_inode_full(mysql, path, NULL, 0, &inode, NULL, &nlinks);
    if (ret < 0)
      return ret;

    snprintf(sql, SQL_MAX,
             "SELECT inode, mode, uid, gid, UNIX_TIMESTAMP(atime), UNIX_TIMESTAMP(mtime) "
             "FROM inodes WHERE inode=%ld",
             inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1){
        mysql_free_result(result);
        return -ENOENT;
    }
    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    stbuf->st_ino = inode;
    stbuf->st_mode = atoi(row[1]);
    stbuf->st_uid = atol(row[2]);
    stbuf->st_gid = atol(row[3]);
    stbuf->st_atime = atol(row[4]);
    stbuf->st_mtime = atol(row[5]);
    stbuf->st_nlink = nlinks;

    mysql_free_result(result);

    return 0;
}

int query_inode_full(MYSQL *mysql, const char *path, char *name, size_t name_len,
		      long *inode, long *parent, long *nlinks)
{
    long ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    int depth = 0;
    char *pathptr = strdup(path), *pathptr_saved = pathptr;
    char *nameptr, *saveptr = NULL;
    char sql_from[SQL_MAX], sql_where[SQL_MAX];
    char *sql_from_end = sql_from, *sql_where_end = sql_where;
    char esc_name[PATH_MAX * 2];

    // TODO: Handle too long or too nested paths that don't fit in SQL_MAX!!!
    sql_from_end += snprintf(sql_from_end, SQL_MAX, "tree AS t0");
    sql_where_end += snprintf(sql_where_end, SQL_MAX, "t0.parent IS NULL");
    while ((nameptr = strtok_r(pathptr, "/", &saveptr)) != NULL) {
        if (depth++ == 0) {
	  pathptr = NULL;
	}

        mysql_real_escape_string(mysql, esc_name, nameptr, strlen(nameptr));
	sql_from_end += snprintf(sql_from_end, SQL_MAX, " LEFT JOIN tree AS t%d ON t%d.inode = t%d.parent",
		 depth, depth-1, depth);
	sql_where_end += snprintf(sql_where_end, SQL_MAX, " AND t%d.name = '%s'",
		 depth, esc_name);
    }
    free(pathptr_saved);

    // TODO: Only run subquery when pointer to nlinks != NULL, otherwise we don't need it.
    snprintf(sql, SQL_MAX, "SELECT t%d.inode, t%d.name, t%d.parent, "
	     		   "       (SELECT COUNT(inode) FROM tree AS t%d WHERE t%d.inode=t%d.inode) "
			   "               AS nlinks "
	     		   "FROM %s WHERE %s",
	     depth, depth, depth, 
	     depth+1, depth+1, depth,
	     sql_from, sql_where);
    log_printf(LOG_D_OTHER, "SQL=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1){
        log_printf(LOG_ERROR, "ERROR: mysql_num_rows()=%d\n",
		   mysql_num_rows(result));
        mysql_free_result(result);
        return -ENOENT;
    }

    row = mysql_fetch_row(result);
    if(!row){
        log_printf(LOG_ERROR, "ERROR: mysql_fetch_row()\n");
        return -EIO;
    }
    log_printf(LOG_D_OTHER, "query_inode(path='%s') => %s, %s, %s, %s\n",
	       path, row[0], row[1], row[2], row[3]);
    
    if (inode)
        *inode = atol(row[0]);
    if (name)
        snprintf(name, name_len, "%s", row[1]);
    if (parent)
        *parent = row[2] ? atol(row[2]) : -1;	/* parent may be NULL */
    if (nlinks)
        *nlinks = atol(row[3]);

    mysql_free_result(result);

    return 0;
}

long query_inode(MYSQL *mysql, const char *path)
{
    long inode, ret;
    
    ret = query_inode_full(mysql, path, NULL, 0, &inode, NULL, NULL);
    if (ret < 0)
      return ret;
    return inode;
}

int query_truncate(MYSQL *mysql, const char *path, off_t length)
{
    int ret;
    char sql[SQL_MAX];

    long inode = query_inode(mysql, path);
    if (inode < 0)
      return inode;

    snprintf(sql, SQL_MAX,
             "UPDATE inodes LEFT JOIN data ON inodes.inode = data.inode SET data=RPAD(data, %lld, '\\0') WHERE inodes.inode=%ld",
             length, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    return 0;

err_out:
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

int query_mkdirentry(MYSQL *mysql, long inode, const char *name, long parent)
{
    int ret;
    char sql[SQL_MAX];
    char esc_name[PATH_MAX * 2];

    mysql_real_escape_string(mysql, esc_name, name, strlen(name));
    snprintf(sql, SQL_MAX,
             "INSERT INTO tree (name, parent, inode) VALUES ('%s', %ld, %ld)",
             esc_name, parent, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    return 0;
}

int query_rmdirentry(MYSQL *mysql, const char *name, long parent)
{
    int ret;
    char sql[SQL_MAX];
    char esc_name[PATH_MAX * 2];

    mysql_real_escape_string(mysql, esc_name, name, strlen(name));
    snprintf(sql, SQL_MAX,
             "DELETE FROM tree WHERE name='%s' AND parent=%ld",
             esc_name, parent);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    return 0;
}

long query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                long parent, int alloc_data)
{
    int ret;
    char sql[SQL_MAX];
    long new_inode_number = 0;
    char *name, esc_name[PATH_MAX * 2];

    name = strrchr(path, '/');
    if (!name || *++name == '\0')
        return -ENOENT;

    mysql_real_escape_string(mysql, esc_name, name, strlen(name));
    snprintf(sql, SQL_MAX,
             "INSERT INTO tree (name, parent) VALUES ('%s', %ld)",
             esc_name, parent);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    new_inode_number = mysql_insert_id(mysql);

    snprintf(sql, SQL_MAX,
             "INSERT INTO inodes(inode, mode, uid, gid, atime, ctime, mtime)"
             "VALUES(%ld, %d, %d, %d, NOW(), NOW(), NOW())",
             new_inode_number, mode,
	     fuse_get_context()->uid, fuse_get_context()->gid);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    if (alloc_data) {
        snprintf(sql, SQL_MAX,
                 "INSERT INTO data SET inode=%ld", new_inode_number);

        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret = mysql_query(mysql, sql);
        if (ret)
          goto err_out;
    }
    return new_inode_number;

err_out:
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

long query_mkdir(MYSQL *mysql, const char *path, mode_t mode, long parent)
{
    return query_mknod(mysql, path, S_IFDIR | mode, 0, parent, 0);
}

int query_readdir(MYSQL *mysql, long inode, void *buf, fuse_fill_dir_t filler)
{
    int ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    snprintf(sql, sizeof(sql), "SELECT name FROM tree WHERE parent = '%ld'",
             inode);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    while((row = mysql_fetch_row(result)) != NULL){
        filler(buf, (char*)basename(row[0]), NULL, 0);
    }

    mysql_free_result(result);

    return ret;
}

int query_chmod(MYSQL *mysql, long inode, mode_t mode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE inodes SET mode=%d WHERE inode=%ld",
             mode, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

int query_chown(MYSQL *mysql, long inode, uid_t uid, gid_t gid)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE inodes SET uid=%u, gid=%u WHERE inode=%ld",
             uid, gid, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

int query_utime(MYSQL *mysql, long inode, struct utimbuf *time)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE inodes "
             "SET atime=FROM_UNIXTIME(%ld), mtime=FROM_UNIXTIME(%ld) "
             "WHERE inode=%lu",
             time->actime, time->modtime, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

int query_read(MYSQL *mysql, long inode, const char *buf, size_t size,
               off_t offset)
{
    int ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;
    unsigned long length;

    snprintf(sql, SQL_MAX,
             "SELECT SUBSTRING(data, %lld, %d) FROM data WHERE inode=%ld",
             offset + 1, size, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 && mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }
    
    row = mysql_fetch_row(result);
    if(!row){
        mysql_free_result(result);
        return -EIO;
    }
    
    length = mysql_fetch_lengths(result)[0];
    memcpy((void*)buf, row[0], length);

    mysql_free_result(result);

    return length;
}

int query_write(MYSQL *mysql, long inode, const char *data, size_t size,
                off_t offset)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND bind[1];
    char sql[SQL_MAX];
    size_t current_size = query_size(mysql, inode);

    stmt = mysql_stmt_init(mysql);
    if (!stmt)
    {
        log_printf(LOG_ERROR, "mysql_stmt_init(), out of memory\n");
	return -EIO;
    }

    memset(bind, 0, sizeof(bind));
    if (offset == 0 && current_size == 0) {
        snprintf(sql, SQL_MAX,
                 "UPDATE inodes LEFT JOIN data ON inodes.inode = data.inode SET data=?, size=%zu WHERE inodes.inode=%ld", size, inode);
    } else if (offset == current_size) {
        snprintf(sql, sizeof(sql),
                 "UPDATE inodes LEFT JOIN data ON inodes.inode = data.inode SET data=CONCAT(data, ?), size=size+%zu WHERE inodes.inode=%ld", size, inode);
    } else {
        size_t pos, new_size;
        pos = snprintf(sql, sizeof(sql),
		 "UPDATE inodes LEFT JOIN data ON inodes.inode = data.inode SET data=CONCAT(");
	if (offset > 0)
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "RPAD(IF(ISNULL(data),'', data), %llu, '\\0'),", offset);
	pos += snprintf(sql + pos, sizeof(sql) - pos, "?,");
	new_size = offset + size;
	if (offset + size < current_size) {
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "SUBSTRING(data FROM %llu),", offset + size + 1);
	    new_size = current_size;
	}
	sql[--pos] = '\0';	/* Remove the trailing comma. */
	pos += snprintf(sql + pos, sizeof(sql) - pos, "), size=%zu WHERE inodes.inode=%ld",
			new_size, inode);
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    if (mysql_stmt_prepare(stmt, sql, strlen(sql))) {
	log_printf(LOG_ERROR, "mysql_stmt_prepare() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    if (mysql_stmt_param_count(stmt) != 1) {
      log_printf(LOG_ERROR, "%s(): stmt_param_count=%d, expected 1\n", __func__, mysql_stmt_param_count(stmt));
      return -EIO;
    }
    bind[0].buffer_type= MYSQL_TYPE_LONG_BLOB;
    bind[0].buffer= (char *)data;
    bind[0].is_null= 0;
    bind[0].length= (unsigned long *)&size;

    if (mysql_stmt_bind_param(stmt, bind)) {
	log_printf(LOG_ERROR, "mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    /*
    if (!mysql_stmt_send_long_data(stmt, 0, data, size))
    {
        log_printf(" send_long_data failed");
	goto err_out;
    }
    */
    if (mysql_stmt_execute(stmt)) {
	log_printf(LOG_ERROR, "mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    if (mysql_stmt_close(stmt))
	log_printf(LOG_ERROR, "failed closing the statement: %s\n", mysql_stmt_error(stmt));

    return size;

err_out:
	log_printf(LOG_ERROR, " %s\n", mysql_stmt_error(stmt));
	if (mysql_stmt_close(stmt))
	    log_printf(LOG_ERROR, "failed closing the statement: %s\n", mysql_stmt_error(stmt));
	return -EIO;
}

size_t query_size(MYSQL *mysql, long inode)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;

    snprintf(sql, SQL_MAX, "SELECT size FROM inodes WHERE inode=%ld",
             inode);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -1;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -1;
    }

    if(mysql_num_rows(result) != 1 && mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -1;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -1;
    }

    if(row[0]){
        ret = atoll(row[0]);
    }else{
        ret = 0;
    }
    mysql_free_result(result);

    return ret;
}

int query_rename(MYSQL *mysql, const char *from, const char *to)
{
    int ret;
    long inode, parent_to, parent_from;
    char *tmp, *new_name, *old_name;
    char esc_new_name[PATH_MAX * 2], esc_old_name[PATH_MAX * 2];
    char sql[SQL_MAX];

    inode = query_inode(mysql, from);

    /* Lots of strdup()s follow because dirname() & basename()
     * may modify the original string. */
    tmp = strdup(from);
    parent_from = query_inode(mysql, dirname(tmp));
    free(tmp);

    tmp = strdup(from);
    old_name = basename(tmp);
    mysql_real_escape_string(mysql, esc_old_name, old_name, strlen(old_name));
    free(tmp);

    tmp = strdup(to);
    parent_to = query_inode(mysql, dirname(tmp));
    free(tmp);

    tmp = strdup(to);
    new_name = basename(tmp);
    mysql_real_escape_string(mysql, esc_new_name, new_name, strlen(new_name));
    free(tmp);

    snprintf(sql, SQL_MAX,
             "UPDATE tree "
	     "SET name='%s', parent=%ld "
	     "WHERE inode=%ld AND name='%s' AND parent=%ld ",
             esc_new_name, parent_to,
	     inode, esc_old_name, parent_from);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    /*
    if (mysql_affected_rows(mysql) < 1)
      return -ETHIS_IS_STRANGE;	/ * Someone deleted the direntry? Do we care? * /
    */

    return 0;
}

int query_inuse_inc(MYSQL *mysql, long inode, int increment)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE inodes SET inuse = inuse + %d "
             "WHERE inode=%lu",
             increment, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

int query_purge_deleted(MYSQL *mysql, long inode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
	     "DELETE FROM inodes WHERE inode=%ld AND inuse=0 AND deleted=1",
             inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

int query_set_deleted(MYSQL *mysql, long inode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
	     "UPDATE inodes LEFT JOIN tree ON inodes.inode = tree.inode SET inodes.deleted=1 "
	     "WHERE inodes.inode = %ld AND tree.name IS NULL",
             inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

