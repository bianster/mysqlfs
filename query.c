/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: query.c,v 1.11 2006/09/06 06:01:34 ludvigm Exp $

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
    char sql[SQL_MAX];
    char esc_path[PATH_MAX * 2];
    MYSQL_RES* result;
    MYSQL_ROW row;

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));

    snprintf(sql, SQL_MAX,
             "SELECT id, mode, UNIX_TIMESTAMP(atime), UNIX_TIMESTAMP(mtime) "
             "FROM fs WHERE path='%s'",
             esc_path);

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

    if(mysql_num_rows(result) != 1 && mysql_num_fields(result) != 2){
        mysql_free_result(result);
        return -ENOENT;
    }
    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    stbuf->st_mode = atoi(row[1]);
    stbuf->st_uid = fuse_get_context()->uid;
    stbuf->st_gid = fuse_get_context()->gid;
    stbuf->st_nlink = 1;
    stbuf->st_atime = atol(row[2]);
    stbuf->st_mtime = atol(row[3]);

    mysql_free_result(result);

    return 0;
}

int query_inode(MYSQL *mysql, const char *path)
{
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));
    snprintf(sql, SQL_MAX, "SELECT id FROM fs WHERE path = '%s'",
             esc_path);

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
        return -ENOENT;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }
    ret = atoi(row[0]);
    mysql_free_result(result);

    return ret;
}

int query_truncate(MYSQL *mysql, const char *path, off_t length)
{
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));
    snprintf(sql, SQL_MAX,
             "UPDATE fs LEFT JOIN data ON fs.id = data.id SET data=RPAD(data, %lld, '\\0') WHERE path='%s'",
             length, esc_path);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    return 0;

err_out:
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

int query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                int parent, int alloc_data)
{
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];
    my_ulonglong new_inode_number = 0;

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));
    snprintf(sql, SQL_MAX,
             "INSERT INTO fs(path, mode, parent, atime, ctime, mtime)"
             "VALUES('%s', %d, %d, NOW(), NOW(), NOW())",
             esc_path, mode, parent);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    new_inode_number = mysql_insert_id(mysql);

    log_printf(LOG_DEBUG, "new_inode_number=%llu\n", new_inode_number);

    if (alloc_data) {
        snprintf(sql, SQL_MAX,
                 "INSERT INTO data SET id=%llu", new_inode_number);

        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret = mysql_query(mysql, sql);
        if (ret)
          goto err_out;
    }
    return 0;

err_out:
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

int query_mkdir(MYSQL *mysql, const char *path, mode_t mode, int parent)
{
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));
    snprintf(sql, sizeof(sql),
             "INSERT INTO fs(path, mode, parent, atime, mtime)"
             "VALUES('%s', %d, %d, NOW(), NOW())",
             esc_path, S_IFDIR | mode, parent);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return ret;
}

int query_readdir(MYSQL *mysql, int inode, void *buf, fuse_fill_dir_t filler)
{
    int ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    snprintf(sql, sizeof(sql), "SELECT path FROM fs WHERE parent = '%d'",
             inode);

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

    while((row = mysql_fetch_row(result)) != NULL){
        filler(buf, (char*)basename(row[0]), NULL, 0);
    }

    mysql_free_result(result);

    return ret;
}

int query_delete(MYSQL *mysql, const char *path)
{
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));
    snprintf(sql, SQL_MAX,
             "DELETE FROM fs WHERE path='%s'", esc_path);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EBUSY;
    }

    return 0;
}

int query_chmod(MYSQL *mysql, const char *path, mode_t mode){
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));

    snprintf(sql, SQL_MAX,
             "UPDATE fs SET mode=%d WHERE path='%s'",
             mode, esc_path);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EBUSY;
    }

    return 0;
}

int query_utime(MYSQL *mysql, const char *path, struct utimbuf *time){
    int ret;
    char esc_path[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));

    snprintf(sql, SQL_MAX,
             "UPDATE fs SET "
             "atime=FROM_UNIXTIME(%ld), mtime=FROM_UNIXTIME(%ld) "
             "WHERE path='%s'",
             time->actime, time->modtime, esc_path);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EBUSY;
    }

    return 0;
}

int query_read(MYSQL *mysql, const char *path, const char *buf, size_t size,
               off_t offset)
{
    int ret;
    char sql[SQL_MAX];
    char esc_path[PATH_MAX * 2];
    MYSQL_RES* result;
    MYSQL_ROW row;
    unsigned long length;

    mysql_real_escape_string(mysql, esc_path, path, strlen(path));

    snprintf(sql, SQL_MAX,
             "SELECT SUBSTRING(data, %lld, %d) FROM fs LEFT JOIN data ON fs.id = data.id WHERE path='%s'",
             offset + 1, size, esc_path);

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

int query_write(MYSQL *mysql, const char *path, const char *data, size_t size,
                off_t offset)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND bind[5];
    int  path_len = strlen(path);
    char sql[SQL_MAX];
    size_t current_size = query_size(mysql, path);

    // log_printf("write(%s, %lu @ %ld) [current=%lu]\n", path, size, offset, current_size);
    stmt = mysql_stmt_init(mysql);
    if (!stmt)
    {
        log_printf(LOG_ERROR, "mysql_stmt_init(), out of memory\n");
	return -EIO;
    }

    memset(bind, 0, sizeof(bind));
    if (offset == 0 && current_size == 0) {
        snprintf(sql, SQL_MAX,
                 "UPDATE fs LEFT JOIN data ON fs.id = data.id SET data=?, size=%zu WHERE path=?",
		 size);
    } else if (offset == current_size) {
        snprintf(sql, sizeof(sql),
                 "UPDATE fs LEFT JOIN data ON fs.id = data.id SET data=CONCAT(data, ?), size=size+%zu "
                 "WHERE path=?", size);
    } else {
        size_t pos, new_size;
        pos = snprintf(sql, sizeof(sql),
		 "UPDATE fs LEFT JOIN data ON fs.id = data.id SET data=CONCAT(");
	if (offset > 0)
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "RPAD(IF(ISNULL(data),'', data), %llu, '\\0'),", offset);
	pos += snprintf(sql + pos, sizeof(sql) - pos, "?,");
	new_size = offset + size;
	if (offset + size < current_size) {
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "SUBSTRING(data FROM %llu),", offset + size + 1);
	    new_size = current_size;
	}
	sql[--pos] = '\0';	/* Remove the trailing comma. */
	pos += snprintf(sql + pos, sizeof(sql) - pos, "), size=%zu WHERE path=?",
			new_size);
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    if (mysql_stmt_prepare(stmt, sql, strlen(sql))) {
	log_printf(LOG_ERROR, "mysql_stmt_prepare() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    if (mysql_stmt_param_count(stmt) != 2) {
      log_printf(LOG_ERROR, "%s(): stmt_param_count=%d, expected 2\n", __func__, mysql_stmt_param_count(stmt));
      return -EIO;
    }
    bind[0].buffer_type= MYSQL_TYPE_LONG_BLOB;
    bind[0].buffer= (char *)data;
    bind[0].is_null= 0;
    bind[0].length= (unsigned long *)&size;

    bind[1].buffer_type= MYSQL_TYPE_STRING;
    bind[1].buffer= (char *)path;
    bind[1].is_null= 0;
    bind[1].length= (unsigned long *)&path_len;

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

size_t query_size(MYSQL *mysql, const char *path)
{
    size_t ret;
    char path_esc[PATH_MAX * 2];
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;

    mysql_real_escape_string(mysql, path_esc, path, strlen(path));
    snprintf(sql, SQL_MAX, "SELECT size FROM fs WHERE path = '%s'",
             path_esc);

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

int query_rename(MYSQL *mysql, const char *from, const char *to){
    int ret;
    char esc_from[PATH_MAX * 2], esc_to[PATH_MAX * 2];
    char sql[SQL_MAX];

    mysql_real_escape_string(mysql, esc_from, from, strlen(from));
    mysql_real_escape_string(mysql, esc_to, to, strlen(to));

    snprintf(sql, SQL_MAX,
             "UPDATE fs SET "
             "path='%s' WHERE path='%s' AND (mode & %d) <> 0",
             esc_to, esc_from, S_IFREG);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    /* This is temporary thing anyway so we can return whatever error we want. */
    if (mysql_affected_rows(mysql) < 1)
      return -EPERM;

    return 0;
}

