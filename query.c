/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  Copyright (C) 2006,2007 Michal Ludvig <michal@logix.cz>
  $Id: query.c,v 1.16 2007/03/28 13:05:48 ludvigm Exp $

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

static inline int lock_inode(MYSQL *mysql, long inode)
{
    // TODO
    return 0;
}

static inline int unlock_inode(MYSQL *mysql, long inode)
{
    // TODO
    return 0;
}

static struct data_blocks_info *
fill_data_blocks_info(struct data_blocks_info *info, size_t size, off_t offset)
{
    info->seq_first = offset / DATA_BLOCK_SIZE;
    info->offset_first = offset % DATA_BLOCK_SIZE;

    unsigned long  nr_following_blocks = ((info->offset_first + size) / DATA_BLOCK_SIZE);	
    info->length_first = nr_following_blocks > 0 ? DATA_BLOCK_SIZE - info->offset_first : size;

    info->seq_last = info->seq_first + nr_following_blocks;
    info->length_last = (info->offset_first + size) % DATA_BLOCK_SIZE;
    /* offset in last block (if it's a different one from the first block) 
     * is always 0 */

    return info;
}

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
             "SELECT inode, mode, uid, gid, atime, mtime "
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
    struct data_blocks_info info;

    fill_data_blocks_info(&info, length, 0);

    long inode = query_inode(mysql, path);
    if (inode < 0)
      return inode;

    lock_inode(mysql, inode);

    snprintf(sql, SQL_MAX,
             "DELETE FROM data_blocks WHERE inode=%ld AND seq > %ld",
	     inode, info.seq_last);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    snprintf(sql, SQL_MAX,
             "UPDATE data_blocks SET data=RPAD(data, %zu, '\\0') "
	     "WHERE inode=%ld AND seq=%ld",
             info.length_last, inode, info.seq_last);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    snprintf(sql, SQL_MAX,
             "UPDATE inodes SET size=%lld WHERE inode=%ld",
             length, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    unlock_inode(mysql, inode);

    return 0;

err_out:
    unlock_inode(mysql, inode);
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

    if (path[0] == '/' && path[1] == '\0') {
        snprintf(sql, SQL_MAX,
                 "INSERT INTO tree (name, parent) VALUES ('/', NULL)");

        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret = mysql_query(mysql, sql);
        if(ret)
          goto err_out;
    } else {
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
    }

    new_inode_number = mysql_insert_id(mysql);

    snprintf(sql, SQL_MAX,
             "INSERT INTO inodes(inode, mode, uid, gid, atime, ctime, mtime)"
             "VALUES(%ld, %d, %d, %d, UNIX_TIMESTAMP(NOW()), "
	            "UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()))",
             new_inode_number, mode,
	     fuse_get_context()->uid, fuse_get_context()->gid);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

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
    size_t index;

    index = snprintf(sql, SQL_MAX, "UPDATE inodes SET ");
    if (uid != (uid_t)-1)
    	index += snprintf(sql + index, SQL_MAX - index, 
			  "uid=%d ", uid);
    if (gid != (gid_t)-1)
    	index += snprintf(sql + index, SQL_MAX - index,
			  "%s gid=%d ", 
			  /* Insert comma if this is a second argument */
			  (uid != (uid_t)-1) ? "," : "",
			  gid);
    snprintf(sql + index, SQL_MAX - index, "WHERE inode=%ld", inode);

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
             "SET atime=%ld, mtime=%ld "
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
    unsigned long length = 0L, copy_len, seq;
    struct data_blocks_info info;
    char *dst = (char *)buf;
    char *src, *zeroes = alloca(DATA_BLOCK_SIZE);

    fill_data_blocks_info(&info, size, offset);

    /* Read all required blocks */
    snprintf(sql, SQL_MAX,
             "SELECT seq, data, LENGTH(data) FROM data_blocks WHERE inode=%ld AND seq>=%lu AND seq <=%lu ORDER BY seq ASC",
             inode, info.seq_first, info.seq_last);

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

    /* This is a bit tricky as we support 'sparse' files now.
     * It means not all requested blocks must exist in the
     * database. For those that don't exist we'll return
     * a block of \0 instead.  */
    row = mysql_fetch_row(result);
    memset(zeroes, 0L, DATA_BLOCK_SIZE);
    for (seq = info.seq_first; seq<=info.seq_last; seq++) {
        off_t row_seq = -1;
	size_t row_len = DATA_BLOCK_SIZE;
	char *data = zeroes;

	if (row && (row_seq = atoll(row[0])) == seq) {
	    data = row[1];
	    row_len = atoll(row[2]);
	}
	    
	if (seq == info.seq_first) {
	    if (row_len < info.offset_first)
	        goto go_away;

	    copy_len = MIN(row_len - info.offset_first, info.length_first);
	    src = data + info.offset_first;
	} else if (seq == info.seq_last) {
	    copy_len = MIN(info.length_last, row_len);
	    src = data;
	} else {
	    copy_len = MIN(DATA_BLOCK_SIZE, row_len);
	    src = data;
	}

	memcpy(dst, src, copy_len);
	dst += copy_len;
	length += copy_len;

	if (row && row_seq == seq)
	    row = mysql_fetch_row(result);
    }

go_away:
    /* Read all remaining rows */
    while (mysql_fetch_row(result));
    mysql_free_result(result);

    return length;
}

static int write_one_block(MYSQL *mysql, long inode,
				 unsigned long seq,
				 const char *data, size_t size,
				 off_t offset)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND bind[1];
    char sql[SQL_MAX];
    size_t current_block_size = query_size_block(mysql, inode, seq);

    /* Shortcut */
    if (size == 0) return 0;

    if (offset + size > DATA_BLOCK_SIZE) {
        log_printf(LOG_ERROR, "%s(): offset(%zu)+size(%zu)>max_block(%d)\n", 
		   __func__, offset, size, DATA_BLOCK_SIZE);
	return -EIO;
    }

    /* We expect the inode is already locked for this thread by caller! */

    if (current_block_size == -ENXIO) {
        /* This data block has not yet been allocated */
        snprintf(sql, SQL_MAX,
                 "INSERT INTO data_blocks SET inode=%ld, seq=%lu, data=''", inode, seq);
        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        if(mysql_query(mysql, sql)){
            log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
            return -EIO;
        }

        current_block_size = query_size_block(mysql, inode, seq);
    }

    stmt = mysql_stmt_init(mysql);
    if (!stmt)
    {
        log_printf(LOG_ERROR, "mysql_stmt_init(), out of memory\n");
	return -EIO;
    }

    memset(bind, 0, sizeof(bind));
    if (offset == 0 && current_block_size == 0) {
        snprintf(sql, SQL_MAX,
                 "UPDATE data_blocks "
		 "SET data=? "
		 "WHERE inode=%ld AND seq=%lu",
		 inode, seq);
    } else if (offset == current_block_size) {
        snprintf(sql, sizeof(sql),
                 "UPDATE data_blocks "
		 "SET data=CONCAT(data, ?) "
		 "WHERE inode=%ld AND seq=%lu",
		 inode, seq);
    } else {
        size_t pos, new_size;
        pos = snprintf(sql, sizeof(sql),
		 "UPDATE data_blocks SET data=CONCAT(");
	if (offset > 0)
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "RPAD(IF(ISNULL(data),'', data), %llu, '\\0'),", offset);
	pos += snprintf(sql + pos, sizeof(sql) - pos, "?,");
	new_size = offset + size;
	if (offset + size < current_block_size) {
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "SUBSTRING(data FROM %llu),", offset + size + 1);
	    new_size = current_block_size;
	}
	sql[--pos] = '\0';	/* Remove the trailing comma. */
	pos += snprintf(sql + pos, sizeof(sql) - pos, ") WHERE inode=%ld AND seq=%lu",
			inode, seq);
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
    bind[0].length= (unsigned long *)(void *)&size;

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

    /* Update file size */
    snprintf(sql, SQL_MAX,
	     "UPDATE inodes SET size=("
	     	"SELECT seq*%d + LENGTH(data) FROM data_blocks WHERE inode=%ld AND seq=("
			"SELECT MAX(seq) FROM data_blocks WHERE inode=%ld"
		")"
	     ") "
	     "WHERE inode=%ld",
	     DATA_BLOCK_SIZE, inode, inode, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if(mysql_query(mysql, sql)) {
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return size;

err_out:
	log_printf(LOG_ERROR, " %s\n", mysql_stmt_error(stmt));
	if (mysql_stmt_close(stmt))
	    log_printf(LOG_ERROR, "failed closing the statement: %s\n", mysql_stmt_error(stmt));
	return -EIO;
}

int query_write(MYSQL *mysql, long inode, const char *data, size_t size,
                off_t offset)
{
    struct data_blocks_info info;
    unsigned long seq;
    const char *ptr;
    int ret, ret_size = 0;

    fill_data_blocks_info(&info, size, offset);

    /* Handle first block */
    lock_inode(mysql, inode);
    ret = write_one_block(mysql, inode, info.seq_first, data,
			  info.length_first, info.offset_first);
    unlock_inode(mysql, inode);
    if (ret < 0)
        return ret;
    ret_size = ret;

    /* Shortcut - if last block seq is the same as first block
     * seq simply go away as it's the same block */
    if (info.seq_first == info.seq_last)
        return ret_size;

    ptr = data + info.length_first;

    /* Handle all full-sized intermediate blocks */
    for (seq = info.seq_first + 1; seq < info.seq_last; seq++) {
        lock_inode(mysql, inode);
        ret = write_one_block(mysql, inode, seq, ptr, DATA_BLOCK_SIZE, 0);
        unlock_inode(mysql, inode);
        if (ret < 0)
            return ret;
	ptr += DATA_BLOCK_SIZE;
	ret_size += ret;
    }

    /* Handle last block */
    lock_inode(mysql, inode);
    ret = write_one_block(mysql, inode, info.seq_last, ptr,
			  info.length_last, 0);
    unlock_inode(mysql, inode);
    if (ret < 0)
        return ret;
    ret_size += ret;

    return ret_size;
}

ssize_t query_size(MYSQL *mysql, long inode)
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
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 || mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    if(row[0]){
        ret = atoll(row[0]);
    }else{
        ret = 0;
    }
    mysql_free_result(result);

    return ret;
}

ssize_t query_size_block(MYSQL *mysql, long inode, unsigned long seq)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;

    snprintf(sql, SQL_MAX, "SELECT LENGTH(data) FROM data_blocks WHERE inode=%ld AND seq=%lu",
             inode, seq);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) == 0) {
        mysql_free_result(result);
        return -ENXIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
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

int query_fsck(MYSQL *mysql)
{
    // See TODO file for what should be here...
    return 0;
}
