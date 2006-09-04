/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: query.h,v 1.6 2006/09/04 13:11:21 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

int query_inode(MYSQL *mysql, const char* path);
int query_getattr(MYSQL *mysql, const char *path, struct stat *stbuf);
int query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                int parent, int alloc_data);
int query_mkdir(MYSQL *mysql, const char* path, mode_t mode, int inode);
int query_readdir(MYSQL *mysql, int inode, void *buf, fuse_fill_dir_t filler);
int query_read(MYSQL *mysql, const char *path, const char* buf, size_t size,
               off_t offset);
int query_write(MYSQL *mysql, const char *path, const char* buf, size_t size,
                off_t offset);
int query_truncate(MYSQL *mysql, const char *path, off_t length);

int query_delete(MYSQL *mysql, const char* path);

int query_symlink(MYSQL *mysql, const char* from, const char* to);
int query_readlink(MYSQL *mysql, const char* path);

int query_rename(MYSQL *mysql, const char* from, const char* to);

int query_chmod(MYSQL *mysql, const char* path, mode_t mode);
int query_utime(MYSQL *mysql, const char* path, struct utimbuf *time);
size_t query_size(MYSQL *mysql, const char* path);
