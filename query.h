/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: query.h,v 1.3 2006/06/05 18:43:38 cuspy Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

int query_inode(MYSQL *mysql, const char* path);
int query_getattr(MYSQL *mysql, const char *path, struct stat *stbuf);
int query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                int parent);
int query_mkdir(MYSQL *mysql, const char* path, mode_t mode, int inode);
int query_readdir(MYSQL *mysql, int inode, void *buf, fuse_fill_dir_t filler);
int query_read(MYSQL *mysql, const char *path, const char* buf, size_t size,
               off_t offset);
int query_write(MYSQL *mysql, const char *path, const char* buf, size_t size,
                off_t offset);

int query_delete(MYSQL *mysql, const char* path);
off_t query_size(MYSQL *mysql, const char* path);
