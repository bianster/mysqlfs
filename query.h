/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: query.h,v 1.9 2007/03/28 13:05:48 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

struct data_blocks_info {
    unsigned long	seq_first,	/* Sequence ID of 1st and last block.  */
			seq_last;
    size_t		length_first,	/* Length of data for reading / writing.  */
			length_last;
    off_t		offset_first;	/* Offset in 1st block.  */
};

long query_inode(MYSQL *mysql, const char* path);
int query_inode_full(MYSQL *mysql, const char* path, char *name, size_t name_len,
		     long *inode, long *parent, long *nlinks);
int query_getattr(MYSQL *mysql, const char *path, struct stat *stbuf);
int query_mkdirentry(MYSQL *mysql, long inode, const char *name, long parent);
int query_rmdirentry(MYSQL *mysql, const char *name, long parent);
long query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                long parent, int alloc_data);
long query_mkdir(MYSQL *mysql, const char* path, mode_t mode, long parent);
int query_readdir(MYSQL *mysql, long inode, void *buf, fuse_fill_dir_t filler);
int query_read(MYSQL *mysql, long inode, const char* buf, size_t size, off_t offset);
int query_write(MYSQL *mysql, long inode, const char* buf, size_t size, off_t offset);
int query_truncate(MYSQL *mysql, const char *path, off_t length);

int query_symlink(MYSQL *mysql, const char* from, const char* to);
int query_readlink(MYSQL *mysql, const char* path);

int query_rename(MYSQL *mysql, const char* from, const char* to);

int query_chmod(MYSQL *mysql, long inode, mode_t mode);
int query_chown(MYSQL *mysql, long inode, uid_t uid, gid_t gid);
int query_utime(MYSQL *mysql, long inode, struct utimbuf *time);

ssize_t query_size(MYSQL *mysql, long inode);
ssize_t query_size_block(MYSQL *mysql, long inode, unsigned long seq);

int query_inuse_inc(MYSQL *mysql, long inode, int increment);
int query_set_deleted(MYSQL *mysql, long inode);
int query_purge_deleted(MYSQL *mysql, long inode);

int query_fsck(MYSQL *mysql);
