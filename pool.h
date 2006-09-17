/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: pool.h,v 1.4 2006/09/17 11:09:32 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

struct mysqlfs_opt {
    char *host;                 /* MySQL host */
    char *user;                 /* MySQL user */
    char *passwd;               /* MySQL password */
    char *db;                   /* MySQL database name */
    unsigned int port;		/* MySQL port */
    char *socket;		/* MySQL socket */
    char *mycnf_group;		/* Group in my.cnf to read defaults from */
    unsigned int init_conns;	/* Number of DB connections to init on startup */
    unsigned int max_idling_conns;	/* Maximum number of idling DB connections */
    char *logfile;
};

/* Initalize pool and preallocate connections */
int pool_init(struct mysqlfs_opt *opt);

/* Close all connections and cleanup pool */
void pool_cleanup();

/* Get DB connection from pool */
void *pool_get();

/* Put DB connection back to the pool */
void pool_put(void *conn);
