/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: pool.h,v 1.1 2006/07/15 19:28:09 cuspy Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

typedef struct{
    int id;
    MYSQL *mysql;
}MYSQL_CONN;

typedef struct{
    int num;
    MYSQL *mysql;
    int *use;
    MYSQL_CONN *conn;
}MYSQL_POOL;

typedef struct{
    char *host;                 /* mysql host */
    char *user;                 /* mysql user */
    char *passwd;               /* mysql password */
    char *db;                   /* mysql database name */
    int connection;             /* connection pooling num */
}MYSQLFS_OPT;

/* initalize and  connet all connection */
MYSQL_POOL *mysqlfs_pool_init(MYSQLFS_OPT *opt);

/* disconnect all connection and free memory */
int mysqlfs_pool_free(MYSQL_POOL *pool);

/* get pooling connection */
MYSQL_CONN *mysqlfs_pool_get(MYSQL_POOL *pool);

/* return pooling connection */
int mysqlfs_pool_return(MYSQL_POOL *pool, MYSQL_CONN *conn);

/* debug function */
void mysqlfs_pool_print(MYSQL_POOL *pool);
