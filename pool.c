/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: pool.c,v 1.1 2006/07/15 19:28:09 cuspy Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <pthread.h>

#include "pool.h"

static pthread_mutex_t mysql_pool_mutex=PTHREAD_MUTEX_INITIALIZER;

MYSQL_POOL *mysqlfs_pool_init(MYSQLFS_OPT *opt)
{
    int i;
    MYSQL_POOL *pool;
    MYSQL *ret;

    pool = malloc(sizeof(MYSQL_POOL));
    if(!pool){
        return NULL;
    }

    pool->num = opt->connection;
    if(pool->num < 0){
        return NULL;
    }

    pool->use = malloc(sizeof(int) * pool->num);
    if(!pool->use){
        free(pool);
        return NULL;
    }

    pool->conn = malloc(sizeof(MYSQL_CONN) * pool->num);
    if(!pool->conn){
        free(pool->use);
        free(pool);
        return NULL;
    }

    for(i=0; i<pool->num; i++){
        printf("connect: %d\n", i);
        pool->conn[i].id = i;
        pool->use[i] = 0;

        pool->conn[i].mysql = mysql_init(NULL);
        if(!pool->conn[i].mysql){
            break;
        }
        
        ret = mysql_real_connect(
            pool->conn[i].mysql, opt->host, opt->user,
            opt->passwd, opt->db, 0, NULL, 0);
        if(!ret){
            fprintf(stderr, "%s\n", mysql_error(pool->conn[i].mysql));
            return NULL;
        }

    }

    return pool;
}

int mysqlfs_pool_free(MYSQL_POOL *pool)
{
    int i;
    
    for(i=0; i<pool->num; i++){
        printf("disconnect: %d\n", i);
        mysql_close(pool->conn[i].mysql);
    }

    if(pool->conn){
        free(pool->conn);
    }

    if(pool->use){
        free(pool->use);
    }

    if(pool){
        free(pool);
    }

    return 0;
}

MYSQL_CONN *mysqlfs_pool_get(MYSQL_POOL *pool)
{
    int i;
    MYSQL_CONN *conn = NULL;

    pthread_mutex_lock(&mysql_pool_mutex);

    for(i=0; i<pool->num; i++){
        if(!pool->use[i]){
//            printf("get connecttion %d\n", i);
            pool->use[i] = 1;
            conn = &(pool->conn[i]);
            break;
        }
    }

    pthread_mutex_unlock(&mysql_pool_mutex);

    return conn;
}

int mysqlfs_pool_return(MYSQL_POOL *pool, MYSQL_CONN *conn)
{

    pthread_mutex_lock(&mysql_pool_mutex);
    pool->use[conn->id] = 0;
/*
    printf("ping=%d\n", mysql_ping(conn->mysql));
    printf("return %d\n", conn->id);
*/
    pthread_mutex_unlock(&mysql_pool_mutex);

    return 0;
}

void mysqlfs_pool_print(MYSQL_POOL *pool)
{
    int i;

    pthread_mutex_lock(&mysql_pool_mutex);
    
    for(i=0; i<pool->num; i++){
        printf("use[%d] = %d\n", i, pool->use[i]);
    }

    pthread_mutex_unlock(&mysql_pool_mutex);

    return;
}
