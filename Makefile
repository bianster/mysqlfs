# $Id:
CC=gcc
LD=ld
MYSQL_CONFIG=/usr/bin/mysql_config
#DEFS=-DFUSE_USE_VERSION=22 -DDEBUG -g
DEFS=-DFUSE_USE_VERSION=22
CFLAGS=-Wall $(DEFS) `pkg-config --cflags fuse` `$(MYSQL_CONFIG) --cflags`
LDFLAGS=`pkg-config --libs fuse` `$(MYSQL_CONFIG) --libs`

all: mysqlfs

clean:
	rm -rf mysqlfs *.o *~ mtrace.log

mysqlfs: mysqlfs.c query.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
