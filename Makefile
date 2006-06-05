CC=gcc
LD=ld
MYSQL_CONFIG=/usr/bin/mysql_config
DEFS=-DDEBUG -g
CFLAGS=-Wall $(DEFS) -DFUSE_USE_VERSION=22 `pkg-config --cflags fuse` `$(MYSQL_CONFIG) --cflags`

LDFLAGS=`pkg-config --libs fuse` `$(MYSQL_CONFIG) --libs`

all: mysqlfs

clean:
	rm -rf mysqlfs *.o *~ mtrace.log

mysqlfs: mysqlfs.c query.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
