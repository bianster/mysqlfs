CC=gcc
LD=ld
MYSQL_CONFIG=/usr/bin/mysql_config
CFLAGS=-Wall -DFUSE_USE_VERSION=22 `pkg-config --cflags fuse` `$(MYSQL_CONFIG) --cflags`
LDFLAGS=`pkg-config --libs fuse` `$(MYSQL_CONFIG) --libs`

all: mysqlfs

clean:
	rm -rf mysqlfs *.o *~

mysqlfs: mysqlfs.c query.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
