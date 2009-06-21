# $Id: Makefile.am,v 1.4 2006/10/02 22:34:03 ludvigm Exp $

bin_PROGRAMS = mysqlfs

# because the source is not in a subdir, we cannot just put the tests in a SUBDIRS= :(
check-recursive : mysqlfs

EXTRA_DIST = schema.sql

SUBDIRS = tests-autotest

mysqlfs_SOURCES = mysqlfs.c query.c pool.c log.c

noinst_HEADERS = mysqlfs.h query.h pool.h log.h
