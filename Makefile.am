# $Id: Makefile.am,v 1.4 2006/10/02 22:34:03 ludvigm Exp $

bin_PROGRAMS = mysqlfs

# because the source is not in a subdir, we cannot just put the tests in a SUBDIRS= :(
check-recursive : mysqlfs

EXTRA_DIST = schema.sql mysqlfs.spec

SUBDIRS = tests-autotest

mysqlfs_SOURCES = mysqlfs.c query.c pool.c log.c

noinst_HEADERS = mysqlfs.h query.h pool.h log.h

if DO_DOXYGEN
doc: Doxyfile pkg/doc-mainpage.c
	$(DOXYGEN)

doc-website: doc
	rsync -avP -e ssh htdocs/html chickenandporn,@PACKAGE_NAME@@frs.sourceforge.net:htdocs
endif

if DO_RPMBUILD
release = $(rpm --query --queryformat="%{RELEASE}\n" --specfile @PACKAGE@.spec |head -1)
rpm: @PACKAGE@-@VERSION@.tar.gz @PACKAGE@.spec
	$(RPMBUILD) -ta $<
endif

