# $Id$

EXTRA_DIST = mysqlfs-plugin.h

pkglib_LTLIBRARIES = dummy-xor.la
dummy_xor_la_SOURCES = xor.c
dummy_xor_la_LDFLAGS = -module

check_PROGRAMS = check-xor check-xor-filt
check_xor_SOURCES = check-xor.c
check_xor_filt_SOURCES = check-xor-filt.c

TESTS = $(check_PROGRAMS)
TESTS_ENVIRONMENT=LD_LIBRARY_PATH=$(top_builddir)/$(subdir)/.libs
