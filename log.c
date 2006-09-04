/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Michal Ludvig <michal@logix.cz>
  $Id: log.c,v 1.1 2006/09/04 11:43:29 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#include <config.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "log.h"

FILE *log_file;
int log_types_mask = LOG_ERROR | LOG_INFO | LOG_DEBUG;
//int log_types_mask = LOG_ERROR | LOG_INFO;
int log_debug_mask = LOG_D_CALL | LOG_D_OTHER;

#define BUFSIZE 512

static char *currentTS(void)
{
	static char buf[BUFSIZE];
	time_t curtime;

	bzero(buf, BUFSIZE);
	curtime = time(NULL);
	strftime(buf, BUFSIZE, "%Y-%m-%d %H:%M:%S", localtime(&curtime));

	return buf;
}

int log_printf(enum log_types type, const char *logmsg, ...)
{
	va_list args;
	char buf[BUFSIZE];

	if ((log_types_mask & type & LOG_MASK_MAJOR) == 0)
	  return;

	if ((type & LOG_DEBUG) && (log_debug_mask & type & LOG_MASK_MINOR) == 0)
	  return;

/*
 * Subtypes for INFO and ERROR are not yet defined
	if ((type & LOG_INFO) && (log_info_mask & type & LOG_MASK_MINOR) == 0)
	  return;

	if ((type & LOG_ERROR) && (log_error_mask & type & LOG_MASK_MINOR) == 0)
	  return;
*/

	bzero(buf, BUFSIZE);
	snprintf(buf, BUFSIZE, "%s %d %s", currentTS(), getpid(), logmsg);
	va_start(args, logmsg);

	return vfprintf(log_file, buf, args);
}

FILE *log_init(const char *filename, int verbose)
{
	FILE    *f;

	if (verbose)
		printf("* Opening logfile '%s': ", filename);

	if ((f = fopen(filename, "a+")) == NULL)
	{
		if (verbose)
			printf("failed: %s\n", strerror(errno));
		exit(1);
	}

	setbuf(f, NULL);
	if (verbose)
		printf(" OK\n");

	return f;
}

void log_finish(FILE *f)
{
	fclose(f);
}

