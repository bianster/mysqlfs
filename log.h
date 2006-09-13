/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Michal Ludvig <michal@logix.cz>
  $Id: log.h,v 1.2 2006/09/13 10:54:37 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

extern FILE *log_file;
extern int log_types_mask;

enum log_types {
  LOG_ERROR	= 0x0001,
  LOG_WARNING	= 0x0002,
  LOG_INFO	= 0x0004,
  LOG_DEBUG	= 0x0008,

  LOG_D_OTHER	= 0x0100 | LOG_DEBUG,
  LOG_D_SQL	= 0x0200 | LOG_DEBUG,
  LOG_D_CALL	= 0x0400 | LOG_DEBUG,
  LOG_D_POOL	= 0x0800 | LOG_DEBUG,
  
  LOG_MASK_MAJOR	= 0x000F,
  LOG_MASK_MINOR	= 0xFF00,
};

int log_printf(enum log_types type, const char *logmsg, ...);
FILE *log_init(const char *filename, int verbose);
void log_finish(FILE *f);
