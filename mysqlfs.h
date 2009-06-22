/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id: mysqlfs.h,v 1.3 2007/03/28 13:05:48 ludvigm Exp $

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file */

/** maximum length of a full pathname */
#define PATH_MAX 1024

/** size of a single datablock written to the database; should be less than the size of a "blob" or mysqlfs.sql needs to be altered */
#define DATA_BLOCK_SIZE	4096

#define MIN(a,b)	((a) < (b) ? (a) : (b))
#define MAX(a,b)	((a) > (b) ? (a) : (b))
