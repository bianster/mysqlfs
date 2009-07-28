/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  $Id$

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file */

/** maximum length of a full pathname */
#define PATH_MAX 1024

/** size of a single datablock written to the database; should be less than the size of a "blob" or mysqlfs.sql needs to be altered (pkg/statusfile.xsd limits to unsigned int; schema.sql uses BLOB which limits this at 2^16-1 */
#define DATA_BLOCK_SIZE	4096

/** basic preprocessor-phase maximum macro */
#define MIN(a,b)	((a) < (b) ? (a) : (b))
/** basic preprocessor-phase minimum macro */
#define MAX(a,b)	((a) > (b) ? (a) : (b))
