/** @file
 *
 * This file serves as both a sample plugin for datablocks in the system, and as a test plugin for
 * the regression testing.  This strategy ensures that examples actually work.
 */

#include <stdio.h>
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include "mysqlfs-plugin.h"

/**
 * Entry-point for the plugin: it allows the version of the mysqlfs trigger this function like a Factory to generate the proper structure of redirectors for plugin actions.
 *
 * The versioning for this project is such that Major and Minor may change on arbitrary boundaries.  For example, 0.4.0-rcX is actually 0.3.99.X so that versions retain a simple always-forward progression.  In such an example, this function would be called with mysqlfs_init(0, 3, 99).  The function could choose, for example, that the 0.3.x line had some changes in the sub-99 point release, so some functions should be swapped for different versions.  Of course, "99" in the point release typically says "this is the last 0.3.Z releases, so this is intended to promote to 0.4.0".
 *
 * The only promises about major and minor numbers is that they will only ever increase, and the reasoning to increase them might be obvious only to us, so don't worry about it too much.
 *
 * @return handle to a structure of entry points for various functions
 * @param version_major part of the version number, broken out to ease comparison
 * @param version_minor part of the version number, broken out to ease comparison
 * @param version_point part of the version number, broken out to ease comparison
 */

static unsigned buffer_size = 0;
static unsigned char ver[3];

static char *dummy_identity()
{
    static char v[50];
    snprintf (v, sizeof(v), "dummy-xor %d.%d.%d buffer=%u", ver[0], ver[1], ver[2], buffer_size);

    return v;
}

static void dummy_setblocksize(unsigned size)
{
    buffer_size = size;
}

/**
 * Pass-thru hook for in-place munging of data in the read and write directions.  These functions are
 * call in-order (but reverse order to write hooks) so that plugins may choose to edit the buffer.
 *
 * We're reusing the same block editing for both read and write since we're doing a symmetric edit
 * to each byte.  Typically you'd want different actions excepting perhaps this simply obfuscation
 * or a symmetric encryption (where E(E(x))== x)
 *
 * Since there's no query_write_one(), this calling of function lists has to be done in the
 * query_read() function's loop.
 *
 * @return 0 if OK, some errno if an error occurs (which will -EXIO the write)
 * @param buf the buffer to edit
 * @param size the size of data in the buffer
 * @param offset the start of the buffer within the target block, in case a byte alignment is required
 *    to key an offset in the munging.  For example, if we're writing 61 bytes starting at byte #4 in
 *    a block, offset will be 4, size will be 61, and it'll fill in the 4th to the 65th bytes of that
 *    block.
 */
static int dummy_readwrite(unsigned char* buf, size_t size, off_t offset)
{
    size_t i;

    for (i = 0; i < size; i++) buf[i] ^= 0x5A;
    return 0;
}

mysqlfs_plugin xor_entry =
{
    .identity = dummy_identity,
    .setblocksize = dummy_setblocksize,
    .read = dummy_readwrite,
    .write = dummy_readwrite
};


/**
 * feed back an entrypoint array.  This is done as a Factory pattern, but all results are the same
 * so we ignore the version data.
 *
 * @return entrypoint array: a pointer to a mysqlfs_plugin
 * @param version_major version of the mysqlfs code
 * @param version_minor version of the mysqlfs code
 * @param version_point version of the mysqlfs code
 */
extern mysqlfs_plugin* mysqlfs_init(unsigned char version_major, unsigned char version_minor, unsigned char version_point)
{
    /* make a quick copy of the version for reporting back */
    ver[0] = version_major;
    ver[1] = version_minor;
    ver[2] = version_point;
    return &xor_entry;
}
