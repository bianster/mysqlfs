/**
 * @file
 *
 * This testcase builds on check-xor.c to verify that:
 * @li the dummy-test has a read and write hook,
 * @li the read and write hook actually edit/alter the buffer
 * @li the read hook reverses whatever the read hook did
 *
 * This testcase can be used to similar test any other plugin during development.
 *
 * Many of the build-up tests that this testcase does are previously verified by check-xor.c, so
 * they are compressed into one-line maintenance nightmares to reduce the on-screen realestate.  I'm
 * not a fan typically of compressed onscreen realestate (which is why I favour my coding style, but
 * that's a holywar): compressed code is more difficult to maintain, newer engineers and gurus
 * alike; this is a testcase.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "mysqlfs-plugin.h"
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <mysqlfs.h>		/* to grab the DATA_BLOCK_SIZE */

/**
 * The most trivial 8-bit checksum known to mankind
 */
unsigned char checksum(const unsigned char *x, size_t size)
{
    size_t i;
    unsigned char s;

    for (i = 0, s = 0; i < size; s = (s + x[i++]) % 256);
    return s;
}

/**
 * Test program to check that the dummy loads; based on the example in dlopen() man page.
 */
int main(int argc, char **argv)
{
    void *handle;
    void *(*bufferfilter)(unsigned char, unsigned char, unsigned char);
    mysqlfs_plugin *filterentries;
    char *error;
    int errorcode;
    unsigned char ver[] = {0, 0, 0};
    unsigned char testdatablock [DATA_BLOCK_SIZE];
    unsigned char cksum;

    handle = dlopen ("dummy-xor.so", RTLD_LAZY);
    /* already checked by check-xor.c */
    if (!handle) { fprintf (stderr, "%s\n", dlerror()); exit(1); }

    dlerror();    /* Clear any existing error */

    bufferfilter = dlsym(handle, "mysqlfs_init");
    /* already checked by check-xor.c */
    if (NULL != (error = dlerror())) { fprintf (stderr, "%s\n", error); exit(1); }

    /* already checked by check-xor.c */
    if (NULL == bufferfilter) { fprintf (stderr, "NULL bufferfilter\n"); exit(1); }

    /* grab the version data */
    {
        int i;
        char *a = strdup (VERSION);
        char *v;
        if (NULL != a)
        {
            v = strtok (a, ".");

            for (i = 0; NULL != v && i < (sizeof(ver)/sizeof(ver[0])); i++)
            {
                ver[i] = atoi(v);
                v = strtok (NULL, ".");
            }
            free (a);
        }
    }

    /* already checked by check-xor.c */
    if (NULL == (filterentries = bufferfilter(ver[0], ver[1], ver[2]))) { fprintf (stderr, "NULL jump points\n"); exit(1); }

    /* already checked by check-xor.c */
    if (NULL == filterentries->identity) { fprintf (stderr, "NULL identity function\n"); exit(1); }


    /*
     * Configure the plugin stack (in this case, only one)
     *
     * If this testcase is expanded to help test another more complex pluging, perhaps you'll need some
     * additional plugin configuration than just this one attribute.  This part of the code makes a
     * good place to stick that :)
     */
    if (NULL != filterentries->setblocksize)
        filterentries->setblocksize(DATA_BLOCK_SIZE);


    /* fill a data block with trash */
    {
        int i;		/* yeah, I like how in C++ ypu can type "for (int x = ..." for a local variable */

        /* I'm committing a host of errors here: I am not seeding random, and I'm using the lower-half of the returned values, not the MSB.  I only need kinda-random, so meh. */
        for (i = 0; i < sizeof(testdatablock); i++)
            testdatablock[i] = (char) random();
    }

    /* do we have read/write hooks to test? */
    if ((NULL == filterentries->read) || (NULL == filterentries->write))
    { fprintf (stderr, "Both read and write hooks must be non-NULL\n"); exit(1); }

    /* grab an initial checksum */
    cksum = checksum (testdatablock, sizeof(testdatablock));
    
    /* munge inbound */
    if (0 != (errorcode = filterentries->write (testdatablock, sizeof(testdatablock), 0)))
    { fprintf (stderr, "Write hook returned nonzero %d\n", errorcode); exit(1); }

    /* compare checksum, XFAIL: expect failure/inequal */
    if (cksum == checksum (testdatablock, sizeof(testdatablock)))
    { fprintf (stderr, "Checksum matched, expected mismatch\n"); exit(1); }

    /* munge outbound */
    if (0 != (errorcode = filterentries->read (testdatablock, sizeof(testdatablock), 0)))
    { fprintf (stderr, "Read hook returned nonzero %d\n", errorcode); exit(1); }

    /* compare checksum */
    if (cksum != checksum (testdatablock, sizeof(testdatablock)))
    { fprintf (stderr, "Checksum mismatched\n"); exit(1); }

    printf ("OK %s\n", filterentries->identity());
    dlclose(handle);
    return 0;
}
