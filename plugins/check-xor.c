#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "mysqlfs-plugin.h"
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <mysqlfs.h>

/**
 * Test program to check that the dummy plugin loads and has the symbols I'm looking for; based on
 * copying the example in dlopen() man page then adding more and more iteratively
 */
int main(int argc, char **argv)
{
    void *handle;
    void *(*bufferfilter)(unsigned char, unsigned char, unsigned char);
    mysqlfs_plugin *filterentries;
    char *error;
    unsigned char ver[] = {0, 0, 0};

    handle = dlopen ("dummy-xor.so", RTLD_LAZY);
    if (!handle)
    {
        fprintf (stderr, "%s\n", dlerror());
        exit(1);
    }

    dlerror();    /* Clear any existing error */

    /*
     * grab the Virtual Method Table Factory Class.  See the next comment
     */
    bufferfilter = dlsym(handle, "mysqlfs_init");
    if (NULL != (error = dlerror()))
    {
        fprintf (stderr, "%s\n", error);
        exit(1);
    }

    /*
     * check that we have a generator class to build the structure of jump-points -- basically
     * the C equivalent of a Virtual Method Table from a C++ function to generate a descendent
     * class from a baseclass.  Chances are, this should be done after giving the plugin any
     * config (such as datablocksize, below) so that the plugin has as much detail as possible
     * from which to base and decisions.  Sure, I'm probably thinking far too huge for this
     * little project, but -- as below -- tasty.
     */
    if (NULL == bufferfilter)
    {
        fprintf (stderr, "NULL bufferfilter\n");
        exit(1);
    }

    /*
     * grab the version data from the ./configure information seeded into config.h
     */
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

    /*
     * check that we have a structure of jump-points -- basically the C equivalent of a
     * Virtual Method Table from a C++ Class.
     */
    if (NULL == (filterentries = bufferfilter(ver[0], ver[1], ver[2])))
    {
        fprintf (stderr, "NULL jump points\n");
        exit(1);
    }

    /*
     * check that we have a function to identify the plugin
     */
    if (NULL == filterentries->identity)
    {
        fprintf (stderr, "NULL identity function\n");
        exit(1);
    }

    /*
     * Part of bringing a plugin online is setting the blocksize we're using.  Sure, that's
     * configurable at compile time today, but I want to enforce that it remains variable, hence
     * up to the compile-time guy not the pluging-build-time, and perhaps even variable over
     * time.  Keeping options open.  Options and opportunities are tasty, aren't they?
     */
    if (NULL != filterentries->setblocksize)
        filterentries->setblocksize(DATA_BLOCK_SIZE);

    /*
     * After blindly setting the blocksize, we're done.  Let's grab the identity and spit it out.
     * This text will appear just before the OK/FAIL that the "make check" test iteration that
     * the Makefile runs
     *
     * Remember, VERSION is a string-text from config.h
     */
    printf ("OK (%s) %s\n", VERSION, filterentries->identity());

    dlclose(handle);
    return 0;
}

