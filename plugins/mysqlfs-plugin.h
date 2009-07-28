/** @file */

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

/**
 * Structure used by plugins to give entrypoints for function-calls
 *
 * Where the setblocksize() function entrypoint is defined, I could have done this as an array, but
 * there's only a few config items.  I'm not that confident we'll grow to a crazy out-of-control
 * number of config items that cannot be address plugin-by-plugin via fake files.  I didn't want to
 * have multiple configs that need to be manually kept in-sync lockstep (such as a blocksize declared
 * in two different header files or even in two weakly-connected projects), so I am making the FUSE
 * layer pass config data to plugins -- allows also for the plugin to refuse/error as well.
 *
 * The general logic of this structure is that if a plugin has a concern about something, it creates
 * a non-null entry point.  If it doesn't care (for example, if it always has a happy status, or
 * doesn't care about written blocks, only read ones) then that entrypoint is simply NULL.
 *
 * Every plugin should have an identity entrypoint
 */
struct s_mysqlfs_plugin {
    char *(*identity)(void);		/**< entrypoint for a simple "who are you?" descriptive text (PSZ) */
    int (*status)(void);		/**< entrypoint for a status code: 0 is OK, anything else implies a statusmsg */
    char *(*statusmsg)(void);		/**< entrypoint for a status description -- if status is non-zero, this function describes why */
    void (*setblocksize)(unsigned);	/**< entrypoint to allow setting of the data blocksize (max 64k) -- see comments in s_mysqlfs_plugin doc */

    int (*read) (unsigned char*, size_t, off_t);	/**< pass-thru hook for in-place munging of data in the read direction */
    int (*write) (unsigned char*, size_t, off_t);	/**< pass-thru hook for in-place munging of data in the write direction */
};
typedef struct s_mysqlfs_plugin mysqlfs_plugin;

