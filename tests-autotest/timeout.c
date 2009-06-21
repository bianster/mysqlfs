#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>

/* global for the money-shot */
int _pid = -1;

void family_assassination (int ignored)
{
    (void) printf ("%s\n", "timeout");
    if (-1 != _pid) kill (_pid, SIGTERM);
    if (-1 != _pid) sleep(1);
    if (-1 != _pid) kill (_pid, SIGKILL);
    exit(1);
}

int main (int argc, char *argv[])
{
    char opt;
    char testing = 1;
    char background = 1;
    unsigned sleeper = 1;
    struct sigaction timeout_sigaction =
    {
        .sa_handler = family_assassination
    };

    (void) sigaction (SIGALRM, &timeout_sigaction, NULL);

    while (-1 != (opt = getopt(argc, argv, "Bt:")))
    switch (opt)
    {
        case 'B': /* background for the daemon */
            background = 1;
            break;
        case 't': /* timeout */
            sleeper = atoi(optarg);
            testing = 0;
            break;
        case '?': /* unknown; ignored */
	    fprintf (stderr, "unknown option\n");
	    return -2;
    }

    if (optind >= argc)
       return 1;
    /*
     * parent returns immediately with a good return to allow backgrounding daemons (themselves
     * non-backgrounding) so that the parent test-runner can continue, letting the test daemon be
     * killed on timeout
     */
    else if (0 < background)
    {
        switch (fork())
        {
            case 0: /* child */
                break;
            default: /* parent */
                return 0;
        }
    }

    /* else */
    switch (_pid = fork())
    {
        case 0: /* child */
            if (testing)
            {
                printf ("argc %d ; optind %d; argv[optind] [%s]\n", argc, optind, argv[optind]);
                sleep (5+sleeper);
            }
            else
            {
                execvp (argv[optind], &argv[optind]);
                return 1;
            }
            
            return 0;

        default: /* parent */
            /* set the morning wakeup */
            alarm (sleeper);

            /* and snooze for the child forked pid */
            //(void) wait (NULL);
            /* and wait for SIGCHLD or SIGALRM */
            (void) pause();

            /* if we got here, the pid finished */
            return 0;
    }
}
