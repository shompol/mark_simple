#!/usr/bin/env python
"""
This is a utility that wraps a CLI application for consumption by
a foxpro program. Designed to fox-wrap any application, not just python

Features:
   -- Catches exceptions and if one happens does not terminate until user presses "enter"
   -- Writes output to log and to screen
"""
try:
    import sys
    import os
    os.environ.pop('TZ', None)  # call this first before some library somewhere used datetime with bad windoz timezone
    import datetime
    import subprocess
    import traceback
except Exception as e:
    traceback.print_exc()
    print("\nTHERE WAS AN ERROR LOADING FOXWRAP:")
    print(e)
    input("Press Enter to exit...")
    raise


def main():
    try:
        foxwrap_args = sys.argv[1:]
        progname = os.path.splitext(os.path.basename(foxwrap_args[0]))[0]
        progparams = [os.path.basename(p) for p in foxwrap_args[1:]]
        logfile_name = "{}_{}_{}.LOG".format(progname, '_'.join(progparams), datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))

        proc = subprocess.Popen(foxwrap_args, stderr=subprocess.PIPE)
        with open(logfile_name, "w") as log_file:
        
            while proc.poll() is None:
                line = proc.stderr.readline().decode()
                #print('.', end='')
                if line:
                    print(line)
                    log_file.write(line)

        if proc.returncode != 0:
            raise Exception("USER PROCESS FAILED")

    except Exception as e:
        traceback.print_exc()
        print("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST:", e)
        input("Press Enter to exit...")
        
    except:   # pylint: disable=broad-except
        traceback.print_exc()
        print("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST")
        input("Press Enter to exit...")


if __name__ == '__main__':
    main()
