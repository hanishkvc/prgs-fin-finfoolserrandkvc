#!/usr/bin/env python3
# Helper routines
# HanishKVC, 2021
# GPL

import os


def wget_better(url, localFName):
    """
    If the file on the server is bigger than the local file,
    then redownload the file freshly, rather than appending to it,
    as chances are the local file was not a partial download, but
    rather a older version of the file with different data.
    """
    #cmd = "curl {} --remote-time --time-cond {} --output {}".format(url,fName,fName)
    if os.path.exists(localFName):
        mtimePrev = os.stat(localFName).st_mtime
    else:
        mtimePrev = -1
    cmd = "wget '{}' --continue --output-document={}".format(url,localFName)
    print(cmd)
    os.system(cmd)
    if os.path.exists(localFName):
        mtimeNow = os.stat(localFName).st_mtime
        if (mtimePrev != -1) and (mtimeNow != mtimePrev):
            os.remove(localFName)
            os.system(cmd)


