#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021

import sys
import calendar
import os

#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
gBaseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}-{}-{}"
gCal = calendar.Calendar()


def proc_days(start, end, handle_date_func):
    """
    call the passed function for each date with the given start and end range.
        The date will be passed to the passed function as year, month, date
        as integers.

    start and end need to be dictionaries {'y': year_int, 'm': month_int}
        month_int is from 1 to 12
    """
    for y in range(start['y'], end['y']+1):
        for m in range(1,13):
            if (y == start['y']) and (m < start['m']):
                continue
            if (y == end['y']) and (m > end['m']):
                continue
            print()
            for d in gCal.itermonthdays(y,m):
                if d == 0:
                    continue
                print(" %d "%(d), end="")
                handle_date_func(y,m,d)


def fetch4date(y, m, d):
    """
    the default handler function passed to proc_days.

    One can call this directly by passing the year, month and date one is interested in.
        month should be one of 1 to 12
    """
    print(y,m,d)
    url = gBaseURL.format(d,calendar.month_name[m][:3],y)
    print(url)
    cmd = "wget {} --continue --output-document=data/{}{:02}{:02}.csv".format(url,y,m,d)
    os.system(cmd)


if len(sys.argv) > 1:
    start = { 'y': int(sys.argv[1]), 'm': 1 }
    end = { 'y': int(sys.argv[2]), 'm': 12 }
    proc_days(start, end, fetch4date)
else:
    bQuit = False
    while not bQuit:
        try:
            cmd = input(":")
            exec(cmd,globals())
        except:
            excInfo = sys.exc_info()
            if excInfo[0] == SystemExit:
                break
            print(excInfo)

