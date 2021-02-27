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

    start and end need to be dictionaries {'y': year_int, 'm': month_int, 'd': date_int}
        month_int should be from 1 to 12
        date_int should be from 1 to 31; 'd' and thus inturn date_int is optional
    """
    print("INFO:proc_days:from {} to {}".format(start, end))
    for y in range(start['y'], end['y']+1):
        for m in range(1,13):
            startDate = None
            endDate = None
            if (y == start['y']):
                if (m < start['m']):
                    continue
                elif (m == start['m']):
                    startDate = start.get('d', None)
            if (y == end['y']):
                if (m > end['m']):
                    continue
                elif (m == end['m']):
                    endDate = end.get('d', None)
            print("INFO:proc_days:{}{:02}:{} to {}".format(y,m,startDate, endDate))
            for d in gCal.itermonthdays(y,m):
                if d == 0:
                    continue
                if (startDate != None) and (d < startDate):
                    continue
                if (endDate != None) and (d > endDate):
                    continue
                print(" %d "%(d))
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


def proc_datestr(dateStr, fallBackMonth=1):
    """
    Convert a date specified in YYYYMMDD format into internal date dictionary format
        MM and DD are optional.
        MM if not specified fallsback to the value passed through fallBackMonth arg.
        If DD is needed, then MM needs to be used.
    """
    year = dateStr[:4]
    month = dateStr[4:6]
    day = dateStr[6:8]
    if (year == '') or (len(year) != 4):
        exit()
    date = {}
    date['y'] = int(year)
    if month != '':
        date['m'] = int(month)
    else:
        date['m'] = fallBackMonth
    if day != '':
        date['d'] = int(day)
    return date


if len(sys.argv) > 1:
    start = proc_datestr(sys.argv[1], 1)
    end = proc_datestr(sys.argv[2], 12)
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

