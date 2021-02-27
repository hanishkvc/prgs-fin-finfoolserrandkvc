#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021

import sys
import calendar

#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
gBaseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}-{}-{}"
gCal = calendar.Calendar()


def proc_days(start, end, func):
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
                func(y,m,d)


def handle_date(y, m, d):
    print(y,m,d)
    url = gBaseURL.format(d,calendar.month_name[m][:3],y)
    print(url)


start = { 'y': int(sys.argv[1]), 'm': 1 }
end = { 'y': int(sys.argv[2]), 'm': 12 }
proc_days(start, end, handle_date)

