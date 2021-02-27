#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021

import sys
import calendar

#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
gBaseURL = "https://www.amfiindia.com/spages/NAVAll.txt?t={}"
gCal = calendar.Calendar()

def get_days(start, end):
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
                print(" %d"%(d), end="")

start = { 'y': int(sys.argv[1]), 'm': 1 }
end = { 'y': int(sys.argv[2]), 'm': 12 }
get_days(start, end)

