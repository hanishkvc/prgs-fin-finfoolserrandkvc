###########################
Try look at MF Nav data
###########################
Author: HanishKVC
Version: v20210308IST1308
License: GPL

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.


Overview
#########

As I didnt find any opensource program to look at the historical data of MF navs,
so created a simple one to try and look at the same in some simple and stupid ways.

This fetches the nav data from the AMFI website.

NOTE: This is a purely experimental program to explore somethings, which I had in
mind. And is not suitable for making any investment or divestment decisions and or
any inferences about things/... Also I am no expert in this matter, so my logics
could be buggy and stupid in more ways than one. Also the data it works with may
not be error free and uptodate always.


Usage
#######

Some of the functions supported by the program include

Fetching
==========

One can use fetch_data to fetch historical nav data.

fetch_data(YYYY[MM[DD]])

fetch_data(YYYY[MM[DD]], YYYY[MM[DD]])

Some sample usage:

   fetch_data(2020)

      This will try to fetch data from 1st Jan 2020 to 31st Dec 2020.

   fetch_data(2020, 202101)

      This will try to fetch data from 1st Jan 2020 to 31st Jan 2021.

   fetch_data(20100501, 2018)

      This will try fetch data from 2010 May 1st to 2018 Dec 31st.

NOTE: If the given range goes into the future, then it wont try to fetch data belonging
to the future.

NOTE: As NAV data for yesterday, could get updated anytime during the current day and
sometimes even beyond in some worst cases. So data fetched by this program need not be
accurate in some cases. If one tries to refetch the same date range as before, at a later
date, then it tries to see if there is any update to the nav data, and if it appears so,
then it will redownload the same. HOWEVER as the program uses the size of the nav data
file and that too only if the length is larger than what it had downloaded previously,
so it need not download the uptodate historical data in some cases. SO DONT DEPEND ON
THIS PROGRAM for any decisions or inferences or ...


Loading
==========

Once the historical nav data has been fetched. One can load a specific date range of this
data to have a look at it.

load_data(YYYY[MM[DD]])

load_data(YYYY[MM[DD]], YYYY[MM[DD]])


Search
========

Search through the loaded data set to see if it contains MFs with matching names.

The user is required to specify a string containing tokens (a token is a alphanumeric
with spaces around it, so each word in a string is a token). THe program will try to
see if any of the MFs in the dataset contain all the tokens in the given string. If so,
the corresponding MF name will be printed.

The user can prefix -NO- to the tokens if required, in which case, if a MF name contains
the corresponding token, the MF name will be skipped.

search_data("match tokens -NO-SkipIfThisFound -NO-SkipEvenIfThisFound")


LookAt
=======

Basic use
----------

One can look at the data belonging to the specified list of MFs.

THe list of MFs to look at is specified as a list of strings. The program will
try to see if any of the MFs in the dataset contain all the tokens in any of the
given strings. If so, the corresponding MF name will be selected, and its data
can be looked at.

One can either look at

   the raw data or

   relative to start date or

   as a moving average over specified number of days or

   as a rolling return across specified number of days.

It will also print the absolute and per annum return.

lookat_data(<ListOfMFNameMatchTokens>, dataProcs=<ListOfDataProcs>)

ONe specifies the type of data to look at by setting the dataProcs, suitably into either

   "raw" and or "rel" and or "dma_N" and or "roll_N"; where N specifies the number of days.

One can call lookat_data multiple times, to build up the set of MFs and their data one
is interested in looking at and then at the end call show_plot, to get a plot all the
data in one shot.

If called multiple times, it should always be wrt to the same date range.

Calling load_data or show_plot will clear the date range, so that the user is free to
work with a new date range.


DateRange
----------

User can optionally specify startDate and endDate as arguments.

If startDate is not specified, it will be mapped to the startDate specified during load_data.

If endDate is not specified, it will be mapped to the endDate specified during load_data.


Calling the Program
======================

If the program is called without any arguments, then it enters the interactive mode, where
user can enter the above and few more program provided functions as well as generic python
expressions.

However if the program is called with a single argument which is a file with extension ".mf",
then the program will assume that it is a script file which contains commands for the program.
They will be executed as if the user had entered them directly into the program one after the
other.


