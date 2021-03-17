####################################
Try look at MF/Indexes/... Nav data
####################################
Author: HanishKVC
Version: v20210317IST1234
License: GPL
Status: OUT OF SYNC with PROGRAM

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
not be error free and uptodate always. So dont use this program for anything.


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

load_data(YYYY[MM[DD]], YYYY[MM[DD]], loadFiltersName=theLoadFiltersName)


LoadFilters
-------------

Many a times one may want to load only a subset of the fetched data, wrt entities in it.
LoadFilters help wrt this. As one may want to filter either based on EntityType and or
based on EntityName, so each loadfilter is a named dictionary containing

   a whitelist of matching templates wrt entityType

   a whitelist of matching templates wrt entityName

   a blacklist of matching templates wrt entityName

One can use loadfilters_setup to define these named loadFilters. One can define multiple
such named loadFilters. Even the program may define some named loadFilters.

loadfilters_setup(loadFiltersName, whiteListEntTypes, whiteListEntNames, blackListEntNames)

One can use loadfilters_list to look at the currently defined loadfilters.

Inturn while calling load_data, one can pass the optional loadFiltersName argument, to
filter entities based on the corresponding list of filters. If user doesnt specify this
argument, then the program will use the 'default' loadFilter. If user doesnt want to
filter any of the entities, then pass None wrt loadFiltersName.

NOTE: For MFs EntityType corresponds to Equity, Money market, etc. One can use
enttypes_list to get the currently known list of entity types.


Search
========

Search through the loaded data set to see if it contains MFs with matching names.

search_data("match template tokens set1 ")

search_data(["match template tokens set1", "match tokens set2", ...])

The user can specify one or more match templates to this function/command.


Match Tempaltes
=================

For each match template specified, the program will search through the currently
loaded entities. If any match is found the same will be selected and used as
appropriate based on the command.

The program tries to check if each of the word/token in the given template is present
in the names in its dataset. If all tokens in a match template are present in a given
name, then it is considered as a match. The order of the tokens does not matter.

By default the logic ignores the case of the words/tokens.

User can prefix the tokens with few predefined strings to control the matching in
a finer manner.

If a token contains -NO- prefixed to it, then the matching name shouldnt contain
this token in it.

If a token is prefixed with ~PART~, then the matching name can contain that token as
part of a bigger token. Otherwise normally each token/word should match fully.

If the matching template itself is prefixed with -RE- then it is interpreted as a
regular expression based matching template, instead of the programs internal logic.

NOTE: a token is a alphanumeric word with spaces around it, so each word in a string
is a token.

ex: search_data("direct index fund tata")
ex: search_data("fund tata index direct")
ex: search_data("fund index -NO-bonus")
ex: search_data(["direct bluechip -NO-dividend", "direct bluechip dividend us"])


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


