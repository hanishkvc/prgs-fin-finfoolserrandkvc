####################################
Try look at MF/Indexes/... Nav data
####################################
Author: HanishKVC
Version: v20210317IST1234
License: GPL
Status: Partly OUT OF SYNC with PROGRAM

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

This is a relative look across multiple data sets.

As I didnt find any opensource program to look at the historical data of MF navs,
so created a simple one to try and look at the same in some simple, silly and
stupid ways.

Also noticed that some of the sites show the sorted list based on the last Rolling
Ret value, as it stands on the day one is looking, rather than by looking at it
for how it panned out over the full time period or so. And also some times the
rating/ranking (not sorted order) is missing for some of the entities. So also
I wanted to have a stupid look across entities (MFs/indexes/...) in stupid/silly
ways.

This fetches the Indian MFs nav data from the AMFI website.

NOTE: Rating/Ranking provided by independent 3rd parties will be based on collating
a score over multiple parameters and or some more additional validation/crosscheck.
AND this program DOESNT have intelligence for either of it, So the sorted list
this prg generates/shows is not accurate at all and can potentially be misleading
for n number of reasons.

NOTE: Even the sites will show more as well as relavant parameters to get a guage
of how the entity is performing wrt different important aspects. So please do
look at those along with the ratings from independent agencies to get the better
picture of how the entity is performing. This program doesnt do any of these more
fine grained and or appropriate analysis, so dont use this for anything other than
timepass.

NOTE: This is a purely experimental program to explore somethings, which I had in
mind. And is not suitable for making any investment or divestment decisions and or
any inferences about things/... Also I am no expert in this matter, so my logics
could be buggy and stupid in more ways than one. Also the data it works with may
not be error free and uptodate always. So dont use this program for anything.


Calling the Program
======================

If the program is called without any arguments, then it enters the interactive mode, where
user can enter standard python statements as well as the functions provided by this program.

   Terminate statement with ; to avoid implicit printing of the results of the statement.

   NOTE: This ; based termination maybe useful for the functions provided by this program.
   Especially for the functions which return list or so.

   Termination with ; is also required to import modules at runtime.

   Multiline statements are supported if user indicates the begining of such multiline
   blocks by terminating them with : or , as part of the 1st line in the multiline block.

      A empty line or a line with lesser indentation than what was at the begining of the
      multiline block, will terminate the multiline block logic.

However if the program is called with a single argument which is a file with extension ".mf",
then the program will assume that it is a script file which contains commands for the program.
They will be executed as if the user had entered them directly into the program one after the
other.



Usage
#######

Some of the functions supported by the program are specified in the sections below.

NOTE: calling help on any of the function will get some basic usage info about them.

   help(function_name)

A Entity could refer to a mutual fund or index or ... one may load into this program.

NOTE: When comparing entities, if they have been active for different amount of time
within the current date range that has been loaded and looked at, then the results
may not give the full picture, depending on how one looks at things.


Fetching
==========

One can use fetch_data to fetch historical nav data.

fetch_data(YYYY[MM[DD]])

fetch_data(YYYY[MM[DD]], YYYY[MM[DD]])

fetch_data(YYYY[MM[DD]], YYYY[MM[DD]], opts)

   opts argument is a dictionary which can one of the two booleans

      'ForceRemote': The logic will try to fetch data from internet,
         irrespective of whether there is a local data pickle file
         or not.

      'ForceLocal': The logic will avoid fetching the data file
         from the internet, even if data pickle is missing/invalid.
         Instead it will reconstruct the pickle from existing local
         data file.

      NOTE: ForceRemote takes precendence over ForceLocal

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

   Rather one requires to remove the data pickle files for the new logic to try and
   recheck with the internet for previously downloaded data. May add a force argument
   to fetch or so in future.


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

The user can specify one or more match templates to this function/command. If one
wants to check wrt multiple match templates, then pass it has a list of strings.


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


Processing Data
===================

procdata_ex(<ListOfOperations>)

procdata_ex("srel=srel(data)")

procdata_ex(["srel=srel(data)", "dma20=dma20(data)", "roll1Y=roll365(data)"])

procdata_ex(["srel=srel(data)", "dma20=dma20(srel)", "roll1Y=roll365(data)", "dma50Roll1Y=dma50(roll1Y)"])

procdata_ex(["srel=srel(data)", "dma20SRel=dma20(srel)", "roll1Y=roll365(data)", "dma50Roll1Y=dma50(roll1Y)"])

NOTE: help(procdata_ex) will give some of the details about using this.

srel - safe relative
----------------------

calculates the relative percentage difference for all data in the dataset, wrt the
value of the same entity on the starting date (which defaults to start of the dateRange
of data loaded). If a given entity has no value available for the given start date, then
the next earliest available non zero value will be used as the base.

It also stores the following as part of MetaData associated with it

   the AbsoluteReturn as well as the ReturnsPerAnnum, as on the last date
   in the date range

   the Period for which the entity was active for the current date range.

      NOTE: This only looks at starting date and not end date. So if a fund
      is no longer active, but was active for part of the date range, its
      life will be assumed to be till end of date range. One can notice such
      situation by looking at the plot of data and seeing the last active value
      stretching without change till end of date range.


dma - moving average
----------------------

dstDataKey=dma<Days>(srcDataKey)

ex: dma50Data=dma50(data)

It calculates the moving average over a specified number of days, for the full dataset.

Some common window size one could use for moving average are 20, 50, 200, ...



roll - rolling return
-----------------------

dstDataKey=roll<Days>(srcDataKey)

ex: rollData=roll365(data)

It calculates rolling returnPerAnnum over the full dataset, wrt given rollingReturn windowSize.

Some common window sizes one could use are 365 (i.e 1Yr), 1095 (i.e 3Yr), 1825 (i.e 5Yr).

It also stores the following additional meta data:

   Average rolling return over the full date range

   Standard Deviation and Average standard deviation from across sub-timeBlocks
   wrt rolling return over the full date range.

   Percentage of times, when the return was below a predefined minimum value like 4% (the default).

   Average rolling return over sub-timeBlocks within the overall date range. For large date ranges,
   it will be for ~every year.

   Standard Deviation wrt rolling returns in each sub-timeBlocks within the overall date range.
   For large date ranges, it will be for ~every year.

   Quantiles of the rolling return for each of the sub-timeBlocks within the overall date range.

NOTE: Full dataset means for all the entities and over the full date range for which data
is loaded.


Look at raw/processed data
=============================


analdata_simple
-----------------

Some of the operations supported include

   roll_avg: The dataSrc should be one generated using roll<Days> operation of procdata_ex.
   This looks at the full period average of the rolling returnPerAnnum over the full dateRange
   loaded, for each entity, to decide how to rank the entities.

      analdata_simple('roll1095', 'top', 'roll_avg')

   roll_ranked: The dataSrc should be one generated using roll<Days> procdata_ex oepration.
   This identifies the pentile to which each entity belongs, when compared to all other
   entities specified, wrt each sub time period to which the overall date range will be
   divided. Inturn it calculates a naive average of the pentile rank across all the sub
   date ranges, and uses the same to rank the entities.

      NOTE: One needs to be extra careful, when trying to interpret this result.
      If one sees change in ranking between roll_avg and roll_ranked, look at the rank array
      to try and see why it might be so. Maybe the entity was performing good in only some of
      the sub-timeblocks (or it peformed bad over many sub-timeblocks or ...) in the overall
      date range or so...


Others
--------

help(plot_data)

help(show_plot)




Saving and Restoring Session
==============================

One can use session_save to save the gData corresponding to the currently loaded data, into
disk. ANd inturn one can use session_load to restore a previously saved session back into
runtime memory. This can help with avoiding the need to go through the individual data files
and build the in memory data, which can save lot of time. This is not a full save and restore
of the runtime session of the program, so one needs to understand the program flow and its
implications, before using it.


Older logic, Not yet updated, wrt new logics/flows (i.e if reqd)
#################################################################

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


Misc Notes
==============

As readme is created on a different day compared to when the logic is/was implemented, so
there could be discrepencies, as I havent cross checked things, when putting what I remember
into this document.
