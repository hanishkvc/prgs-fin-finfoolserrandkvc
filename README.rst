####################################
Try look at MF/Indexes/Stock... data
####################################
Author: HanishKVC
Version: v20210414IST0126
License: GPL
Status: Not fully updated wrt new version of PROGRAM

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

This is a relative look across multiple data sets having data over a period
of time.

As I didnt find any opensource program to look at the historical data of MF navs,
so created a simple one to try and look at the same in some simple, silly and
stupid ways.

Also noticed that some of the sites show the sorted list based on the last Rolling
Ret value, as it stands on the day one is looking, rather than by looking at it
for how it panned out over the full time period or so. And also some times the
rating/ranking (not sorted order) is missing for some of the entities. So also
I wanted to have a stupid look across entities (MFs/indexes/...) in stupid/silly
ways.

This tries to fetch the Indian MFs nav data from the AMFI website and Index data
from nse website. As one can look at this data from their website for personal use,
so I am assuming that it should be fine to fetch the data again for personal use,
but if you plan to access the data from these sources for any other use, do cross
check with the data sources once, before fetching and using the data from them.
Also avoid overloading their servers when trying to fetch, by fetching spread over
a long time and not at once.

NOTE:
Rating/Ranking provided by knowledgable independent 3rd parties will be based on
collating a score over multiple parameters and or some more additional validation
/crosscheck. THIS PROGRAM DOESNT have intelligence for either of it, So sorted list
this prg generates/shows is not accurate at all and can potentially be misleading
for n number of reasons.

NOTE: Financial sites will show more as well as relavant parameters to get a guage
of how the entity is performing wrt different important aspects. So please do
look at those along with the ratings from independent agencies to get the better
picture of how the entity is performing. This program doesnt do any of these more
fine grained and or appropriate analysis, so dont use this for anything other than
timepass.

NOTE: This is a purely experimental program to explore somethings, which I had in
mind. And is not suitable for making any investment or divestment decisions and or
any inferences about things/... Also I am no expert in this matter, so my logics
could be buggy and stupid in more ways than one. Also the data it works with may
not be error free and may not be uptodate always. So dont use this program for
anything.

The logic has also been updated to fetch historic/bhav data from nse website,
so that one can look at historic stock data in blind and stupid ways.


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

However if the program is called with a single argument which is a file with extension ".ffe",
then the program will assume that it is a script file which contains commands for the program.
They will be executed as if the user had entered them directly into the program one after the
other.


Paths used
============

The program by default stores downloaded files into ~/.cache/ffe folder.

User can change this by setting a environment variable called FINFOOLSERRAND_BASE.

The program will show the paths being used, when it is run.


Usage
#######

Some of the functions supported by the program are specified in the sections below.

Calling help on any of the function will get some basic usage info about them.

   help(function_name)

The main data base / dataset maintained by this program is called edb (entities db).
A Entity could refer to a mutual fund or index or ... one may load into this program.

NOTE: When comparing entities, if they have been active for different amount of time
within the current date range that has been loaded and looked at, then the results
may not give the full picture, depending on how one looks at things.

NOTE: Currently, by default it duplicates the respective last valid data for holidays
(including weekends). So there will be some variation wrt measures/data which depends
on historic data. [[[ One can change this behavior by setting appropriate variable in
datasrc module. However other logics like rolling ops etc dont account for differences
due to this. Partly updated now, need to cross check things once and add additional
logic if any required]]]

NOTE: By default it fetchs/loads data only till yesterday.

A sample session could involve

   edb.fetch(2013, 2021)
   edb.load(2013, 2021)
   procedb.infoset1_prep()
   edb.enttypes();
   procedb.infoset1_result(['open equity large', 'open equity large mid', 'open equity flexi', 'open equity multi', 'open equity elss'], ['direct'])
   procedb.infoset1_result(['open equity elss', 'open hybrid aggressive'], ['direct'])
   edb.enttype_members('nse index')
   plot.data('srel', 'nse index', ['-RE-Nifty 50', 'smlcap'])
   # A simple look at stocks only
   stocks.load()
   stocks.prep()
   stocks.topbottom()
   stocks.plot(['STOCKSYMBOL1', 'STOCKSYMBOL2'])
   import crazy;
   crazy.above_ndays()
   hlpr.print_list(crazy.below_ndays())


Fetching
==========

One can use edb.fetch_data (edb.fetch will also do) to fetch historical data.

edb.fetch_data(YYYY[MM[DD]])

edb.fetch_data(YYYY[MM[DD]], YYYY[MM[DD]])

edb.fetch_data(YYYY[MM[DD]], YYYY[MM[DD]], opts)

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

   edb.fetch_data(2020)

      This will try to fetch data from 1st Jan 2020 to 31st Dec 2020.

   edb.fetch_data(2020, 202101)

      This will try to fetch data from 1st Jan 2020 to 31st Jan 2021.

   edb.fetch_data(20100501, 2018)

      This will try fetch data from 2010 May 1st to 2018 Dec 31st.

   edb.fetch_data(202103, opts={ 'ForceRemote': True })

      This will try refetch the data for 2021 March from the internet again,
      even if it is already downloaded, ie if there is any change in size of
      the data file on the server.

NOTE: If the given range goes into the future, then it wont try to fetch data belonging
to the future.

NOTE: Wrt MF as NAV data for yesterday, could get updated anytime during current day and
sometimes even beyond in some worst cases. So data fetched by this program need not be
accurate in some cases. If one tries to refetch the same date range as before, at a later
date, then it tries to see if there is any update to the nav data, and if it appears so,
then it will redownload the same. HOWEVER as the program uses the size of the nav data
file and that too only if the length is larger than what it had downloaded previously,
so it need not download the uptodate historical data in some cases. SO DONT DEPEND ON
THIS PROGRAM for any decisions or inferences or ...

NOTE: There could be bug wrt parsing downloaded data csv files and or issues with saving
and restoring pickle. So also the things done/shown by the program could be wrong.

NOTE: Program checks for and then if required introduces a minimum gap in time between
successive downloads during fetching, so that one doesnt overload internet and or servers.


Loading
==========

Once the historical data has been fetched. One can load a specific date range of this data
to have a look at it.

edb.load_data(YYYY[MM[DD]])

edb.load_data(YYYY[MM[DD]], YYYY[MM[DD]])

edb.load_data(YYYY[MM[DD]], YYYY[MM[DD]], loadFiltersName=theLoadFiltersName)

TOTHINK: edb.load_data can be configured to try and fetch the data, if its not already fetched.
Need to think, if I will re-enable this logic again. However if you want to force a redownload
etc, then you have to call edb.fetch_data directly with appropraite arguments.

The edb.load_data (edb.load can also be used), will download from all types of data sources by
default. However if one wants to download only MF or only Stock related data, then one can pass
dataSrcType argument as required. Or else call edb.load_mfs or edb.load_stocks.


LoadFilters
-------------

Many a times one may want to load only a subset of the fetched data, wrt entities in it.
LoadFilters help wrt this. As one may want to filter either based on EntityType and or
based on EntityName, so each loadfilter is a named dictionary containing

   a whitelist of matching templates wrt entityType

   a whitelist of matching templates wrt entityName

   a blacklist of matching templates wrt entityName

One can use loadfilters.setup to define these named loadFilters. One can define multiple
such named loadFilters. Even the program may define some named loadFilters.

loadfilters.setup(loadFiltersName, whiteListEntTypes, whiteListEntNames, blackListEntNames)

One can use loadfilters.list to look at the currently defined loadfilters.

Inturn while calling edb.load_data, one can pass the optional loadFiltersName argument, to
filter entities based on the corresponding list of filters.

   If user doesnt specify this argument, then the program will set this to a special
   LOADFILTERSNAME_AUTO loadFilters. Which automatically maps to the loadFilters prefered
   by the individual data sources.

   If user doesnt want to filter any of the entities, then pass None wrt loadFiltersName.



Search
========

Search through the loaded data set to see if it contains entities with matching names.

edb.search_data("match template tokens set1 ")

edb.search_data(["match template tokens set1", "match tokens set2", ...])

The user can specify one or more match templates to this function/command. If one
wants to check wrt multiple match templates, then pass it has a list of strings.

NOTE: This searches for entities with matching name, across all the entity types in the
entities database. However if one wants to find entities with matching name belonging
to a subset of the entTypes, then use edb.enttype_members.


Match Tempaltes
=================

For each match template specified, the program will search through the currently
loaded entities database. If any match is found the same will be selected and used
as appropriate based on the command.

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
In this case to ignore case, one will have to use -RE-(?i).

NOTE: a token is a alphanumeric word with spaces around it, so each word in a string
is a token.

entTypeTmpls correspond to matching templates used wrt finding suitable entity types.
While entNameTmpls correspond to finding matching entity names.

ex: search_data("direct index fund tata")
ex: search_data("fund tata index direct")
ex: search_data("fund index -NO-bonus")
ex: search_data(["direct bluechip -NO-dividend", "direct bluechip dividend us"])


Processing Data - procedb.ops
===============================

procedb.ops(<ListOfOperations>)

procedb.ops("srel=srel(data)")

procedb.ops(["srel=srel(data)", "mas20=mas20(data)", "roll1Y=roll365(data)"])

procedb.ops(["srel=srel(data)", "mas20=mas20(srel)", "roll1Y=roll365(data)", "mas50Roll1Y=mas50(roll1Y)"])

procedb.ops(["srel=srel(data)", "mas20SRel=mas20(srel)", "roll1Y=roll365(data)", "mas50Roll1Y=mas50(roll1Y)"])

NOTE: help(procedb.ops) will give some of the details about using this.

srel - safe relative
----------------------

calculates the relative percentage difference for all data in the dataset, wrt the
value of the same entity on the starting date (which defaults to start of the dateRange
of data loaded). If a given entity has no value available for the given start date, then
the next earliest available non zero value will be used as the base.

NOTE: calculate based on ValueOnEachDay/ValueOnGivenDate

It also stores the following as part of MetaData associated with it

   the AbsoluteReturn as well as the ReturnsPerAnnum, as on the last date
   in the date range

   the Period for which the entity was active for the current date range.

      NOTE: This only looks at starting date and not end date. So if a fund
      is no longer active, but was active for part of the date range, its
      life will be assumed to be till end of date range. One can notice such
      situation by looking at the plot of data and seeing the last active value
      stretching without change till end of date range.



rel - relative to given date
-----------------------------

Calculate the relative percentage difference for all data in the dataset, wrt the
value of the same entity on the given base date, wrt each entity.

NOTE: calculate based on ValueOnEachDay/ValueOnGivenDate

As part of its associated meta data, it stores the following info calculated btw
the endDate and baseDate

   the absolute return

   the return per annum

   duration in years


reton - return on given date
------------------------------

Calculate the relative percentage difference (appreciation/depreciation) on a given
date relative to all other dates in the dataset, for each entity.

NOTE: calculate based on ValueOnGivenDate/ValueOnEachDay


mas - moving average simple
-----------------------------

dstDataKey=mas<Days>(srcDataKey)

ex: mas50Data=mas50(data)

It calculates the moving average over a specified number of days, for the full dataset.

Some common window size one could use for moving average are 20, 50, 200, ...

All data points in the window are given same weightage.


mae - moving average exponential
----------------------------------

dstDataKey = mae<Days>(srcDataKey)

ex: mae50Data = mae50(data)

Calculates exponential moving average wrt the specified number of days, for the full dataset.

For each date, the nearest date data will have higher weightage compared to older/farther date
data.


roll - rolling return
-----------------------

dstDataKey=roll<Days>(srcDataKey)

ex: rollData=roll365(data)

It calculates rolling returnPerAnnum over the full dataset, wrt given rollingReturn windowSize.

Some common window sizes one could use are

   If weekends are not skipped, then 365 (i.e 1Yr), 1095 (i.e 3Yr), 1825 (i.e 5Yr).

   If weekends are skipped, then 260 (i.e 1Yr), 782 (i.e 3Yr), 1303 (i.e 5Yr).

It also stores the following additional meta data:

   Average of the rolling return over the full date range.

   Standard Deviation of the rolling return over the full date range.

   Percentage of times, when the return was below a predefined minimum value like 4% (the default).

   Adjusted Average of Rolling return (wrt MinThreshold) divided by StdDev of Rolling return
   [ MaShaMT = (Avg-MinT)/Std ]

   For how many years we have data about the entity.

NOTE: If comparing entities which have been active for different amounts of time, then the
results may not be directly comparable, do remember that, as they all wouldn't have gone through
the same cycle of events. Also because the MetaData stored accounts for its active period only,
and ignores any time duration at begin or end, when there is no data (ie not alive/active/...).
The logic does save the years active info, so one can use it when comparing other attributes,
to get a rough sense of things.


block - avg,std wrt each block
-------------------------------

dstDataKey=block<Days>(srcDataKey)

Calculate the following wrt values in each block of BlockDays from the dateRangeEnd towards dateRangeStart,
for the given srcDataKey.

   average of values wrt each block

   standard deviation of the values wrt each block

   quantile(quartile) of values wrt each block

As part of the MetaLabel give the following info:

   A list containing average of values wrt each block in the date range.

   Average of the averages across each block.

   Average of the standard deviations across each block.

   Quantiles of the rolling return for each of the sub-timeBlocks within the overall date range.



NOTE: Full dataset means for all the entities and over the full date range for which data is loaded.

NOTE: IN the above operations where <Days> is mentioned, one can either pass the number of days directly
Or else one can pass the duration notations of ?W or ?M or ?Y (? == any number) to specify a given num
of weeks or months or years, as the case may be. If one uses the duration notation, then the program,
will automatically use a roughly appropriate number of days based on whether skipping of weekends is
currently enabled or not.


Look at raw/processed data
=============================


procedb.anal_simple
----------------------

Sort/Order the entities in the dataset based on the criteria (analType and sort order) given

Some of the analTypes supported include

   normal: Depending on the value in the given dataSrc on the given date or index, decide
   how to order the entities.

   srel_absret: The dataSrc should be one generated using srel procedb.ops operation.
   Look at the associated absoluteReturn value for each of the specified entities, and
   order the entities.

   srel_retpa: The dataSrc should be one generated using srel procedb.ops operation.
   Look at the associated returnPerAnnum value for each of the specified entities, and
   order the entities.

   roll_avg: The dataSrc should be one generated using roll<Days> operation of procedb.ops.
   This looks at the full period average of rolling returnPerAnnum over the full dateRange
   loaded, for each entity, to decide how to order the entities.

   block_ranked: The dataSrc should be one generated using block<Days> procedb.ops oepration.
   This identifies the pentile to which each entity belongs, when compared to all other
   entities loaded, wrt each block period. Inturn it calculates a naive average of the
   pentile rank across all the blocks, and uses the same to order the specified subset of
   entities.

      NOTE: One needs to be extra careful, when trying to interpret this result.
      If one sees change in ordering between roll_avg and block_ranked(of blockOp on roll data),
      look at the rank array to try and see why it might be so. Maybe the entity was performing
      good in only some of the blocks (sub time periods) (or it peformed bad over many blocks
      or ...) in the overall date range or so...

      NOTE: If number of entities loaded is small, then block_ranked pentile ranking
      may not be useful always. (Here we are talking about the total number of entities,
      in the loaded dataset and not the subset that may be selected for sorting using
      entCodes).

Example usage:

      procedb.anal_simple('roll3Y', 'roll_avg', 'top')


procedb.infoset1
-------------------

Print some possibly useful info about the entities in the loaded set. It prints data about
each entity individually as well as for each type of data, it will provide comparative prints.
Wrt these comparative prints, it tries to order the entities, based on the average of the
3 year rolling rets. However if a entity has not been active for 3 years, then such entities
will get bundled to the end of the ordered list, based on the last return per annum data
available for such entities (wrt its start date).

User needs to first run procedb.infoset1_prep, before calling one of the procedb.infoset1_result calls.
This will print processed data, wrt specified entities, based on what was generated during
procedb.infoset1_prep.

procedb.infoset1_prep()

   process the raw data using a standard set of operations like srel, roll3Y, roll5Y
   and reton, in order to generate possibly useful info.

procedb.infoset1_result()

   Display processed data wrt all entities in the loaded dataset.

procedb.infoset1_result(listOfEntityTypeMatchTemplates)

   Display processed data wrt all entities which belong to any of the matching entTypes.

   ex: procedb.infoset1_result('elss')

   ex: procedb.infoset1_result('open large')

   ex: procedb.infoset1_result(['elss', 'open large', 'open flexi', 'open multi'])

procedb.infoset1_result([], listOfEntityNameMatchTemplates)

   Display processed data wrt entities, whose name match any of the given entName matching template.

   ex: procedb.infoset1_result([], 'axis')

   ex: procedb.infoset1_result([], 'pgim direct')

   ex: procedb.infoset1_result([], ['nifty direct', 'nasdaq direct'])

procedb.infoset1_result(listOfEntityTypeMatchTemplates, listOfEntityNameMatchTemplates)

   Display processed data wrt entities, which belong to one of the matched entTypes and inturn
   whose name matches any of the passed entNameMatchTemplate. The user can select between
   resultType 'result1' and or 'result2', this decides how the subset of entities displayed
   are identified.

procedb.infoset1_result1_entcodes(listOfEntCodes)

   Display processed data for the list of entities specified using their entCode. User can create
   the passed list of entCodes using any mechanism they find suitable and or need.

procedb.infoset1_result2_entcodes(listOfEntCodes)

   This identifies the top N and bottom N entities based on absolute return wrt last 1 day, 7 days,
   1 month and 3 month and inturn show some of the data corresponding to all the entities identified
   till then.

   If no entCodes list passed, then it looks at all the entities, when identifying the top/bottom N
   entities. Else it identifies the top/bottom N entities from within the passed list of entities.

NOTE: By default only 20 entities are printed as part of the comparitive prints, if you want to
change this, pass numEntities argument to procedb.infoset1_result.


Processed Datas
-----------------

Absolute Return

Return per annum

Moving average

Rolling Return

Standard Deviation

MaSharpeMT

   A ratio between the adjusted average (wrt a predefined value) of a given set of values
   to their standard deviation.

MaBeta

   A measure of how similar or not is the changes in values of a given entity wrt changes
   in value of another entity.

Quantile


Plot Functions
-----------------

help(plot.data)

help(plot._data)

help(plot._linregress)

help(plot.linregress)

help(plot.show)


Processing Data - ops module
===============================

ops.pivotpoints

ops.weekly_view

ops.monthly_view

ops.ma_rsi

   calculate rsi based on simple moving average of gain and loss.


Entity types
==============

The entities (MFs/stocks/indexes/...) maintained by the program could belong to different
categories/types.

edb.enttypes()
-----------------

Will list all the types currently known to the program. Loading of data will set this list.

for example:

   wrt MFs, it could be

      open ended equity
      money market
      hybrid etc

   wrt Stocks, it could be

      Index
      Nifty 50
      Nifty smallcap
      NSE Pharma
      ...


edb.enttype_members(entTypeTmpls, entNameTmpls)
------------------------------------------------

List all the entities belonging to the given entTypes. If entNameTmpls is also provided,
then only list those entities, whose name matches one of the passed entName match template.



Saving and Restoring Session
==============================

One can use session_save to save entities db which corresponds to the currently loaded data,
into disk. Inturn one can use session_restore to restore a previously saved session back into
runtime memory.

This can help with avoiding the need to go through the individual data files and build the in
memory data, which can save lot of time. This is not a full save and restore of the runtime
session of the program, so one needs to understand the program flow and its implications,
before using it. But it can help speed up working with datasets across multiple runtime
sessions in a relatively fast way. Note that this also saves and restores any of the
processed data sets and not just the initial raw data set.

./FinFoolsErrandKVC.py
OO>edb.load(2013,2021)
OO>procedb.infoset1_prep()
OO>procedb.infoset1_result('elss')
OO>session_save('mysave1869')
OO>quit()

./FinFoolsErrandKVC.py
OO>session_restore('mysave1869')
OO>procedb.infoset1_result('index')



Helper Modules
================

Stocks
---------

THis provides some simple helper functions to look at stocks.

stocks.load()

   THis loads last 7 years of stocks related data.

stocks.prep()

   This calculates certain things like mas50, mas200, mae24, mae 50, roll3Y,
   roll5Y, and so on.

stocks._plot('STOCK_SYMBOL')
stocks.plot(['STOCK_SYMBOL1', 'STOCK_SYMBOL2', ...])

   Look at the data and corresponding moving averages and linear regression line
   fit wrt the given stock(s). It also shows the pivot point lines wrt latest
   day, week and month based data.


stocks.topbottom()

   Look at the stocks which were the top or bottom N over the last day, week, month.
   Be warned that this is based on simple absolute return. Inturn it will show some
   related data wrt these stocks.



Crazy
-------

This module is not imported by default. User has to explicitly import it by giving
the below command.

import crazy;

Remember to load data such that it ends on a working day and not a holiday/weekend,
so that there is valid data on the last date in the date range currently loaded.
Or else control dataIndex and cmpEndDateIndex such that they dont fall on a
holiday/weekend.

crazy.above_ndays()

crazy.below_ndays()




Misc 
######


DateRange
==========

User can optionally specify startDate and endDate as arguments.

If startDate is not specified, it will be mapped to the startDate specified during edb.load_data.

If endDate is not specified, it will be mapped to the endDate specified during edb.load_data.


Misc Notes
==============

As readme is created on a different day compared to when the logic is/was implemented, so
there could be discrepencies, as I havent cross checked things, when putting what I remember
into this document.

TODO
------

Handle stock splits

Handle stock dividends




Changes
----------

This notes only some of the changes, once in a bluemoon, look at git log for all changes.


20210325IST0104

THe logic updated to take care of recreating the data pickles, wrt fetched data,
due to the restructuring involving splitting of gData into gData and gMeta.

In case this doesnt seem to work for you, you can always force things by calling
edb.fetch_data and passing ForceRemote=True opts to it.

20210328IST1722

Some Indexes added to the mix. Or one could always look at index funds in the worst case.

MaShaMinT added to ProcDataEx RollingRet meta data/label.

20210331IST0404

MaBeta added as a additional procedb function.

202104XYISTABCD

the logic has been divided into few classes and modules, and the program flow is build
around this now.

20210414ISTABCD_HappyVishuUgadhiRelease

The default path used by program has been changed.

Date handling as been partly simplified and also now based on python datetime.

Add string based duration notation of ?W/?M/?Y

crazy module added, but not imported by default.


