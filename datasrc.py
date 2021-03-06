# A class to help work with Data Sources
# HanishKVC, 2021

import os
import sys
import time
import calendar
import enum
import datetime
import hlpr
import todayfile


# The Enums used to identify the type of data source
# It could be a Stocks related data source or
# It could be a Mutual funds related data source
# or Either of them.
DSType = enum.Enum('DSType', 'Any MF Stock')

# Enable this to ensure that weekends and holidays
# are skipped from the entities db.
# This is disabled currently, as some MFs have data
# during weekends.
gbSkipSkippedDateInEntDBAlso = False


class DataSrc:
    """
    Work with data available from a remote server, in a efficient manner.
    Provides logics to
        Fetch and save a local pickled copy of the fetched data.
        Load the pickled data into Entities DB. In turn if a local copy
            is not available, then fetch from remote server.
        Load a predefined list of holidays, if available. These dates
            will be skipped when fetching and or loading.

    Expects the following templates to be defined by the child class for creating
    suitable url and local file path (including file name).
    These are passed through strftime, so user can use suitable format specifiers
    to generate strings with required date components in it.
    urlTmpl: template used to create the url to fetch.
    pathTmpl: template used to create local filename for saving fetched data.

    dataKeys: should be a list of data keys.

    The child class should also provide valid implementation of the following
    functions.

        _valid_remotefile
        _parse_file
        _valid_picklefile [This is optional]
    """

    urlTmpl = None
    pathTmpl = None
    holiTmpl = "{}.holidays"
    urlFTypesTmpl = None
    pathFTypesTmpl = "types/{}.ftypes.{}"
    listFTypes = None
    dataKeys = None
    tag = "DSBase"
    bSkipWeekEnds = False
    bSkipToday = True
    earliestDate = 0


    def _load_holidays(self, fPath):
        """
        Load the holidays list associated with this data source.
        Each line should contain one holiday date in YYYYMMDD format.
        Skip lines that start with #
        Skip lines that are empty
        """
        self.holidays = set()
        try:
            f = open(fPath)
            for l in f:
                l = l.strip()
                if (l == '') or (l[0] == '#'):
                    continue
                i = int(l)
                self.holidays.add(i)
            f.close()
            print("INFO:{}:Loaded holiday list {}".format(self.tag, fPath))
        except:
            print("INFO:{}:No holiday list".format(self.tag))


    def _prefix_path(self, basePath, thePath, theMsg=""):
        if (thePath != None) and (basePath != None):
            thePath = os.path.expanduser(os.path.join(basePath, thePath))
        print("INFO:{}:{}:{}".format(self.tag, theMsg, thePath))
        return thePath


    def __init__(self, basePath, loadFilters, nameCleanupMap):
        """
        Initialise a data source instance.

        basePath: is used to setup the local file path, wrt fetched files.
        loadFilters: a DataSrc can define/suggest its own set of loadFilters.
            The same can be selected by end user, if they want to, by copying
            the same as the 'active' set, before calling the load function.
            DataSrc base class saves the root loadFilters dictionary.
            The child classes can define the loadFilters they prefer.
        nameCleanupMap: will be used to cleanup the entity names.
        NOTE: Ideally called after child class as setup tag,
            but before setting dataSrcType.
        """
        self.dataSrcType = DSType.Any
        self.loadFilters = loadFilters
        self.nameCleanupMap = nameCleanupMap
        self.pathTmpl = self._prefix_path(basePath, self.pathTmpl, "pathTmpl")
        self.pathFTypesTmpl = self.pathFTypesTmpl.format(self.tag, '{}')
        self.pathFTypesTmpl = self._prefix_path(basePath, self.pathFTypesTmpl, "pathFTypesTmpl")
        self.holiTmpl = self.holiTmpl.format(self.tag)
        self.holiTmpl = self._prefix_path(basePath, self.holiTmpl, "holiTmpl")
        self.listNoDataDates = []
        self._load_holidays(self.holiTmpl)


    def _valid_remotefile(self, fName):
        """
        Check if the given file is a valid data file containing fetched data from
        the remote server or not.
        NOTE: Child classes need to provide a valid implementation of this.
        """
        return False


    def valid_remotefile(self, fName):
        """
        Check if the given file is a valid data file containing fetched data from
        the remove server on not.
        This traps exceptions if any by the provided _valid_remotefile.
        NOTE: Dont override this in the child class, override _valid_remotefile.
        """
        try:
            bOk = self._valid_remotefile(fName)
        except:
            print("ERRR:{}:ValidRemoteFile:{}".format(self.tag, sys.exc_info()))
            bOk = False
        return bOk


    def remove_if_invalid(self, fName, errMsg):
        """
        Check if passed file is valid or not by calling valid_remotefile.
        In case if its invalid, remove the file.
        """
        if not self.valid_remotefile(fName):
            print(errMsg)
            try:
                os.remove(fName)
            except FileNotFoundError:
                pass


    def _fetch_remote(self, url, fName):
        """
        Fetch give url to specified file, and check its valid.
        """
        print(url, fName)
        hlpr.wget_better(url, fName)
        errMsg = "ERRR:{}:_FetchRemote:[{}] not a valid file, removing it".format(self.tag, fName)
        self.remove_if_invalid(fName, errMsg)


    def _parse_file(self, fName, today):
        """
        Parse the given file and load its data into the today dictionary.
        NOTE: Child classes need to provide a valid implementation of this.
        """
        today['bUpToDate'] = False
        return today


    def _valid_picklefile(self, fName):
        """
        Verify that the passed local file is a pickle file containing potentially
        valid data in it.
        NOTE: Child classes can optionally provide a better implementation of this.
        Returns: bValid(OrNot), bUpToDate, todayDict
        """
        if hlpr.pickle_ok(fName, 64):
            bOk, today, temp = hlpr.load_pickle(fName)
            if bOk:
                bMarker, bUpToDate = todayfile.valid_today(today)
                if bMarker:
                    return True, bUpToDate, today
        return False, False, None


    def _valid_date(self, theDate):
        """
        Check if given date is valid for given data source.
        The checks include:
            If its a week end and bSkipWeekEnds is Enabled,
            If its earlier than earliestDate specified.
            If its in data sources holiday list.
        """
        if self.bSkipWeekEnds and (theDate.isoweekday() > 5):
            return False
        dateInt = hlpr.dateint(theDate.year, theDate.month, theDate.day)
        if dateInt < self.earliestDate:
            return False
        if dateInt in self.holidays:
            return False
        if self.bSkipToday and (theDate == datetime.date.today()):
            return False
        return True


    def fetch4date(self, theDate, opts):
        """
        Fetch data for the given date.

        opts: a list of options supported by this logic
            'ForceLocal': When the logic decides that it has to fetch
                data file from the internet, it will cross check, if
                ForceLocal is True. If True, then the logic wont try
                to redownload
            'ForceRemote': If true, then the logic will try to fetch
                the data file again from the internet, irrespective
                of the local data pickle file is ok or not.
            NOTE: ForceRemote takes precedence over ForceLocal.
        """
        if not self._valid_date(theDate):
            return
        dateInt = hlpr.dateint(theDate.year, theDate.month, theDate.day)
        url = time.strftime(self.urlTmpl, theDate.timetuple())
        fName = time.strftime(self.pathTmpl, theDate.timetuple())
        bParseFile=False
        if opts == None:
            opts = {}
        bForceRemote = opts.get('ForceRemote', False)
        bForceLocal = opts.get('ForceLocal', False)
        if bForceRemote:
            self._fetch_remote(url, fName)
            bParseFile=True
        elif not self._valid_picklefile(fName)[0]:
            if not bForceLocal:
                self._fetch_remote(url, fName)
            else:
                errMsg = "ERRR:{}:Fetch4Date:{}:Available remote file not valid".format(self.tag, fName)
                self.remove_if_invalid(fName, errMsg)
            bParseFile=True
        if bParseFile:
            try:
                today = todayfile.init(dateInt, self.dataKeys)
                self._parse_file(fName, today)
                hlpr.save_pickle(fName, today, [], "{}:Fetch4Date".format(self.tag))
            except:
                print("ERRR:{}:Fetch4Date:{}:ForceRemote[{}], ForceLocal[{}]".format(self.tag, fName, bForceRemote, bForceLocal))
                print(sys.exc_info())


    def load4date(self, theDate, entDB, opts):
        """
        Load data for the given date into Entities DB.

        NOTE: If loading pickled data fails, then it will try to load
        the data corresponding to given date, from the locally downloaded
        data file if possible, else it will try to fetch it freshly from
        the internet/remote server.

        NOTE: This logic wont fill in missing data wrt holidays,
        you will have to call fillin4holidays explicitly.
        """
        dateInt = hlpr.dateint(theDate.year, theDate.month, theDate.day)
        if not self._valid_date(theDate):
            if gbSkipSkippedDateInEntDBAlso:
                entDB.skip_date(dateInt)
            return
        fName = time.strftime(self.pathTmpl, theDate.timetuple())
        ok = False
        for i in range(3):
            ok, bUpToDate, today = self._valid_picklefile(fName)
            if ok:
                break
            print("WARN:{}:Load4Date:Try={}: No valid data pickle found for {}".format(self.tag, i, fName))
            if i > 0:
                optsFD = { 'ForceRemote': True }
                if opts.get('LoadLocalOnly'):
                    break
            else:
                optsFD = { 'ForceLocal': True }
            self.fetch4date(theDate, optsFD)
        if ok:
            todayfile.load2edb(today, entDB, self.loadFilters, self.nameCleanupMap, 'active', self.tag)
        else:
            self.listNoDataDates.append(dateInt)
            print("WARN:{}:Load4Date:No data wrt {}, so skipping".format(self.tag, fName))


    def _ftype_fname(self, theFName):
        """
        The default FileName for a given FixedType.

        NOTE: THis is used by default _fetch_ftype, so a child class implementing
        _load_ftype can call this to get the local filename corresponding to ftype,
        i.e if it has not overridden _fetch_ftype.
        """
        return self.pathFTypesTmpl.format(theFName)


    def _fetch_ftype(self, theName, theFName, opts):
        """
        Fetch a given fixed type data file from a remote server.

        NOTE: If a given data source requires to handle url template
        or fetching in a different way, then it can override this function.
        """
        url = self.urlFTypesTmpl.format(theFName)
        fName = self._ftype_fname(theFName)
        print(url, fName)
        hlpr.wget_better(url, fName)


    def fetch_ftypes(self, opts=None):
        """
        Fetch Fixed/Rarely changing MxN Grouping/Types if any.
        """
        if self.listFTypes == None:
            print("INFO:{}:FetchFTypes: No FTypes to fetch".format(self.tag))
            return
        for tName,tFName in self.listFTypes:
            try:
                self._fetch_ftype(tName, tFName, opts)
            except:
                print("ERRR:{}:FetchFTypes: Failed fetching {}".format(self.tag, tName))


    def _load_ftype(self, theName, theFName, entDB, opts):
        """
        Load the specified FType.
        NOTE: Child class needs to implement this.
        """
        raise NotImplementedError


    def load_ftypes(self, entDB, opts=None):
        """
        Load the Fixed MxN Grouping/Types if any.
        """
        if self.listFTypes == None:
            print("INFO:{}:LoadFTypes: No FTypes to load".format(self.tag))
            return
        for tName, tFName in self.listFTypes:
            try:
                self._load_ftype(tName, tFName, entDB, opts)
            except:
                print("ERRR:{}:LoadFTypes: Failed loading {}".format(self.tag, tName))


