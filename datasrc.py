# A class to help work with Data Sources
# HanishKVC, 2021

import os
import sys
import time
import calendar
import hlpr
import todayfile


class DataSrc:
    """
    Work with data available from a remote server, in a efficient manner.
    Provides logics to
        Fetch and save a local pickled copy of the fetched data.
        Load the pickled data into Entities DB. In turn if a local copy
            is not available, then fetch from remote server.

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
    dataKeys = None
    tag = "DSBase"
    bSkipWeekEnds = False


    def __init__(self, basePath, loadFilters, nameCleanupMap):
        """
        Initialise a data source instance.

        basePath: is used to setup the local file path, wrt fetched files.
        loadFilters: a DataSrc can define/suggest its own set of load filters.
            The same can be selected by end user, if they want to, by copying
            the same as the 'active' set.
        nameCleanupMap: will be used to cleanup the entity names.
        """
        self.loadFilters = loadFilters
        self.nameCleanupMap = nameCleanupMap
        if (self.pathTmpl != None) and (basePath != None):
            self.pathTmpl = os.path.expanduser(os.path.join(basePath, self.pathTmpl))
        print("INFO:{}:pathTmpl:{}".format(self.tag, self.pathTmpl))


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
            os.remove(fName)


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


    def fetch4date(self, y, m, d, opts):
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
        if self.bSkipWeekEnds and (calendar.weekday(y,m,d) > 4):
            return
        timeTuple = (y, m, d, 0, 0, 0, 0, 0, 0)
        dateInt = hlpr.dateint(y,m,d)
        url = time.strftime(self.urlTmpl, timeTuple)
        fName = time.strftime(self.pathTmpl, timeTuple)
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


    def load4date(self, y, m, d, edb, opts):
        """
        Load data for the given date into Entities DB.

        NOTE: If loading pickled data fails, then it will try to load
        the data corresponding to given date, from the locally downloaded
        data file if possible, else it will try to fetch it freshly from
        the internet/remote server.

        NOTE: This logic wont fill in missing data wrt holidays,
        you will have to call fillin4holidays explicitly.
        """
        if self.bSkipWeekEnds and (calendar.weekday(y,m,d) > 4):
            return
        timeTuple = (y, m, d, 0, 0, 0, 0, 0, 0)
        fName = time.strftime(self.pathTmpl, timeTuple)
        ok = False
        for i in range(3):
            ok, bUpToDate, today = self._valid_picklefile(fName)
            if ok:
                break
            print("WARN:{}:Load4Date:Try={}: No valid data pickle found for {}".format(self.tag, i, fName))
            if i > 0:
                opts = { 'ForceRemote': True }
            else:
                opts = { 'ForceLocal': True }
            self.fetch4date(y, m, d, opts)
        if ok:
            todayfile.load2edb(today, edb, self.loadFilters, self.nameCleanupMap, 'active', self.tag)
        else:
            print("WARN:{}:Load4Date:No data wrt {}, so skipping".format(self.tag, fName))



