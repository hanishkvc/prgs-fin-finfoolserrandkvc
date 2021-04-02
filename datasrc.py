# A class to help work with Data Sources
# HanishKVC, 2021

import os
import calendar
import sys
import datetime
import hlpr
import enttypes
import todayfile


class DataSource:
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


    def __init__(self, basePath, loadFilters, nameCleanupMap):
        """
        Initialise a data source instance.
        """
        self.loadFilters = loadFilters
        self.nameCleanupMap = nameCleanupMap
        if (pathTmpl != None) and (basePath != None):
            self.pathTmpl = os.path.expanduser(os.path.join(basePath, pathTmpl))
        print("INFO:{}:pathTmpl:{}".format(self.tag, self.pathTmpl))


    def _valid_remotefile(self, fName):
        """
        Check if the given file is a valid data file containing fetched data from
        the remote server or not.
        NOTE: Child classes need to provide a valid implementation of this.
        """
        return False


    def _fetch_remote(self, url, fName):
        """
        Fetch give url to specified file, and check its valid.
        """
        print(url, fName)
        hlpr.wget_better(url, fName)
        if not self._valid_remotefile(fName):
            print("ERRR:{}:_FetchRemote:[{}] not a valid file, removing it".format(self.tag, fName))
            os.remove(fName)


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
        TOTHINK: Should I also check for bUpToDate and return false, if its false.
            This could potentially force a attempt at fetching data from remote
            server again.
        """
        if hlpr.pickle_ok(fName, 64):
            bOk, today, temp = hlpr.load_pickle(fName)
            if bOk:
                bMarker, bUpToDate = todayfile.valid_today(today)
                if bMarker:
                    return True
        return False


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
            _fetch_remote(url, fName)
            bParseFile=True
        elif not self._valid_picklefile(fName):
            if not bForceLocal:
                _fetch_remote(url, fName)
            bParseFile=True
        if bParseFile:
            try:
                today = todayfile.init(dateInt, self.dataKeys)
                self._parse_file(fName, today)
                hlpr.save_pickle(fName, today, [], "{}:Fetch4Date".format(self.tag))
            except:
                print("ERRR:{}:Fetch4Date:{}:ForceRemote[{}], ForceLocal[{}]".format(self.tag, fName, bForceRemote, bForceLocal))
                print(sys.exc_info())


def load4date(y, m, d, opts):
    """
    Load data for the given date.

    NOTE: If loading pickled data fails, then it will try to load
    the data corresponding to given date, from the locally downloaded
    csv file if possible, else it will try to fetch it freshly from
    the internet/remote server.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    ok = False
    for i in range(3):
        ok,today,tIgnore = hlpr.load_pickle(fName)
        if ok:
            break
        else:
            print("WARN:IndiaMF:load4date:Try={}: No data pickle found for {}".format(i, fName))
            if i > 0:
                opts = { 'ForceRemote': True }
            else:
                opts = { 'ForceLocal': True }
            fetch4date(y, m, d, opts)
    if ok:
        _loaddata(today)
    else:
        print("WARN:IndiaMF:load4date:No data wrt {}, so skipping".format(fName))



