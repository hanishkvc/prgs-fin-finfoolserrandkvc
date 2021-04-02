# A class to help work with Data Sources
# HanishKVC, 2021

import os
import calendar
import sys
import datetime
import hlpr
import enttypes


class DataSource:


    pathTmpl = None
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


    def _valid_remotefile(fName):
        """
        Check if the given file is a valid data file or not.
        NOTE: Child classes need to provide a valid implementation of this.
        """
        return False


    def _fetch_remote(url, fName):
        """
        Fetch give url to specified file, and check its valid.
        """
        print(url, fName)
        hlpr.wget_better(url, fName)
        if not self._valid_remotefile(fName):
            print("ERRR:{}:_FetchRemote:[{}] not a valid file, removing it".format(self.tag, fName))
            os.remove(fName)


fetchPreDateErrCnt = 0
def fetch4date(y, m, d, opts):
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
    global fetchPreDateErrCnt
    if (y < 2006) or ((y == 2006) and (m < 4)):
        if (fetchPreDateErrCnt % 20) == 0:
            print("WARN:IndiaMF:fetch4date:AMFI data starts from 200604, so skipping for {:04}{:02}...".format(y,m))
        fetchPreDateErrCnt += 1
        return
    url = MFS_BaseURL.format(d,calendar.month_name[m][:3],y)
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    bParseCSV=False
    if opts == None:
        opts = {}
    bForceRemote = opts.get('ForceRemote', False)
    bForceLocal = opts.get('ForceLocal', False)
    if bForceRemote:
        _fetchdata(url, fName)
        bParseCSV=True
    elif not hlpr.pickle_ok(fName,4e3):
        if not bForceLocal:
            _fetchdata(url, fName)
        bParseCSV=True
    if bParseCSV:
        try:
            today = parse_csv(fName)
            hlpr.save_pickle(fName, today, [], "IndiaMF:fetch4Date")
        except:
            print("ERRR:IndiaMF:fetch4date:{}:ForceRemote[{}], ForceLocal[{}]".format(fName, bForceRemote, bForceLocal))
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



