# Work with loadfilters
# HanishKVC, 2021
#

"""
LoadFilters: Each loadFilters is a named set of match templates which is used to
filter entity types as well as entity names.
User or Modules can define loadFilters, which inturn can be used as required later.
"""


#
# The default global dictionary to store the different loadFilters.
#
gLoadFilters = { }
def _dLoadFilters(dLoadFilters=None):
    """
    If a custom dictionary of loadFilters is not specified,
    then the default global dictionary will be used.
    """
    if dLoadFilters == None:
        dLoadFilters = gLoadFilters
    return dLoadFilters


def setup(loadFiltersName, whiteListEntTypes=None, whiteListEntNames=None, blackListEntNames=None, dLoadFilters=None):
    """
    Add a new named loadFilters into the dLoadFilters dictionary.
    Same can be used later, if required by logics like load_data,...
    """
    dLoadFilters = _dLoadFilters(dLoadFilters)
    dLoadFilters[loadFiltersName] = {
        'whiteListEntTypes': whiteListEntTypes,
        'whiteListEntNames': whiteListEntNames,
        'blackListEntNames': blackListEntNames
        }


def copy(fromLF, toLF, dLoadFilters=None):
    """
    Make a copy of the fromLF loadFilters into a loadFilters named toLF.
    """
    dLoadFilters = _dLoadFilters(dLoadFilters)
    dLoadFilters[toLF] = dLoadFilters[fromLF].copy()


def get(loadFiltersName, dLoadFilters=None):
    """
    Retreive the specified loadFilters.
    If there is no valid loadFilters of that name,
    then a empty loadFilters is returned.
    """
    dLoadFilters = _dLoadFilters(dLoadFilters)
    loadFilters = dLoadFilters.get(loadFiltersName, None)
    if loadFilters == None:
        loadFilters = { 'whiteListEntTypes': None, 'whiteListEntNames': None, 'blackListEntNames': None }
    return loadFilters


def list(caller="Main", dLoadFilters=None):
    """
    List the currently defined loadFilters.
    """
    dLoadFilters = _dLoadFilters(dLoadFilters)
    print("INFO:{}:LoadFilters".format(caller))
    for lfName in dLoadFilters:
        print("    {}".format(lfName))
        for t in dLoadFilters[lfName]:
            print("        {} : {}".format(t, dLoadFilters[lfName][t]))


def activate(loadFiltersName=None, dLoadFilters=None):
    """
    Set the specified loadFilters as the active loadFilters.
    If None is passed, then active loadFilters, if any is cleared.
    """
    dLoadFilters = _dLoadFilters(dLoadFilters)
    if loadFiltersName != None:
        group = dLoadFilters[loadFiltersName]
    else:
        group = None
    dLoadFilters['active'] = group


