# Work with loadfilters
# HanishKVC, 2021
#

"""
LoadFilters: Each loadFilters is a named set of match templates which is used to
filter entity types as well as entity names.
User or Modules can define loadFilters, which inturn can be used as required later.
"""

def setup(dLoadFilters, loadFiltersName, whiteListEntTypes=None, whiteListEntNames=None, blackListEntNames=None):
    """
    Add a new loadFilters into the dLoadFilters dictionary
    """
    if dLoadFilters == None:
        return
    dLoadFilters[loadFiltersName] = {
        'whiteListEntTypes': whiteListEntTypes,
        'whiteListEntNames': whiteListEntNames,
        'blackListEntNames': blackListEntNames
        }


def get(dLoadFilters, loadFiltersName):
    """
    Retreive the specified loadFilters.
    If there is no valid loadFilters of that name,
    then a empty loadFilters is returned.
    """
    if dLoadFilters == None:
        loadFilters = None
    else:
        loadFilters = dLoadFilters.get(loadFiltersName, None)
    if loadFilters == None:
        loadFilters = { 'whiteListEntTypes': None, 'whiteListEntNames': None, 'blackListEntNames': None }
    return loadFilters


def list(dLoadFilters, caller="Main"):
    """
    List the currently defined loadFilters.
    """
    print("INFO:{}:LoadFilters".format(caller))
    for lfName in dLoadFilters:
        print("    {}".format(lfName))
        for t in dLoadFilters[lfName]:
            print("        {} : {}".format(t, dLoadFilters[lfName][t]))


def activate(dLoadFilters, loadFiltersName=None):
    """
    Set the specified loadFilters as the active loadFilters.
    If None is passed, then active loadFilters, if any is cleared.
    """
    if loadFiltersName != None:
        group = dLoadFilters[loadFiltersName]
    else:
        group = None
    dLoadFilters['active'] = group


