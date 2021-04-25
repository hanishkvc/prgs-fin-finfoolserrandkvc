# Today File module
# HanishKVC, 2021


import hlpr
import loadfilters


TODAY_MARKER = "TODAYFILEKVC_V96"


def init(date, dataKeys):
    """
    Initialise a today dictionary.
    """
    today = {
        'marker': TODAY_MARKER,
        'date': date,
        'bUpToDate': True,
        'entTypes': {},
        'dataKeys': dataKeys,
        'codeD': {},
        'data': [],
        'more': {}
        }
    return today


def add_enttype(today, typeName):
    """
    Add the index corresponding to the specified typeName, wrt today dictionary.
    NOTE: If the passed typeName is not already present, then it will be added.
    """
    lMembers = today['entTypes'].get(typeName, None)
    if lMembers == None:
        lMembers = []
        today['entTypes'][typeName] = lMembers
    return lMembers


def add_ent(today, entCode, entName, entValues, entType, date):
    """
    Add given entity and its entityType to today.
    It also updates the bUpToDate flag in today.
    """
    typeMembers = add_enttype(today, entType)
    typeMembers.append(entCode)
    entIndex = len(today['data'])
    today['data'].append([entCode, entName, entValues])
    today['codeD'][entCode] = entIndex
    #if today['date'] > date:
    if today['date'] != date:
        today['bUpToDate'] = False


def add_morecat(today, cat, catType=list):
    """
    Create a category in more dictionary to store any additional meta/related data
    wrt entities or given date or so.
    The category could either be a list or dictionary of items.
    """
    if cat in today['more']:
        return
    if catType == list:
        today['more'][cat] = []
    else:
        today['more'][cat] = {}


def add_morecat_data(today, cat, data, key=None):
    """
    Add data to the more-category.
    If key is None: the underlying category is assumed to be a list.
    """
    theCat = today['more'][cat]
    if key == None:
        theCat.append(data)
    else:
        theCat[key] = data


def valid_today(today):
    """
    Check passed today dictionary contains a valid marker.
    Also give the UpToDate status stored in it.
    """
    if 'marker' in today:
        bMarker = (today['marker'] == TODAY_MARKER)
        return bMarker, today['bUpToDate']
    else:
        return False, False


def load2edb(today, entDB, loadFilters=None, nameCleanupMap=None, filterName=None, caller="TodayFile"):
    """
    Load data in today dictionary into the given entities db (entDB).

    Apply the specified filters if any wrt entType or entName. Inturn the
    filtered data which passes the check will only be loaded into entDB.

    Name of entities will be cleaned up using nameCleanupMap, if any.

    today dictionary contains

        'marker': TODAY_MARKER
        'date': YYYYMMDD
        'bUpToDate': True/False
        'entTypes': {
            typeName1: [entCode1A, entCode1B, ...],
            typeName2: [entCode2A, entCode2B, ...],
            ....
            }
        'dataKeys': [ key1, key2, ...]
        'codeD': { ent1Code: ent1Index, ent2Code: ent2Index, ... }
        'data': [
            [ent1Code, ent1Name, [ent1Val1, ent1Val2, ...] ],
            [ent2Code, ent2Name, [ent2Val1, ent2Val2, ...] ],
            ...
            ]
        'more': {
            cat1: [ data(s)1, data(s)2, ... ]
            cat2: { key1: data1, key2: data2, ... }
            }
    TOTHINK: Should I maintain entDate within today['data'] for each ent.
        Can give finer entity level info has to data is uptodate or not.
        But as currently I am not using it, so ignoring for now.
    """
    loadFilters = loadfilters.get(filterName, loadFilters)
    # Handle entTypes and their entities
    for curEntType in today['entTypes']:
        entCodes = today['entTypes'][curEntType]
        curEntTypeId = entDB.add_type(curEntType)
        if loadFilters['whiteListEntTypes'] == None:
            bSkipCurType = False
        else:
            fm,pm = hlpr.matches_templates(curEntType, loadFilters['whiteListEntTypes'])
            if len(fm) == 0:
                bSkipCurType = True
            else:
                bSkipCurType = False
        if bSkipCurType:
            continue
        # Handle entities
        for entCode in entCodes:
            entIndex = today['codeD'][entCode]
            code, name, values = today['data'][entIndex]
            if nameCleanupMap != None:
                name = hlpr.string_cleanup(name, nameCleanupMap)
            if (entCode != code):
                input("DBUG:{}:_LoadData: Code[{}] NotMatchExpected [{}], skipping".format(caller, code, entCode))
                continue
            if (loadFilters['whiteListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, loadFilters['whiteListEntNames'])
                if len(fm) == 0:
                    #gMeta['skipped'].add(str([code, name]))
                    continue
            if (loadFilters['blackListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, loadFilters['blackListEntNames'])
                if len(fm) > 0:
                    #gMeta['skipped'].add(str([code, name]))
                    continue
            datas = {}
            for i in range(len(today['dataKeys'])):
                dataKey = today['dataKeys'][i]
                datas[dataKey] = values[i]
            entDB.add_data(entCode, datas, name, curEntTypeId)
    # Handle the more categories of data, which is blindly copied into entDB
    for cat in today['more']:
        theCat = today['more'][cat]
        catType = type(today['more'][cat])
        if cat == 'corpActD':
            for data in theCat:
                entDB.add_corpact(data[0], data[1], data[2], data[3], data[4])
        else:
            entDB.add_morecat(cat, catType)
            for data in theCat:
                if catType == list:
                    entDB.add_morecat_data(cat, data)
                else:
                    entDB.add_morecat_data(cat, theCat[data], data)


