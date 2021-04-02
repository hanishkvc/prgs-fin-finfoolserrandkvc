# Today File module
# HanishKVC, 2021


import hlpr


TODAY_MARKER = "TODAYFILEKVC_V1"


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
        'data': []
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


def valid_today(today):
    """
    Check passed today dictionary contains a valid marker.
    Also give the UpToDate status stored in it.
    """
    bMarker = (today['marker'] == TODAY_MARKER)
    return bMarker, today['bUpToDate']


def load2edb(today, edb, loadFilters, nameCleanupMap, caller="TodayFile"):
    """
    Load data in today dictionary into the given entities db (edb).

    Apply the filters if any wrt entType or entName. Inturn the
    filtered data which passes the check will only be loaded into edb.

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
    TOTHINK: Should I maintain entDate within today['data'] for each ent.
        Can give finer entity level info has to data is uptodate or not.
        But as currently I am not using it, so ignoring for now.
    """
    # Handle entTypes and their entities
    for curEntType in today['entTypes']:
        entCodes = today['entTypes'][curEntType]
        curEntTypeId = edb.add_type(curEntType)
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
            edb.add_data(entCode, datas, name, curEntType)


