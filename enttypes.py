#!/usr/bin/env python3
# Handle entity types as part of FinFoolsErrand
# HanishKVC, 2021


gMeta = None
def init(theMeta, bClearFields=True):
    """
    Initialise the entity types module wrt the currently active meta dictionary.

    bClearField: Also clear the entity type related fields in the given Meta dictionary.

    NOTE: THe passed meta dictionary will be used implicitly if required in future operations,
    if the user doesnt pass any new meta dictionary explicitly.
    """
    global gMeta
    if bClearFields:
        theMeta['entTypes'] = {}
        theMeta['entType4Id'] = []
        theMeta['entType2Id'] = {}
    gMeta = theMeta


def _themeta(theMeta):
    if theMeta == None:
        return gMeta
    else:
        return theMeta


def add(entType, theCaller="?", theMeta=None):
    """
    Add the given entType into meta dictionary, if required.

    Also return the entTypeId for the given entType.
    """
    theMeta = _themeta(theMeta)
    if entType not in theMeta['entTypes']:
        theMeta['entTypes'][entType] = []
        checkTypeId = theMeta['entType2Id'].get(entType, -1)
        if checkTypeId != -1:
            input("DBUG:EntTypes-{}:add: entType2Id not in sync with entTypes".format(theCaller))
        else:
            theMeta['entType4Id'].append(entType)
        theMeta['entType2Id'][entType] = len(theMeta['entType4Id'])-1
    return theMeta['entType2Id'][entType]


def list(theMeta=None):
    """
    List entityTypes found in currently loaded data.

    theMeta: the meta dictionary to use to get the details of entTypes available.
        However if None, then meta dictionary which was initialised the last time,
        will be used automatically.
    """
    theMeta = _themeta(theMeta)
    for k in theMeta['entTypes']:
        print(k)


def members(entType, theMeta=None):
    """
    List the members of the specified entityType
    """
    theMeta = _themeta(theMeta)
    print("INFO:EntTypes: [{}] members:".format(entType))
    for m in gMeta['entTypes'][entType]:
        print(m, gMeta['names'][gMeta['code2index'][m]])


