# Handle entity types related members of Entities class.
# HanishKVC, 2021
# GPL

import hlpr


def _init(self):
    """
    Initialise the entity types related members of passed self.
    """
    self.nxtTypeIndex = 0
    self.typesD = {}
    self.typesL = []
    self.typeMembers = {}


def _add(self, typeName):
    """
    Add the given typeName into entities db, if required.

    Also return the typeId associated with the given typeName.
    """
    if typeName not in self.typesD:
        self.typesD[typeName]=self.nxtTypeIndex
        self.typesL.append(typeName)
        self.typeMembers[typeName] = []
        self.nxtTypeIndex += 1
    return self.typesD[typeName]


def _list(self):
    """
    List entity types found in the passed entities db.
    """
    return self.typesL


def _add_member(self, entTypeId, entCode):
    """
    Add a entity to the members list associated with its entityType.
    """
    entType = self.typesL[entTypeId]
    if entCode not in self.typeMembers[entType]:
        self.typeMembers[entType].append(entCode)


def _members(self, entTypeTmpls, entNameTmpls=[]):
    """
    List the members of the specified entity types

    entTypeTmpls could either be a string or a list of strings.
    Each of these strings will be treated as a matching template
    to help identify the entity types one should get the members
    for.

    The entities belonging to the selected entTypes will be filtered
    through the entNameTmpls. If entNameTmpls is empty, then all
    members of the selected entTypes will be selected.
    """
    if type(entTypeTmpls) == str:
        entTypeTmpls = [ entTypeTmpls ]
    entTypesList = []
    for entType in self.typesD:
        fm,pm = hlpr.matches_templates(entType, entTypeTmpls)
        if len(fm) > 0:
            entTypesList.append(entType)
    entCodes = []
    for entType in entTypesList:
        print("INFO:EntType: [{}] members:".format(entType))
        for entCode in self.typeMembers[entType]:
            entIndex = self.meta['codeD'][entCode]
            entName = self.meta['name'][entIndex]
            if len(entNameTmpls) == 0:
                bEntSelect = True
            else:
                fm,pm = hlpr.matches_templates(entName, entNameTmpls)
                if len(fm) > 0:
                    bEntSelect = True
                else:
                    bEntSelect = False
            if bEntSelect:
                print("\t{:20} {}".format(entCode, entName))
                entCodes.append(entCode)
    return entCodes



#
# Functions which can be used by end user
#

#
# The user can either explicitly specify the entDB to use with each of these functions
# ORELSE the function will fall back to using the entDB that was explicitly initialised
# before by the logic.
#
# NOTE: In these user functions, the entDB is the last argument, which is optional.
#       While in internal _functions, entDB is the compulsory 1st argument.
#


gEntDB = None
def _entDB(entDB):
    """
    Get a possibly valid/useful entDB
    """
    if entDB == None:
        return gEntDB
    return entDB


def list(entDB=None):
    """
    List all the types in entDB
    """
    entDB = _entDB(entDB)
    lTypes = _list(entDB)
    for t in lTypes:
        print(t)
    return lTypes


def members(entTypeTmpls, entNameTmpls=[], entDB=None):
    """
    List the members of the specified entity types

    entTypeTmpls could either be a string or a list of strings.
    Each of these strings will be treated as a matching template
    to help identify the entity types one should get the members
    for.

    The entities belonging to the selected entTypes will be filtered
    through the entNameTmpls. If entNameTmpls is empty, then all
    members of the selected entTypes will be selected.
    """
    entDB = _entDB(entDB)
    return _members(entDB, entTypeTmpls, entNameTmpls)


