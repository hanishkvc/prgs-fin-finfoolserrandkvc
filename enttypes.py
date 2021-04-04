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
    for k in self.typesD:
        print(k)


def _add_member(self, entTypeId, entCode):
    """
    Add a entity to the members list associated with its entityType.
    """
    self.typeMembers[self.typesL[entTypeId]].append(entCode)


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
                print("\t", entCode, entName)
                entCodes.append(entCode)
    return entCodes


