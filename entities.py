#!/usr/bin/env python3
# Work with sequences of data belonging to related groups of entities
# HanishKVC, 2021
# GPL
#

import sys
import os
import numpy
import time
import traceback
import hlpr
import enttypes



class EntitiesDB:


    def _init_types(self):
        """
        Create the types db related members.
        typesD: Allows identify the typeIndex/typeId associated with given typeName
        typesL: Allows one to get the typeName from a given typeId
        """
        enttypes._init(self)


    def _init_dates(self, dateCnt):
        """
        Create things related to dates.
        """
        self.nxtDateIndex = 0
        self.dates = numpy.zeros(dateCnt)
        self.datesD = {}


    def _set_aliases(self, aliases=None):
        if aliases == None:
            aliases = self.aliases
        if aliases == None:
            return
        for key in aliases:
            for alias in aliases[key]:
                self.data[alias] = self.data[key]


    def _init_ents(self, dataKeys, aliases, entCnt, dateCnt):
        """
        Create the members required to handle the data(s) related to the entities.
        """
        self.nxtEntIndex = 0
        self.data = {}
        self.meta = {}
        for dataKey in dataKeys:
            self.data[dataKey] = numpy.zeros([entCnt, dateCnt])
        self._set_aliases(aliases)
        self.meta['name'] = numpy.empty(entCnt, dtype=object)
        self.meta['codeL'] = numpy.empty(entCnt, dtype=object)
        self.meta['codeD'] = {}
        self.meta['typeId'] = numpy.empty(entCnt, dtype=object)
        self.meta['firstSeen'] = numpy.zeros(entCnt)
        self.meta['lastSeen'] = numpy.zeros(entCnt)


    def __init__(self, dataKeys, aliases, entCnt, dateCnt):
        """
        Initialise a entities object.

        dataKeys: specifies a list of dataKeys' that will be used
        to store different kinds of data associated with each entity
        in this entities db.
        aliases: A dictionary of aliases, which map some of the
            dataKeys' to aliases. For each dataKey that needs aliases
            defined for it, should have a entry in the dictioanry,
            with the required aliases in a list as the value for
            that key.
        entCnt: The number of entities one expects to store in this.
        dateCnt: The number of dates for which we expect to store
        data in this entities db.
        """
        if type(dataKeys) != list:
            dataKeys = [ dataKeys ]
        self.dataKeys = dataKeys
        self.aliases = aliases
        self._init_types()
        self._init_dates(dateCnt)
        self._init_ents(dataKeys, aliases, entCnt, dateCnt)


    def add_type(self, typeName):
        """
        Add a type (typeName) into types db, if not already present.
        Return the typeId.
        """
        return enttypes._add(self, typeName)


    def add_type_member(self, entTypeId, entCode):
        """
        Add a entity to the members list associated with its entityType.
        """
        enttypes._add_member(self, entTypeId, entCode)


    def list_types(self, entTypeTmpls=enttypes.TYPE_MATCH_ALL):
        """
        List all matching entTypes.
        """
        return enttypes.list(entTypeTmpls, self)


    def list_type_members(self, entTypeTmpls=enttypes.TYPE_MATCH_ALL, entNameTmpls=enttypes.NAME_MATCH_ALL):
        """
        List matching members in all matching entTypes.

        entNameTmpls: If [], matchs all members within each matched entType.
        """
        return enttypes._members(self, entTypeTmpls, entNameTmpls)


    def add_date(self, dateInt):
        """
        It is assumed that the date is added in chronological sequence.
        dateInt: should be in YYYYMMDD format.
        """
        self.dates[self.nxtDateIndex] = dateInt
        self.datesD[dateInt] = self.nxtDateIndex
        self.lastAddedDate = dateInt
        self.nxtDateIndex += 1


    def daterange2index(self, startDate, endDate):
        """
        Get the indexes corresponding to the start and end date

        If either of the date is -1, then it will be mapped to
        either the beginning or end of the current valid dataset,
        as appropriate. i.e start maps to 0, end maps to curDateIndex.
        """
        if startDate == -1:
            startDateIndex = 0
        else:
            startDateIndex = self.datesD[startDate]
        if endDate == -1:
            endDateIndex = self.nxtDateIndex-1
        else:
            endDateIndex = self.datesD[endDate]
        return startDateIndex, endDateIndex


    def add_ent(self, entCode, entName, entTypeId):
        """
        Add a entity to the entities db, if required, and return its index.

        NOTE: entName and entTypeId required only if a new entity is being added,
              else if one is trying to get the entIndex, these can be ignored.
              However if name is passed, for a entity already in DB, the same
              will be updated in the DB.
        """
        entIndex = self.meta['codeD'].get(entCode, -1)
        if entIndex == -1:
            entIndex = self.nxtEntIndex
            self.nxtEntIndex += 1
            self.meta['name'][entIndex] = entName
            self.meta['codeL'][entIndex] = entCode
            self.meta['codeD'][entCode] = entIndex
            self.meta['typeId'][entIndex] = entTypeId
            self.add_type_member(entTypeId, entCode)
        else:
            if entName != None:
                self.meta['name'][entIndex] = entName
        return entIndex


    def get_entindex(self, entCode, entName=None, entTypeId=None):
        """
        Get the index associated with a given entity code,
        adding the entity into the entities db, if required.

        NOTE: entName and entTypeId required only if a new entity
              is being added, indirectly using this logic.
        If entName is passed, then it will be checked against what
        is already there in entities db, and a warning generated
        if there is  mismatch.
        """
        entIndex = self.add_ent(entCode, entName, entTypeId)
        if entName != None:
            nameInRecord = self.meta['name'][entIndex]
            if nameInRecord != entName:
                print("WARN:Entities:GetEntIndex:NameCheck: Existing [{}] != Passed [{}]".format(nameInRecord, entName))
        return entIndex


    def add_data(self, entCode, entData, entName=None, entTypeId=None):
        """
        Add one or more data beloning to a entity.
        entCode: Specifies a unique code associated with the entity
        entData: is either a single value or a dictionary of dataValues
            with their dataKeys.
            If its a single value, then its assumed to correspond to
            the first data key specified during init.
        entName: is the name of the entity whose data is being added.
        entTypeId: the typeId to which this entity belongs.

        NOTE: entName and entTypeId required only if a new entity is being added,
              else if one is trying to get the entIndex, these are ignored.
        """
        if type(entData) != dict:
            entData = { self.dataKeys[0]: entData }
        entIndex = self.get_entindex(entCode, entName, entTypeId)
        if self.nxtDateIndex == 0:
            input("DBUG:Entities:AddData: Trying to add entity data, before date is specified")
            return
        if self.meta['firstSeen'][entIndex] == 0:
            #self.meta['firstSeen'][entIndex] = self.dates[self.nxtDateIndex-1]
            self.meta['firstSeen'][entIndex] = self.lastAddedDate
        self.meta['lastSeen'][entIndex] = self.lastAddedDate
        for dataKey in entData:
            self.data[dataKey][entIndex,self.nxtDateIndex-1] = entData[dataKey]


    def optimise_size(self, dataKeys):
        """
        Reduce the arrays used to fit the currently loaded set of data.
        """
        for dataKey in dataKeys:
            self.data[dataKey] = self.data[dataKey][:self.nxtEntIndex,:self.nxtDateIndex]
        self._set_aliases()
        for key in [ 'firstSeen', 'lastSeen', 'name', 'codeL', 'typeId' ]:
            self.meta[key] = self.meta[key][:self.nxtEntIndex]
        self.dates = self.dates[:self.nxtDateIndex]


