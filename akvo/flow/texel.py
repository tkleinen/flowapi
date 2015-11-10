# -*- coding: utf-8 -*-
'''
Created on Oct 20, 2015

@author: theo
'''
KEY=r'B3+dye342B4HpLuJ+Ked3GCyjjTVA5/4fv1ZT0SEKi0='
SECRET=r'fBA5XsXAtSXzbOs4acpg5l2S+ptPPZhiKKzMSwYTfqQ='
INSTANCE=r'http://acacia.akvoflow.org/'

SURVEYS = {
'00. EC meting' : 7030916,
'01. EC Update' : 3030924,
}

REGISTRATION_FORM = 7030916

import api

def report(instance):
    print instance['keyId']
    return True # continue downloading

# initialize an akvo.flow.api instance 
flowAPI = api.Instance(key=KEY,secret=SECRET,instance=INSTANCE)

# get the registration form instances
regs = flowAPI.get_registration_instances(REGISTRATION_FORM)

# define what additional fields from the registration forms are needed in the export
fields = {'Identifier': 'surveyedLocaleIdentifier', 'Location': 'surveyedLocaleDisplayName'}    

# export all surveys
for name,surveyId in SURVEYS.items():
    print name

    # create new csv file with survey id as name
    with open('{name}.csv'. format(name=surveyId),'w') as destination:
        # export all data for this survey to the csv file and print the instance ids along the way
        flowAPI.to_csv(surveyId, destination, callback=report, reginfo={'instances': regs, 'fields': fields})
                    
