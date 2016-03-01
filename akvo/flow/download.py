# -*- coding: utf-8 -*-
'''
Created on Sep 29, 2015

@author: theo
'''
import api
import json
import sys

def report(instance):
    sys.stdout.write('.') 
    sys.stdout.flush()
    return True # continue downloading

try:
    with open('flowapi.json') as fp:
        config = json.load(fp)
except:
    print u'Erreur lors de chargement de données de configuration à partir du fichier flowapi.json'
    sys.exit()

# initialize an akvo.flow.api instance for DHA Mauritanie 
flowAPI = api.Instance(key=config['KEY'],secret=config['SECRET'],instance=config['INSTANCE'])

print u'Téléchargement en cours...'

# get the registration form instances (villages)
villages = flowAPI.get_registration_instances(config['REGISTRATION_FORM'])

# define what additional fields from the registration forms are needed in the export
fields = {'Identifier': 'surveyedLocaleIdentifier', 'Localisation': 'surveyedLocaleDisplayName'}    

# export all surveys
for surveyId in config['SURVEYS']:
    print
    survey = flowAPI.get_survey(surveyId)
    print surveyId

    # create new csv file with survey id as name
    with open('{name}.csv'. format(name=surveyId),'w') as destination:
        # export all data for this survey to the csv file and print the instance ids along the way
        flowAPI.to_csv(surveyId, destination, callback=report, reginfo={'instances': villages, 'fields': fields})

print u'Téléchargement terminé'
