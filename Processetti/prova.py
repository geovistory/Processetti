import json
import pprint
from collections import Counter
from operator import itemgetter
import csv
from dotmap import DotMap
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import openpyxl
import geopandas as gpd



# string cleaning

def clean_up_str(string):

    a = ''
    if string:
        a = string.replace('\xa0', '').replace('  ', ' ').strip()
    
    return a  


# string cleanup and reduction to lower case

def str_normalization(string):

    if type(string) == str and (string != ''):
        string = string.strip().replace('\xa0', '').replace('  ', ' ').lower()
    
    return string


#introduction of np.nan values

def nan_compiler(df):

        return df.replace('', np.nan, inplace=True)



# year calculation (average)

def date(r_col):

    lista=[]
    for v in r_col['values']:
        if v.value.timePrimitive:
            lista.append(int(v.value.timePrimitive.label[:4]))
            
    if len(lista)>=1:
        return int(np.rint(np.mean(lista)))
    else:
        return np.nan


# time-spen definition (average year: beginning, at a certain time, end)

def datatio(r_col_begin, r_col_someTime, r_col_end, element):

    begin, some_time, end = date(r_col_begin), date(r_col_someTime), date(r_col_end)
    element+= [begin, some_time, end]

    return(element)








### dataframe generators - produce df from specific Geovistory queries



# processetti dataframe generator

def processetti_generator(file):
    
    file = file
    with open(file, encoding='utf-8') as json_file:
        data_processetti = json.load(json_file)
    dm_processetti = DotMap(data_processetti)
    dm_procesetti_rows = dm_processetti.rows
    ### Processetti list = psl
    psl=[['volume', 'section', 'number_archival_units', 'pk_processetto', 'enquiry','pk_person', 'motivation_type',
        'start_date','end_date', 'anno', 'id_union', 'number_union']]
    
    for r in dm_procesetti_rows: # r for row
        element = []
        # volume
        element += ['-'.join([v.entity_label for v in r.col_0.entities]) if len(r.col_0.entities)>=1 else np.nan]   
        # sezione (unità archivistica)
        element += [clean_up_str(r.col_1.entities[0].entity_label)]
        # numero di section per enquiry 
        element += [len(r.col_1.entities)]
        # enquiry 
        element += [r.col_2.entity.pk_entity, clean_up_str(r.col_2.entity.entity_label)]
        # pk_person     ## Integrazione (08/08/2022) -- garantirà un miglior allineamento 
        element += [r.col_6.entities[0].pk_entity if r.col_6.entities else np.nan]
        # motivation type
        element += [clean_up_str('; '.join([v.entity_label for v in r.col_3.entities])) if len(r.col_3.entities)>=1 else np.nan]
        # arco e anno processetto
        valor = []
        if len(r.col_4['values']) == 0:
            element += [np.nan, np.nan, np.nan]
        else:
            for v in  r.col_4['values']:
                valor.append(int(v.value.timePrimitive.label[:4]))
            element += [r.col_4['values'][0]['value'].timePrimitive.label[:10], r.col_4[
                'values'][len(r.col_4['values'])-1]['value'].timePrimitive.label[:10], round(np.mean(valor))]
        # union
        if len(r.col_5.entities)==1:
            element += [r.col_5.entities[0].pk_entity, len(r.col_5.entities)]
        elif len(r.col_5.entities)>1:
            element += ['-'.join([str(v.pk_entity) for v in r.col_5.entities]) if len(r.col_5.entities)>=1 else np.nan, len(r.col_5.entities)]  
        else:
            element += [np.nan, 0]

        psl.append(element)

    return pd.DataFrame(psl[1:], columns=psl[0])  


# persons dataframe generator - according to role or quality (applicant, witness, defunct)

def persons_generator(file, quality=''):
    file = file
    quality = quality
    with open(file, encoding='utf-8') as json_file:
        data = json.load(json_file)

    dm = DotMap(data)
    dm_r = dm.rows  

    if ('tes' or 'wit') in quality:
        quality='testimone'
    elif ('ric' or 'req' or 'app' or 'inv') in quality:
        quality='richiedente'
    elif ('def' or 'dec') in quality:
        quality='defunto'
    

    if (quality == 'testimone') or (quality == 'richiedente'):
        # Richiedenti list = rl
        lista = [['pk_processetto',  'pk_person', 'person', 'qualità', 'genere', 'origini', 'professioni', 'anno_nascita']]

        for r in dm_r:
            for el in r.col_0.entities:
                element = []
                element += [el.pk_entity]
                # pk_person, person, qualità, genere
                element += [r.col_1.entity.pk_entity, clean_up_str(r.col_1.entity.entity_label), quality, (
                    r.col_2.entities[0].entity_label if len(r.col_2.entities)>0 else np.nan)]
                # origini
                element += [clean_up_str('; '.join(e.entity_label for e in r.col_3.entities)) if len(r.col_3.entities)>=1 else np.nan]
                # professioni
                element += ['; '.join(e.entity_label for e in r.col_5.entities) if len(r.col_5.entities)>=1 else np.nan]
                # Anno di nascita 
                element += [date(r.col_4)]                
                lista.append(element)

    elif quality == 'defunto':
        lista = [['pk_person', 'person', 'qualità', 'genere', 'origini', 'professioni', 'anno_nascita', 'anno_morte', 'luogo_morte', 'id_union', 'union_count', 'type_union', 'pk_person(partner)', 'partner', 'pk_processetto']]
        for r in dm_r:
            element=[]
            # pk_person, person, qualità, genere, origini, professioni
            element += [r.col_0.entity.pk_entity, clean_up_str(r.col_0.entity.entity_label), quality, (r.col_1.entities[0].entity_label if len(r.col_1.entities)>0 else np.nan), (
                ('; '.join(e.entity_label for e in r.col_2.entities)) if len(r.col_2.entities)>=1 else np.nan), ('; '.join(e.entity_label for e in r.col_4.entities) if len(r.col_4.entities)>=1 else np.nan)]
            # anno_nascita
            element += [date(r.col_3)]
            # anno_morte, luogo_morte
            element += [date(r.col_6), ('; '.join(e.entity_label for e in r.col_7.entities) if len(r.col_7.entities)>=1 else np.nan)]
            # union, conteggio_unioni, tipo unione
            element += ['-'.join([str(v.pk_entity) for v in r.col_8.entities]) if len(r.col_8.entities)>=1 else np.nan, len(r.col_8.entities), (
                '; '.join(e.entity_label for e in r.col_9.entities) if len(r.col_9.entities)>=1 else np.nan)]
            # pk_person(partner), partner
            ## riscrivi
            id_part, part = [], []
            for e in r.col_10.entities:
                if e.pk_entity != r.col_0.entity.pk_entity:
                    id_part.append(e.pk_entity)
                    part.append(clean_up_str(e.entity_label))
            element += [('-'.join(str(e) for e in id_part) if len(id_part)>=1 else np.nan), ('; '.join(part) if len(part)>=1 else np.nan)]  
            # pk_processetto
            element += ['-'.join([str(v.pk_entity) for v in r.col_11.entities]) if len(r.col_11.entities)>=1 else np.nan]
            lista.append(element)  

    return  pd.DataFrame(lista[1:], columns=lista[0])


# person name dataframe generator 

def persons_name_generator(file):

    file = file 
    with open(file, encoding='utf-8') as json_file:
        data_name = json.load(json_file)
    dmperson_name = DotMap(data_name)
    dmperson_name_rows = dmperson_name.rows

    pnl = [['pk_person', 'person', 'cognome', 'cognome_count', 'titolo_rispetto', 'titolo_profesisonale']]

    for r in dmperson_name_rows:
        element = []
        element += [r.col_0.entity.pk_entity, r.col_0.entity.entity_label, clean_up_str('; '.join(c.entity_label for c in r.col_1.entities)), len(r.col_1.entities) , str_normalization('; '.join(tr.entity_label for tr in r.col_2.entities)),
        str_normalization('; '.join(tp.entity_label for tp in r.col_3.entities))]
        pnl.append(element)

    return pd.DataFrame(pnl[1:], columns=pnl[0])


# localization dataframe generator

def localisation_generator(file):
    file = file
    with open(file, encoding='utf-8') as json_file:
        data_localisation = json.load(json_file)
    dmlocalisation = DotMap(data_localisation)
    dmlocalisation_rows = dmlocalisation.rows

    # Localisation list = ll
    ll = [['id_localisation', 'pk_person', 'person', 'geographical_place', 'GP_type', 'lat', 'lon', 'localisation_type', 'begin', 'at_some_time', 'end', 'occours_before', 'occours_after']]
    for r in dmlocalisation_rows:
        element = []
        # id_localisatio, pk_person, person, geo_place + (id), 
        ## valuta la comodità di tenere separato l'ID geographical Place, geographical place, type, coordinate (x,y), localization type 
        element += [r.col_0.entity.pk_entity, r.col_1.entities[0].pk_entity, clean_up_str(r.col_1.entities[0].entity_label), (r.col_2.entities[0].entity_label + f' ({ r.col_2.entities[0].pk_entity})')  if len(
            r.col_2.entities)>0 else np.nan, r.col_10.entities[0].entity_label if len(r.col_10.entities)>=1 else np.nan]
        if len(r.col_12['values'])>0:    
            element += [r.col_12['values'][0].value.geometry.geoJSON.coordinates[0], r.col_12['values'][0].value.geometry.geoJSON.coordinates[1]]
        else:
            element +=[np.nan, np.nan]
        element += [r.col_11.entities[0].entity_label if len(r.col_11.entities)>0 else np.nan]
        # Begin
        element += [round(np.mean([int(r.col_3['values'][0].value.timePrimitive.label[:4]), int(r.col_4['values'][0].value.timePrimitive.label[:4])])) if (len(r.col_3['values'])>0) and (len(r.col_4['values'])>0) else int(
            r.col_3['values'][0].value.timePrimitive.label[:4]) if (len(r.col_3['values'])>0) else int(r.col_4['values'][0].value.timePrimitive.label[:4]) if len(r.col_4['values'])>0 else np.nan]
        # At some time within
        element += [int(r.col_5['values'][0].value.timePrimitive.label[:4]) if len(r.col_5['values'])>0 else np.nan]
        # End
        element += [round(np.mean([int(r.col_6['values'][0].value.timePrimitive.label[:4]), int(r.col_7['values'][0].value.timePrimitive.label[:4])])) if (len(r.col_6['values'])>0) and (len(r.col_7['values'])>0) else int(
            r.col_6['values'][0].value.timePrimitive.label[:4]) if (len(r.col_6['values'])>0) else int(r.col_7['values'][0].value.timePrimitive.label[:4]) if len(r.col_7['values'])>0 else np.nan]    
        # Occours before
        element += ['-'.join(str(v.pk_entity) for v in r.col_8.entities) if len(r.col_8.entities)>0 else np.nan]
        # Occours after
        element += ['-'.join(str(v.pk_entity) for v in r.col_9.entities) if len(r.col_9.entities)>0 else np.nan]
        
        ll.append(element)

    return pd.DataFrame(ll[1:], columns=ll[0])





def activityDomains_generator(file):
    file = file
    with open(file, encoding='utf-8') as json_file:
        data_domain = json.load(json_file)

    dmd = DotMap(data_domain)
    dmd_r = dmd.rows

    # act_domain list = adl
    adl = [['id_occupation', 'occupation', 'broder_occupation', 'activity_domain', 'broder_activity']]
    for row in dmd_r:
        element = []
        element += [row.col_0.entity.pk_entity, row.col_0.entity.entity_label]
        if len(row.col_1.entities)>=1:
            ocupations=[]
            for e in row.col_1.entities:
                ocupations +=[e.entity_label]
            element += [ocupations]
        else:
            element += [np.nan]
        if row.col_2.entities:
            activity=[]
            for e in row.col_2.entities:
                activity += [e.entity_label]
            element += [activity]
        else:
            element += [np.nan]
        if row.col_3.entities:
            activity_bt=[]
            for e in row.col_3.entities:
                activity_bt += [clean_up_str(e.entity_label)]
            element += [activity_bt]
        else:
            element += [np.nan]
        
        adl.append(element)
    

    return pd.DataFrame(adl[1:], columns=adl[0])  