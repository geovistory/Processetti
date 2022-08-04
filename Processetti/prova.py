
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



# pulizzia delle stringhe

def clean_up_str(string):
    a = ''
    if string:
        a = string.replace('\xa0', '').replace('  ', ' ').strip()
    return a  


# Funzione di elminazione delle dupplicazioni in leggenda
##  https://stackoverflow.com/questions/19385639/duplicate-items-in-legend-in-matplotlib

def legend_without_duplicate_labels(ax):
    handles, labels = ax.get_legend_handles_labels()
    unique = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l not in labels[:i]]
    ax.legend(*zip(*unique))


# calcolo dell'anno (medio)

def date(r_col):

    lista=[]
    for v in r_col['values']:
        if v.value.timePrimitive:
            lista.append(int(v.value.timePrimitive.label[:4]))
            
    if len(lista)>=1:
        return int(np.rint(np.mean(lista)))
    else:
        return np.nan


# sintesi time-spen (anno medio: begin, at some time, end)

def datatio(r_col_begin, r_col_someTime, r_col_end, element):

    begin, some_time, end = date(r_col_begin), date(r_col_someTime), date(r_col_end)
    element+= [begin, some_time, end]

    return(element)


def person_ds(file, testo):
    file = file
    testo = testo
    with open(file, encoding='utf-8') as json_file:
        data_ric = json.load(json_file)

    dmr = DotMap(data_ric)

    dmr_r = dmr.rows  

    # Richiedenti list = rl

    rl = [['pk_processetto',  'pk_person', 'person', 'qualità', 'genere', 'origini', 'anno_nascita', 'professioni' ]]


    for r in dmr_r:
        for el in r.col_0.entities:
            element = []
            element += [el.pk_entity]

            # pk_person, person, qualità, genere
            element += [r.col_1.entity.pk_entity, clean_up_str(r.col_1.entity.entity_label), testo, (
                r.col_2.entities[0].entity_label if len(r.col_2.entities)>0 else np.nan)]
            
            # origini
            element += [clean_up_str('; '.join(e.entity_label for e in r.col_3.entities)) if len(r.col_3.entities)>=1 else np.nan]

            # Anno di nascita 
            element += [date(r.col_4)]
            
            # professioni
            element += ['; '.join(e.entity_label for e in r.col_5.entities) if len(r.col_5.entities)>=1 else np.nan]
        
            
            rl.append(element)


    return  pd.DataFrame(rl[1:], columns=rl[0]) 