import requests
import xml.etree.ElementTree as ET
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime

# paises = ['CHL']

paises = ['CHL',
        'ITA',
        'ESP',
        'COL',
        'DEU',
        'ARG']

ghos = ['Number of deaths',
        'Number of infant deaths',
        'Number of under-five deaths',
        'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)',
        'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)',
        'Estimates of number of homicides',
        'Estimates of rates of homicides per 100 000 population',
        'Crude suicide rates (per 100 000 population)',
        'Mortality rate attributed to unintentional poisoning (per 100 000 population)',
        'Number of deaths attributed to non-communicable diseases, by type of disease and sex',
        'Estimated road traffic death rate (per 100 000 population)',
        'Estimated number of road traffic deaths',
        'Mean BMI (kg/m&#xb2;) (crude estimate)',
        'Mean BMI (kg/m&#xb2;) (age-standardized estimate)',
        'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)',
        'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)',
        'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)',
        'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)',
        'Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)',
        'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)',
        'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)',
        'Estimate of daily cigarette smoking prevalence (%)',
        'Estimate of daily tobacco smoking prevalence (%)',
        'Estimate of current cigarette smoking prevalence (%)',
        'Estimate of current tobacco smoking prevalence (%)',
        'Mean systolic blood pressure (crude estimate)',
        'Mean fasting blood glucose (mmol/l) (crude estimate)',
        'Mean Total Cholesterol (crude estimate)']

hijos = ['GHO',
        'COUNTRY',
        'SEX',
        'YEAR',
        'GHECAUSES',
        'AGEGROUP',
        'Display',
        'Numeric',
        'Low',
        'High']

dic_facts = {'GHO':[],
            'COUNTRY':[],
            'SEX':[],
            'YEAR':[],
            'GHECAUSES':[],
            'AGEGROUP':[],
            'Display':[],
            'Numeric':[],
            'Low':[],
            'High':[],
            'Alcohol':[],
            'Tabaco':[],
            'Cigarrillos':[],
            'Presion':[],
            'Glucosa':[],
            'Colesterol':[]}

for pais in paises:
    r = requests.get(f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{pais}.xml')
    b = r.content
    root = ET.fromstring(b)

    for fact in root:
        gho = fact.find('GHO').text
        if gho in ghos:
            for hijo1 in hijos:
                estaba = False
                for hijo in fact:
                    if hijo.tag == hijo1:
                        estaba=True
                        if hijo.text:
                            if hijo.tag == 'High' or hijo.tag == 'Low':
                                dic_facts[hijo.tag].append(float(hijo.text))
                            elif hijo.tag == 'Numeric':
                                dic_facts[hijo.tag].append(float(hijo.text))
                            elif hijo.tag == 'YEAR':
                                date = datetime(year=int(hijo.text), month=1, day=1)
                                dic_facts[hijo.tag].append(date)
                            else:
                                dic_facts[hijo.tag].append(hijo.text)
                        else:
                            if hijo.tag == 'SEX':
                                dic_facts[hijo.tag].append('Both sexes')
                            else:
                                dic_facts[hijo.tag].append(None)
                if not estaba:
                    dic_facts[hijo1].append(None)

lista_gho = dic_facts['GHO']
lista_numeric = dic_facts['Numeric']
lista_a = []
lista_t = []
lista_ci = []
lista_p = []
lista_g = []
lista_co = []

for i in range(len(dic_facts['GHO'])):
    if lista_gho[i] == 'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)':
        lista_a.append(lista_numeric[i])
        lista_t.append(None)
        lista_ci.append(None)
        lista_p.append(None)
        lista_g.append(None)
        lista_co.append(None)
    elif lista_gho[i] == 'Estimate of daily tobacco smoking prevalence (%)':
        lista_t.append(lista_numeric[i])
        lista_a.append(None)
        lista_ci.append(None)
        lista_p.append(None)
        lista_g.append(None)
        lista_co.append(None)
    elif lista_gho[i] == 'Estimate of daily cigarette smoking prevalence (%)':
        lista_ci.append(lista_numeric[i])
        lista_a.append(None)
        lista_t.append(None)
        lista_p.append(None)
        lista_g.append(None)
        lista_co.append(None)
    elif lista_gho[i] == 'Mean systolic blood pressure (crude estimate)':
        lista_p.append(lista_numeric[i])
        lista_a.append(None)
        lista_t.append(None)
        lista_ci.append(None)
        lista_g.append(None)
        lista_co.append(None)
    elif lista_gho[i] == 'Mean fasting blood glucose (mmol/l) (crude estimate)':
        lista_g.append(lista_numeric[i])
        lista_a.append(None)
        lista_t.append(None)
        lista_ci.append(None)
        lista_p.append(None)
        lista_co.append(None)
    elif lista_gho[i] == 'Mean Total Cholesterol (crude estimate)':
        lista_co.append(lista_numeric[i])
        lista_a.append(None)
        lista_t.append(None)
        lista_ci.append(None)
        lista_p.append(None)
        lista_g.append(None)
    else:
        lista_a.append(None)
        lista_t.append(None)
        lista_ci.append(None)
        lista_p.append(None)
        lista_g.append(None)
        lista_co.append(None)

dic_facts['Alcohol'] = lista_a
dic_facts['Tabaco'] = lista_t
dic_facts['Cigarrillos'] = lista_ci
dic_facts['Presion'] = lista_p
dic_facts['Glucosa'] = lista_g
dic_facts['Colesterol'] = lista_co

for i in dic_facts.values():
    print(len(i))

df = pd.DataFrame(data=dic_facts)

gc = gspread.service_account(filename='taller-tarea-4-316220-f878f09f3b11.json')
sh = gc.open_by_key('1BRCfZQ6GDf63nWYSXuti2bNbXmOncykxe2XMnwwkdF8')
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet,df)
