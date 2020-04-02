#start session
import findspark
findspark.init('/opt/cloudera/parcels/SPARK2/lib/spark2')
#from pyspark import SparkContext
#from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import SparkSession
from timeit import default_timer as timer
#from modules import claims_sql
from pyspark.sql.functions import col, round

import sys
import os
import pprint
pp = pprint.PrettyPrinter(indent=4)
#DW Netezza Login Information
import getpass
import json
from datetime import date, datetime
import pandas as pd
pd.set_option('display.max_columns', None,'display.max_columns', None)

import requests
import zipfile
import xml.etree.ElementTree

import argparse
import glob

import requests
import urllib.request
import time
from bs4 import BeautifulSoup

def init_parser():
    # initiate the parser
    parser = argparse.ArgumentParser()
    # add long and short argument
    parser.add_argument("lanid", help="set lan id for EDW")
    # read arguments from the command line
    args = parser.parse_args()
    return args


def getLatestQuarantine_doc():
    months = ['january','february','march','april','may','june',\
          'july','august','september','october','november','december']


    url = 'https://www.mass.gov/info-details/covid-19-cases-quarantine-and-monitoring'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Get date:
    getcurrdate = str(soup.findAll(class_='ma__table--responsive__wrapper')[0].find('tr'))\
                    .replace('<th>COVID-19 cases in Massachusetts','')\
                    .replace('<tr>\xa0as of ','').replace('</th>\n<th>\xa0</th>\n</tr>','')
    get_dt_str = getcurrdate.replace(',','').split(' ')
    mth = get_dt_str[0].lower().strip(' ')
    mth_str = str(months.index(mth)+1)
    day_str = get_dt_str[1]
    if len(day_str) == 1:
        day_str = '0'+day_str
    curr_date = mth_str+'-'+day_str
    print(getcurrdate,get_dt_str,curr_date)

    # Get quarantine:
    data = soup.findAll(class_='ma__table--responsive__wrapper')[1].find_all('tr')
    #print(data)
    total = int(str(data[1].find_all('td')[0]).replace('<td>','').replace('</td>',''))
    completed = int(str(data[2].find_all('td')[0]).replace('<td>','').replace('</td>',''))
    current = int(str(data[3].find_all('td')[0]).replace('<td>','').replace('</td>',''))
    print(curr_date,total,completed,current)
    # output current quarantine data
    cols = ['Day','Total_Quarantined','Completed_Quarantine','Current_Quarantine']
    df = pd.DataFrame([[curr_date,total,completed,current]],columns=cols)
    df_existing = pd.read_csv(os.getcwd()+'/analysis/input/covid_19_quarantine_mass_gov.csv')
    #print(df_existing)
    df_existing = df_existing[df_existing['Day'] != curr_date]
    df = df_existing.append(df)
    df.to_csv(os.getcwd()+'/analysis/output/covid_19_quarantine_mass_gov.csv')
    print(df)

    # Get Latest Doc
    url = ''
    data_d = soup.findAll('a')
    print(data_d)
    for row in data_d:
        #if row.text =='Doc' and row['href'].find('/doc/covid-19-cases') != -1:
        if row.text =='DOC' or (row.text =='Doc' and row['href'].find('/doc/covid-19-cases') != -1):
            url = 'https://www.mass.gov/'+row['href']
            print(url)

    file = os.getcwd()+'/analysis/input/covid-19-case-report-'+curr_date+'-2020.docx'
    print('Downloading file from massgov.com to -->',file)
    opener = urllib.request.URLopener()
    opener.addheader('User-Agent', 'whatever')
    filename, headers = opener.retrieve(url,file)

    return curr_date

def usa_facts_data():
    # Usafacts website
    url = 'https://usafacts.org/visualizations/coronavirus-covid-19-spread-map/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    conf_url = ''
    death_url = ''
    data_d = soup.findAll('a')
    for row in data_d:
        #if row.text =='Doc' and row['href'].find('/doc/covid-19-cases') != -1:
        if row.text =='Confirmed Cases':
            conf_url = row['href']
            print(conf_url)
            file = os.getcwd()+'/analysis/input/covid_confirmed_usafacts.csv'
            print('Downloading file from usafacts.org to -->',file)
            opener = urllib.request.URLopener()
            opener.addheader('User-Agent', 'whatever')
            filename, headers = opener.retrieve(conf_url,file)
        if row.text =='Deaths':
            death_url = row['href']
            print(death_url)
            file = os.getcwd()+'/analysis/input/covid_deaths_usafacts.csv'
            print('Downloading file from usafacts.org to -->',file)
            opener = urllib.request.URLopener()
            opener.addheader('User-Agent', 'whatever')
            filename, headers = opener.retrieve(death_url,file)

# from IPython.core.interactiveshell import InteractiveShell #display results setting
# InteractiveShell.ast_node_interactivity = "all"
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'

def read_daily_covid_data(file_x):
    data_str = ''
    #file_x = os.getcwd()+'/analysis/input/covid-19-case-report-'+dt+'-2020.docx'
    print(file_x)
    with zipfile.ZipFile(file_x) as docx:
        tree = xml.etree.ElementTree.XML(docx.read('word/document.xml'))
    count_tables = 0
    for table in tree.iter(TABLE):
        count_tables = count_tables +1
        #print(count_tables,'--->',table)
        for row in table.iter(ROW):
            for cell in row.iter(CELL):
                data_str = data_str + ' '.join(node.text for node in cell.iter(TEXT)) +'\n'
                #print(data_str)
                #print(type(data_str))

    return count_tables,data_str

def get_county(filedata,dates):
    data_dict_dt = {}
    for day in dates:
        data_dict_dt[day] = {}
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']

        #print('County --->' , curr.split('County')[1].split('Sex')[0])
        p1 = curr.split('County')

        county_raw = [ m.replace(' ','') for m in p1[1].split('Sex')[0].split('\n') if m != '']
        counties = [i[1] for i in enumerate(county_raw) if i[0]%2 == 0]
        counties_data = [i[1] for i in enumerate(county_raw) if i[0]%2 != 0]
        county = dict(list(zip(counties,counties_data)))
        data_dict_dt[day]['county'] = county

    pp.pprint(data_dict_dt)
    #print(list(data_dict_dt[list(data_dict_dt.keys())[-1]]['county'].keys()))
    #County data
    county_dict = {}
    counties = list(data_dict_dt[list(data_dict_dt.keys())[-1]]['county'].keys())
    print(counties)

    for m in counties:
        if m.find('Dukes') != -1:
            m1 = 'Dukes'
        else:
            m1=m
        county_dict[m1] = {}
        for day,data_y in data_dict_dt.items():
            try:
                county_dict[m1][day] = int(data_y['county'][m])
            except:
                county_dict[m1][day] = 0
    time_data = pd.DataFrame(county_dict)
    #time_data['Total'] = time_data.sum()
    time_data.loc[:,'Total'] = time_data.sum(numeric_only=True, axis=1)

    counties_dukes = [m if m.find('Dukes') == -1 else 'Dukes' for m in counties ]
    counties_dukes_Transposed = time_data.loc[:,counties_dukes].T
    #path = '''//bos0105dc01/EDS-Dfs/Enterprise_Analytics/Analytics Pods/Member Analytics Pod/COVID19/Wave5_Predictive Heat Map and Listening Post/4_Data/Data_ADT/'
    #counties_dukes_Transposed.to_csv('county_lvl_covid_19_mass_gov_time_series.csv')

    county_data = []
    for day,data_y in data_dict_dt.items():
        curr = []
        for value in counties:
            if value.find('Dukes') != -1:
                m1 = 'Dukes'
            else:
                m1=value
            try:
                curr.append([day,m1,int(data_y['county'][value])])
            except:
                curr.append([day,m1,0])
        county_data = county_data + curr
    #print(county_data)
    df_county = pd.DataFrame(county_data,columns=['Day','County','Confirmed_Cases'])
    print(df_county)
    df_county.to_csv(os.getcwd()+'/analysis/output/county_lvl_covid_19_mass_gov_time_series.csv')
    #print(counties_dukes_Transposed)
    return df_county

def get_testing(filedata,dates):
    data = []
    data_dict_dt = {}
    for day in dates:
        data_dict_dt[day] = {}
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        #print(tbl_cnt)
        p1 =[]
        print('==================================================')
        print(day,'---->',tbl_cnt)
        #print(curr.split('Hospitalization')[1])
        #print('County --->' , curr.split('County')[1].split('Sex')[0])
        if tbl_cnt == 3:
            #print(curr)
            #p1 = curr.split('Hospitalization')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[1]
            #if curr.find('Reported Deaths') == -1:
            p1 = curr.split('Hospitalization')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[1]
            if curr.find('Reported Deaths') != -1:
                p1 = 'MAStatePublicHealthLaboratory'+p1
            #print('\t@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

            p1 = p1.replace('*','')
            p1 = p1.replace('TotalPositive\nTotalTested\nPatients','MAStatePublicHealthLaboratory\n')
            #p1 = p1.replace('\nTotalPositive*\nTotalTested*\nPatients\n','MAStatePublicHealthLaboratory\n')
            #p1 = p1.replace('Laboratory\nTotalPositive\nTotalTested**\n','')
            p1 = p1.replace('Laboratory\nTotalPositive\nTotalTested\n','')
            # else:
            #     #p1 = curr.split('Hospitalization')[1].replace(' ','').split('ReportedDeaths')[1]
            #     #p1 = curr.split('ReportedDeaths')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[1]
            #     p1 = curr.split('Hospitalization')[1].replace(' ','').split('ReportedDeaths')[1]
            #     print('\t^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
            #     p1 = p1.replace('*','')
            #     print(p1)

                #p1 = 'MAStatePublicHealthLaboratory'+p1

            p1 = p1.split('\n')
            p1 = [m for m in p1 if m != '']
            print(len(p1),p1)
            #print(p1)
        elif tbl_cnt == 2:
            if day not in ('3-09','3-10','3-11','3-12','3-13','3-14','3-15'):
                #print(p1)
                p1 = curr.split('Hospitalization')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[1]
                #print(p1)
                p1 = 'MAStatePublicHealthLaboratory'+p1
                p1 = p1.split('\n')
                p1 = [m.replace('*','') if m != '*' else '0' for m in p1 ] #p1.replace('*','')
                #print(p1)
                #print('**************')
                #print(p1)
        else:
            print(day,'***---->',tbl_cnt)
            #print(curr.split('Reported Deaths')[1])
            p1 = curr.split('Reported Deaths')[1].replace(' ','').split('MAStatePublicHealthLaboratory')
            if len(p1) > 1:
                p1 =p1[2]
            else:
                p1=p1[1]
            #print('**************************************')
            #print(curr.split('Reported Deaths')[1].replace(' ',''))
            #print('-------------------------------------')
            p1 = p1.replace('*','')
            p1 = p1.replace('TotalPositive\nTotalTested\nPatients','MAStatePublicHealthLaboratory\n')
            #p1 = p1.replace('\nTotalPositive*\nTotalTested*\nPatients\n','MAStatePublicHealthLaboratory\n')
            #p1 = p1.replace('Laboratory\nTotalPositive\nTotalTested**\n','')
            p1 = p1.replace('Laboratory\nTotalPositive\nTotalTested\n','')
            p1 = 'MAStatePublicHealthLaboratory\n'+p1
            p1 = p1.split('\n')
            p1 = [m for m in p1 if m != '']
            #print(p1)

        if len(p1) > 0:
            lab =[m.replace(' ','') for m in p1 if m != '']
            #print(len(lab),lab)
            lab = [[day,i[1],lab[i[0]+1].replace(' ',''),lab[i[0]+2].replace(' ','')] for i in enumerate(lab) if i[0]%3 == 0]
            data_dict_dt[day]['testing'] = lab
        else:
            data_dict_dt[day]['testing'] = []
        #data_dict_dt[day]['testing'] = p1
        data = data + data_dict_dt[day]['testing']
        #print(data_dict_dt[day])
    df_data = pd.DataFrame(data,columns=['Day','Laboratory','Tested_Positive','Total_Tested'])
    print(df_data)
    #pp.pprint(data_dict_dt)
    df_data.to_csv(os.getcwd()+'/analysis/output/covid_19_testing_mass_gov.csv')
    return df_data

def get_hospitalizations(filedata,dates):
    data = []
    data_dict_dt = {}
    for day in dates:
        data_dict_dt[day] = {}
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        #print(tbl_cnt)
        p1 =[]
        #print('County --->' , curr.split('County')[1].split('Sex')[0])
        if tbl_cnt ==3:
            print(curr.find('Reported Deaths') != -1)
            if curr.find('Reported Deaths') != -1:
                p1 = curr.split('Hospitalization')[1].replace(' ','').split('ReportedDeaths')[0]
            else:
                print('\t$$$$$$$$$$$$$$$$$$$$$$$$$$$$$--------------------')
                p1 = curr.split('Hospitalization')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[0]
                print(p1)
            p1 = p1.split('\n')
            print('\t$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
            #p1  = [m.replace(' ','') for m in p1 if m != '']
            print(day,p1)
        elif tbl_cnt ==2:
            p1 = curr.split('Hospitalization')[1].replace(' ','').split('MAStatePublicHealthLaboratory')[0]
            p1 = p1.split('Laboratory')[0]
            #p1 = 'MAStatePublicHealthLaboratory'+p1
            p1 = p1.split('\n')
            #print('**************')
            #print(p1)
        else:
            p1 = curr.split('Hospitalization')[1].replace(' ','').split('ReportedDeaths')[0]
            p1 = p1.split('Laboratory')[0]
            #p1 = 'MAStatePublicHealthLaboratory'+p1
            p1 = p1.split('\n')
            #print('**************')
            #print(p1)

        if len(p1) > 0:
            hosp1 =[m.replace(' ','') for m in p1 if m != '']
            #print(lab)
            hosp1 = [[day,hosp1[i[0]],hosp1[i[0]+1]] for i in enumerate(hosp1) if i[0]%2 == 0]
            data_dict_dt[day]['hospitalization'] = hosp1
        else:
            data_dict_dt[day]['hospitalization'] = []
        #data_dict_dt[day]['testing'] = p1
        data = data + data_dict_dt[day]['hospitalization']
        #print(data_dict_dt[day])
    df_data = pd.DataFrame(data,columns=['Day','Category','Confirmed_Cases'])
    print(df_data)
    #pp.pprint(data_dict_dt)
    df_data.to_csv(os.getcwd()+'/analysis/output/covid_19_hospitalizations_mass_gov.csv')
    return df_data

def get_gender(filedata,dates):
    data = []
    for day in dates:
        #data_dict_dt[day] = {}
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        p1 = curr.split('Sex')[1]
        if p1.find('Age Group') == -1:
            p1 = p1.split('Exposure')[0]
        else:
            p1 = p1.split('Age Group')[0]
        p1 = p1.split('\n')
        p1 = [m.replace(' ','') for m in p1 if m != '']
        p1 = [[day,p1[i[0]],p1[i[0]+1]] for i in enumerate(p1) if i[0]%2 == 0]
        data = data + p1

    df_data = pd.DataFrame(data,columns=['Day','Gender','Confirmed_Cases'])
    print(df_data)
    #pp.pprint(data_dict_dt)
    df_data.to_csv(os.getcwd()+'/analysis/output/covid_19_gender_mass_gov.csv')
    return df_data

def get_age(filedata,dates):
    data = []
    for day in dates:
        #data_dict_dt[day] = {}
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        p1 = []
        #print(curr)
        #print('------------------',curr.find('Age Group'))
        if curr.find('Age Group') != -1:
            if curr.split('Age Group')[1].find('Deaths') == -1:
                p1 = curr.split('Age Group')[1].split('Exposure')[0]
            else:
                p1 = curr.split('Age Group')[1].split('Exposure')[0].split('Deaths')[0]
            print(p1)
            p1 = p1.split('\n')
            p1 = [m.replace(' ','') for m in p1 if m != '']
            #print(p1)
            p1 = [[day,p1[i[0]],p1[i[0]+1]] for i in enumerate(p1) if i[0]%2 == 0]

        #print(day,p1)
        data = data + p1

    df_data = pd.DataFrame(data,columns=['Day','Age_Group','Confirmed_Cases'])
    print(df_data)
    df_data.to_csv(os.getcwd()+'/analysis/output/covid_19_age_mass_gov.csv')
    return df_data

def get_exposure(filedata,dates):
    data = []
    for day in dates:
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        p1 = curr.split('Exposure')[1].split('Hospitalization')[0].split('Deaths')[0]
        p1 = p1.split('\n')
        p1 = [m.replace('/','').replace('*','').replace(' ','') for m in p1 if m != '']
        #print(p1)
        p1 = [[day,p1[i[0]],p1[i[0]+1]] for i in enumerate(p1) if i[0]%2 == 0]
        #print(day,p1)
        data = data + p1

    df_data = pd.DataFrame(data,columns=['Day','Exposure','Confirmed_Cases'])
    print(df_data)
    df_data.to_csv(os.getcwd()+'/analysis/output/covid_19_exposure_mass_gov.csv')
    return df_data

def get_deaths(filedata,dates,county,hosp):
    data = []
    for day in dates:
        tbl_cnt = filedata[day]['tbl_cnt']
        curr = filedata[day]['data']
        #print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n')
        #print(curr)
        if curr.find('Deaths') != -1:
            p1 = curr.split('Deaths')[1].split('Hospitalization')[0]
            #print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n')
            #print(p1)
            # if p1.find('Reported Deaths') !=-1:
            #     p1 = p1.split('Reported Deaths')[0]
            #     print('\t^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
            #     print(p1)
            p1 = p1.split('\n')
            p1 = int([m.replace(' ','') for m in p1 if m != ''][1])
            data.append([day,p1])
    df_data = pd.DataFrame(data,columns=['Day','Deaths'])

    df_data['new_deaths'] =0
    df_data['new_deaths'] = df_data['Deaths'].diff()
    #df_data.fillna(0).to_csv(os.getcwd()+'/analysis/output/covid_19_deaths_mass_gov.csv')
    #print(df_data)

    # add county,hospitalization Data_ADT
    df_cases = county[['Day','Confirmed_Cases']].groupby(['Day']).sum().reset_index()
    df_cases['Confirmed_Cases'] = df_cases['Confirmed_Cases'].astype('int')
    df_cases['new_cases'] =0
    df_cases['new_cases'] = df_cases['Confirmed_Cases'].diff()
    df_cases = df_cases.fillna(0)

    df_hosp = hosp[['Day','Category','Confirmed_Cases']]
    #print(df_hosp)
    df_hosp['Confirmed_Cases'] = df_hosp['Confirmed_Cases'].astype('int')
    df_hosp = df_hosp[df_hosp['Category'] == 'Patientwashospitalized'].groupby(['Day','Category']).sum().reset_index()
    df_hosp['new_hospitalizations'] =0
    df_hosp['new_hospitalizations'] = df_hosp['Confirmed_Cases'].diff()
    df_hosp = df_hosp.fillna(0)
    df_hosp['Hospitalizations'] = df_hosp['Confirmed_Cases']
    df_hosp = df_hosp[['Day','Hospitalizations','new_hospitalizations']]

    #df_cases = pd.joidf_cases.concat(df_hosp,how='left',on=['Day'])
    df_cases = pd.merge(df_cases, df_hosp, how='left',on=['Day'])
    df_cases = pd.merge(df_cases, df_data, how='left',on=['Day']).fillna(0)

    df_cases['Deaths'] = df_cases['Deaths'].astype('int')

    print(df_cases)
    #print(df_hosp)
    df_cases.to_csv(os.getcwd()+'/analysis/output/covid_19_deaths_mass_gov.csv')
    return df_cases

def getMassGovData(file_data):
    #uid = 'abaner02'
    #sql = 'select * from analytics.w5_covid_19_MASSGOV_'+uid
    #df = spark.sql(sql).toPandas()
    #print(df)

    data = []
    #dates = ['3-13','3-14','3-15','3-16','3-17','3-18','3-19','3-20','3-21','3-22','3-23','3-24',
    #'3-25']
    dates = list(sorted(file_data.keys()))
    print(dates)
    #dates = ['3-23','3-24']
    county = get_county(file_data,dates)
    testing = get_testing(file_data,dates)
    hosp = get_hospitalizations(file_data,dates)
    gender = get_gender(file_data,dates)
    age = get_age(file_data,dates)
    #exposure = get_exposure(file_data,dates)
    deaths = get_deaths(file_data,dates,county,hosp)

def getADTData(spark,uid,pwd):
    print(uid,pwd)
    sql = '''
    (select * from V_BCBSMA_ADT_CMT_FNL) a'''
    #uid = 'abaner02'
    df = spark \
            .read \
            .format("jdbc") \
            .option("url", "jdbc:netezza://bntzp01z.bcbsma.com:5480/PDWAPPRP") \
            .option("user", uid) \
            .option("password", pwd) \
            .option("driver", "org.netezza.Driver") \
            .option("dbtable", sql) \
            .load()

    df.createOrReplaceTempView('ADT_DATA')

    # load county - zip mapping
    df_county_zip = pd.read_csv(os.getcwd()+'/analysis/input/county_zip_code_list.csv',sep='|',dtype={'Zip':'str'})
    spark.createDataFrame(df_county_zip).createOrReplaceTempView('COUNTY_ZIP')
    print(df_county_zip.head())

    sql = '''
    select a.*,
        SUBSTRING(a.CURR_TIMESTAMP, 1, 10) AS yyyy_mm_dd,
        SUBSTRING(a.CURR_TIMESTAMP, 1, 4) AS yyyy,
        SUBSTRING(a.CURR_TIMESTAMP, 1, 7) AS yyyy_mm,
        SUBSTRING(a.CURR_TIMESTAMP, 6, 5) AS mm_dd,
        b.County as patient_county,
        c.County as facility_county
        from ADT_DATA as a
            left join COUNTY_ZIP b
                on a.PATIENT_ZIP_CODE = b.Zip
            left join COUNTY_ZIP c
                on a.ENCOUNTER_FACILITY_ZIP = c.Zip
    '''
    df = spark.sql(sql)
    df.createOrReplaceTempView('ADT_DATA_COUNTY')
    df.write.mode("overwrite").format('parquet').saveAsTable('analytics.w5_covid_19_ADT_Netezza_'+uid)

    sql = '''
    select distinct
        mm_dd as Day,
        yyyy_mm_dd,
        facility_county,
        ENCOUNTER_FACILITY_NAME,
        ENCOUNTER_FACILITY_STREET,
        ENCOUNTER_FACILITY_CITY,
        ENCOUNTER_FACILITY_STATE,
        ENCOUNTER_FACILITY_ZIP,
        count(distinct member_id) as mem_cnt
    from ADT_DATA_COUNTY
       where yyyy = '2020' and ENCOUNTER_FACILITY_STATE = 'MA'
    group by
       mm_dd,
       yyyy_mm_dd,
       facility_county,
       ENCOUNTER_FACILITY_NAME,
       ENCOUNTER_FACILITY_STREET,
       ENCOUNTER_FACILITY_CITY,
       ENCOUNTER_FACILITY_STATE,
       ENCOUNTER_FACILITY_ZIP
    '''
    df=spark.sql(sql)
    df.createOrReplaceTempView('ADT_COUNTY_FACILITY')
    #df.write.mode("overwrite").format('parquet').saveAsTable('analytics.w5_covid_19_ADT_Facilities_'+uid)
    #df.toPandas().to_csv('facilties_county_level_ADT_by_date.csv')
    #len(df.toPandas()), df.toPandas().head()

    sql = '''
    select distinct
        mm_dd as Day,
        yyyy_mm_dd,
        facility_county,
        ENCOUNTER_FACILITY_NAME,
        ENCOUNTER_FACILITY_STREET,
        ENCOUNTER_FACILITY_CITY,
        ENCOUNTER_FACILITY_STATE,
        ENCOUNTER_FACILITY_ZIP,
        count(distinct member_id) as covid19_related_mem_cnt
    from ADT_DATA_COUNTY
       where yyyy = '2020' and ENCOUNTER_FACILITY_STATE = 'MA' and
       encounter_diagnosis like '%COVID%' or encounter_cheif_complaint like '%COVID%'
        or encounter_diagnosis like '%covid%' or encounter_cheif_complaint like '%covid%'
        or encounter_diagnosis like '%CORONAV%' or encounter_cheif_complaint like '%CORONAV%'
        or encounter_diagnosis like '%coronav%' or encounter_cheif_complaint like '%coronav%'
        or encounter_diagnosis like '%CORONA V%' or encounter_cheif_complaint like '%CORONA V%'
        or encounter_diagnosis like '%corona v%' or encounter_cheif_complaint like '%corona v%'
        or encounter_diagnosis like '%Fever%' or encounter_cheif_complaint like '%Fever%'
        or encounter_diagnosis like '%fever%' or encounter_cheif_complaint like '%fever%'
        or encounter_diagnosis like '%Flu%' or encounter_cheif_complaint like '%Flu%'
        or encounter_diagnosis like '%flu%' or encounter_cheif_complaint like '%flu%'
        or encounter_diagnosis like '%Cough%' or encounter_cheif_complaint like '%Cough%'
        or encounter_diagnosis like '%cough%' or encounter_cheif_complaint like '%cough%'
        or encounter_diagnosis like '%Viral%' or encounter_cheif_complaint like '%Viral%'
        or encounter_diagnosis like '%viral%' or encounter_cheif_complaint like '%viral%'
        or encounter_diagnosis like '%Viral Ill%' or encounter_cheif_complaint like '%Viral Ill%'
        or encounter_diagnosis like '%viral ill%' or encounter_cheif_complaint like '%viral ill%'

        or encounter_diagnosis like '%pain%' or encounter_cheif_complaint like '%pain%'
        or encounter_diagnosis like '%Pain%' or encounter_cheif_complaint like '%Pain%'
        or encounter_diagnosis like '%Shortness of breath%' or encounter_cheif_complaint like '%Shortness of breath%'
        or encounter_diagnosis like '%Shortness Of breath%' or encounter_cheif_complaint like '%Shortness Of breath%'
        or encounter_diagnosis like '%shortness of breath%' or encounter_cheif_complaint like '%shortness of breath%'

        or encounter_diagnosis like '%breathing%' or encounter_cheif_complaint like '%breathing%'
        or encounter_diagnosis like '%Breathing%' or encounter_cheif_complaint like '%Breathing%'

        or encounter_diagnosis like '%Ache%' or encounter_cheif_complaint like '%Ache%'
        or encounter_diagnosis like '%ache%' or encounter_cheif_complaint like '%ache%'
        or encounter_diagnosis like '%Nasal Congestion%' or encounter_cheif_complaint like '%Nasal Congestion%'
        or encounter_diagnosis like '%nasal congestion%' or encounter_cheif_complaint like '%nasal congestion%'
        or encounter_diagnosis like '%Sore Throat%' or encounter_cheif_complaint like '%Sore Throat%'
        or encounter_diagnosis like '%sore throat%' or encounter_cheif_complaint like '%sore throat%'
        or encounter_diagnosis like '%Diarrhea%' or encounter_cheif_complaint like '%Diarrhea%'
        or encounter_diagnosis like '%diarrhea%' or encounter_cheif_complaint like '%diarrhea%'

    group by
       mm_dd,
       yyyy_mm_dd,
       facility_county,
       ENCOUNTER_FACILITY_NAME,
       ENCOUNTER_FACILITY_STREET,
       ENCOUNTER_FACILITY_CITY,
       ENCOUNTER_FACILITY_STATE,
       ENCOUNTER_FACILITY_ZIP
    '''
    df=spark.sql(sql)
    df.createOrReplaceTempView('ADT_COUNTY_FACILITY_COVID19')

    sql = '''
    select a.*, b.covid19_related_mem_cnt
    from ADT_COUNTY_FACILITY a
        left join ADT_COUNTY_FACILITY_COVID19 b
            on a.yyyy_mm_dd = b.yyyy_mm_dd
                and a.facility_county = b.facility_county
                and a.ENCOUNTER_FACILITY_NAME = b.ENCOUNTER_FACILITY_NAME
                and a.ENCOUNTER_FACILITY_STREET = b.ENCOUNTER_FACILITY_STREET
                and a.ENCOUNTER_FACILITY_CITY = b.ENCOUNTER_FACILITY_CITY
                and a.ENCOUNTER_FACILITY_STATE = b.ENCOUNTER_FACILITY_STATE
                and a.ENCOUNTER_FACILITY_ZIP = b.ENCOUNTER_FACILITY_ZIP

    '''
    df=spark.sql(sql)
    df.createOrReplaceTempView('ADT_COUNTY_FACILITY_COVID19')
    df.write.mode("overwrite").format('parquet').saveAsTable('analytics.w5_covid_19_ADT_Facilities_'+uid)
    df.toPandas().fillna(0).to_csv(os.getcwd()+'/analysis/output/facilties_county_level_ADT_by_date.csv')
    print(df.toPandas().head(),len(df.toPandas()))

def getBCL(spark,uid,pwd):
    print(uid,pwd)
    sql = '''
    (select * from ADMIN.V_BCL_MONTHLY_CALL ) a'''
    #uid = 'abaner02'
    df = spark \
            .read \
            .format("jdbc") \
            .option("url", "jdbc:netezza://bntzp01z.bcbsma.com:5480/PDWAPPRP") \
            .option("user", uid) \
            .option("password", pwd) \
            .option("driver", "org.netezza.Driver") \
            .option("dbtable", sql) \
            .load()

    df.createOrReplaceTempView('BCL_DATA')
    df.write.mode("overwrite").format('parquet').saveAsTable('analytics.w5_BLUE_CARE_LINE_Netezza_'+uid)
    #df_p = df.toPandas()
    #print(len(df_p))
    #print(df_p.head())
    print(df.show())


if __name__ == "__main__":
    pwd = getpass.getpass("Enter LAN pwd: ")
    args = init_parser()
    # Phasing out netezza
    spark = SparkSession \
            .builder \
            .appName("COVID-19_ADT_MASSGOV") \
            .config("spark.executor.instances", "5") \
            .config("spark.executor.memory", "48G") \
            .config("spark.executor.cores", "5") \
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # get latest data and doc
    curr_date = getLatestQuarantine_doc()
    # get latest usafacts data
    usa_facts_data()

    file_nm = {}
    CWD = os.getcwd()
    for name in glob.glob(CWD+'/analysis/input/*.docx'):
        dt = name.split('report-')[1].split('-2020')[0]
        #print(name,name.split('report-')[1].split('-2020')[0])
        file_nm[dt] = {}
        file_nm[dt]['file'] = name
        tbl_cnt, data = read_daily_covid_data(name)
        file_nm[dt]['data'] = data
        file_nm[dt]['tbl_cnt'] = tbl_cnt

    #pp.pprint(file_nm)

    getMassGovData(file_nm)
    getADTData(spark,args.lanid,pwd)
    #getBCL(spark,args.lanid,pwd)
    spark.stop()
