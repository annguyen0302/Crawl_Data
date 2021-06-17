# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 09:04:50 2021

@author: An
"""
from datetime import datetime, timedelta

import requests
from pandas.io.json import json_normalize
import pandas as pd 
import os
from io import StringIO

from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import oauth2client
pd.set_option('display.max_columns',500)

# create list game 
list_game = requests.get('https://api.sensortower.com:443/v1/ios/ajax/user_apps?auth_token=2S7j1y_a4tJVzsicdBBr').json()

list_game = json_normalize(list_game['user_apps'])[['appName','id','os', 'appId']]
list_game = list_game.values.tolist()

# list country and game crawl
Download  = pd.DataFrame()
KeyWord = pd.DataFrame()

country_list = ['US','JP']

# file Download by keyword
for x in range(len(list_game)):
    for y in range(len(country_list)):
        try: 
            print(list_game[x][0],list_game[x][2], country_list[y])
            key_down = requests.get('https://api.sensortower.com:443/v1/{}/keywords/downloads/history?user_app_id={}&country={}&device=phone&date_granularity=monthly&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[x][2],list_game[x][1], country_list[y])).json()
            key_down = json_normalize(key_down['data'])       
            key_down['game_name'] = list_game[x][0]
            key_down['OS'] = list_game[x][2]
            key_down['country'] = country_list[y]
            key_down[['month1','month2']] = pd.DataFrame(key_down['data'].to_list())
            key_down[['month1_1', 'month1_2', 'month1_3']] = pd.DataFrame(key_down['month1'].to_list())
            key_down[['month2_1', 'month2_2', 'month2_3']] = pd.DataFrame(key_down['month2'].to_list())
            key_down['download_number'] = key_down['month1_3'] + key_down['month2_3']
            key_down = key_down[['game_name', 'OS','name', 'download_number','country']]
            print(key_down)
            Download = Download.append(key_down)
        except: 
            pass

Download.to_csv('%s\sensortower\Download_keyword.csv'%(os.getcwd()),index  = False)

#upload Download file




# file Keyword score
for z in range(len(list_game)):
    for t in range(len(country_list)):
        try:
            print(list_game[z][0],list_game[z][2], country_list[t])
            key_word = requests.get('https://api.sensortower.com:443/v1/{}/keywords/keywords?user_app_id={}&country={}&device=phone&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[z][2], list_game[z][1], country_list[t])).json()
            key_word = json_normalize(key_word['keywords'])[['term','downloads_estimate','traffic','phone_apps.difficulty','phone_apps.rank']]
            key_word.columns = ['term','downloads','traffic','difficulty','rank']
            key_word['game_name'] = list_game[z][0]
            key_word['OS'] = list_game[z][2]
            key_word['country'] = country_list[t]
            KeyWord = KeyWord.append(key_word)
        except:
            pass
        
KeyWord.to_csv('%s\sensortower\Keyword.csv'%(os.getcwd()),index  = False)


# file download by country
Download_by_country = pd.DataFrame()

for i in range(len(list_game)):
    try:
        print('Extracting download by country ' + list_game[i][0])
        if list_game[i][2] == 'ios':
        # if download_country is IOS 
            Download_country = requests.get('https://api.sensortower.com:443/v1/{}/sales_report_estimates?app_ids={}&date_granularity=daily&start_date=2021-02-15&end_date=2021-06-20&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[i][2], list_game[i][3])).json()
            Download_country = json_normalize(Download_country)
            Download_country['iu'] = Download_country['iu'].fillna(0)
            Download_country['au'] = Download_country['au'].fillna(0)
            Download_country['download'] = Download_country['iu'] + Download_country['au']
            Download_country['date'] = Download_country['d'].str[:10]
            Download_country = Download_country[['cc','date','download']]
            Download_country['game_name'] = list_game[i][0] + " " + list_game[i][2].upper()
            Download_country['os'] = list_game[i][2]
        elif list_game[i][2] == 'android':    
        # if download_country is Android
            Download_country = requests.get('https://api.sensortower.com:443/v1/{}/sales_report_estimates?app_ids={}&date_granularity=daily&start_date=2021-02-15&end_date=2021-06-20&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[i][2], list_game[i][3])).json()
            Download_country = json_normalize(Download_country)
            Download_country['date'] = Download_country['d'].str[:10]
            Download_country = Download_country[['c','date','u']]
            Download_country.columns = ['cc','date','download']
            Download_country['game_name'] = list_game[i][0]  + " " + list_game[i][2].upper()
            Download_country['os'] = list_game[i][2]
        Download_by_country = Download_by_country.append(Download_country)
    except:
        pass
    

Download_by_country.to_csv('%s\sensortower\Download_by_country.csv'%(os.getcwd()),index  = False)


# file retention & session duration
retention_by_game = pd.DataFrame()
sessionduration_by_game = pd.DataFrame()

for i in range(len(list_game)):
    try:
        print('Extracting retention' + " " +  list_game[i][0])
        retention = requests.get('https://api.sensortower.com:443/v1/{}/usage/retention?app_ids={}&date_granularity=all_time&start_date=2018-01-01&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[i][2], list_game[i][3])).json()
        retention = json_normalize(retention['app_data'])
        retention['game_name'] = list_game[i][0] + " " + list_game[i][2].upper()
        retention['os'] = list_game[i][2]
        retention['D1'] = retention['corrected_retention'][0][1]
        retention['D2'] = retention['corrected_retention'][0][2]
        retention['D3'] = retention['corrected_retention'][0][3]
        retention['D4'] = retention['corrected_retention'][0][4]
        retention['D5'] = retention['corrected_retention'][0][5]
        retention['D6'] = retention['corrected_retention'][0][6]
        retention['D7'] = retention['corrected_retention'][0][7]
        retention['D14'] = retention['corrected_retention'][0][14]
        retention['D30'] = retention['corrected_retention'][0][30]
        retention.drop(columns = ['app_id','country','corrected_retention', 'date','end_date','date_granularity'], inplace = True)
        retention_by_game = retention_by_game.append(retention)
    except:
        pass
    

for i in range(len(list_game)):
    try:
        print('Extracting session duration' + " " + list_game[i][0])
        session_duration = requests.get('https://api.sensortower.com:443/v1/{}/usage/session_duration?app_ids={}&date_granularity=all_time&start_date=2018-01-01&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_game[i][2], list_game[i][3])).json()
        session = json_normalize(session_duration['app_data'])
        session.drop(columns  = ['app_id','country','date_granularity','date', 'end_date', 'time_period'], inplace = True)
        session['game_name'] = list_game[i][0] + " " + list_game[i][2].upper()
        session['os'] = list_game[i][2]
        session.columns = ['confidene','average_session_duration', 'group_duration_3','group_duration_10', 'group_duration_30','group_duration_60','group_duration_180','group_duration_600','group_duration_1800','group_duration_3600','group_duration3601','game_name','os']
        sessionduration_by_game = sessionduration_by_game.append(session)
    except:
        pass
        
performance_list_game = pd.merge(retention_by_game, sessionduration_by_game, on = ['game_name','os'], how = 'outer')

performance_list_game.to_csv('%s\sensortower\performance.csv'%(os.getcwd()),index  = False)

#top performance


days_subtract_10_day = timedelta(10)
days_subtract_17_day = timedelta(17)
days_subtract_24_day = timedelta(24)
days_subtract_31_day = timedelta(31)
days_subtract_38_day = timedelta(38)
days_subtract_45_day = timedelta(45)
days_subtract_52_day = timedelta(52)
days_subtract_59_day = timedelta(59)

date_time_today = datetime.strftime(datetime.today(), '%Y-%m-%d')
date_start_10 = datetime.strftime(datetime.today() - days_subtract_10_day, '%Y-%m-%d')
date_start_17 = datetime.strftime(datetime.today() - days_subtract_17_day, '%Y-%m-%d')
date_start_24 = datetime.strftime(datetime.today() - days_subtract_24_day, '%Y-%m-%d')
date_start_31 = datetime.strftime(datetime.today() - days_subtract_31_day, '%Y-%m-%d')
date_start_38 = datetime.strftime(datetime.today() - days_subtract_38_day, '%Y-%m-%d')
date_start_45 = datetime.strftime(datetime.today() - days_subtract_45_day, '%Y-%m-%d')
date_start_52 = datetime.strftime(datetime.today() - days_subtract_52_day, '%Y-%m-%d')


list_date = [date_start_52, date_start_45, date_start_38, date_start_31, date_start_24, date_start_17, date_start_10, date_time_today]


top_performance_publish = pd.DataFrame()

for i in range(len(list_date)):
    try:    
        top_performance = requests.get('https://api.sensortower.com:443/v1/unified/top_and_trending/publishers?comparison_attribute=absolute&time_range=day&measure=units&category=7004&date={}&end_date={}&limit=50&auth_token=2S7j1y_a4tJVzsicdBBr'.format(list_date[i],list_date[i+1])).json()
        performance = json_normalize(top_performance, ['apps'])    
        for z in range(len(performance['apps'])):
            for t in range(len(performance['apps'][z])):
                    print('Extracting: ' + performance['apps'][z][t]['name'] + " " +  list_date[i] )
                    temp_performance = pd.DataFrame(columns = list(['os','Number_download','game_name','publisher_name','canonical_country','start_date']))
                    
                    temp_performance.loc[0] = [performance['apps'][z][t]['os'], performance['apps'][z][t]['units_absolute'], performance['apps'][z][t]['name'], performance['apps'][z][t]['publisher_name'], performance['apps'][z][t]['canonical_country'], list_date[i]]
                    
                    top_performance_publish = top_performance_publish.append(temp_performance)
    except:
        pass

top_performance_publish.to_csv(r'%s\sensortower\top_perf.csv'%(os.getcwd()),index  = True)
