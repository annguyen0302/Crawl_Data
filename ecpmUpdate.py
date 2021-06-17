# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 11:25:31 2021

@author: An
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 09:53:11 2021

@author: An
"""

import os
import pickle 
import pandas as pd
import numpy as np
import requests
from pandas.io.json import json_normalize
import sys

from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import oauth2client

# Constants for the AdMob API service.


API_NAME = 'admob'
API_VERSION = 'v1'
API_SCOPE = 'https://www.googleapis.com/auth/admob.readonly'

TOKEN_FILE = 'token.pickle'

def load_user_credentials():
    client_secrets = os.path.join(
    os.path.dirname(os.getcwd()), 'client_secret_675102216040-5f9084fb90o4ktfsp6g6amhdus7mj7ti.apps.googleusercontent.com.json')
    return client_secrets


# function to connect token
if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, 'rb') as token:
      credentials = pickle.load(token)

    if credentials and credentials.expired and credentials.refresh_token:
      credentials.refresh(Request())

  # If there are no valid stored credentials, authenticate using the
  # client_secrets file.
else:
    client_secrets = load_user_credentials()
    flow = Flow.from_client_secrets_file(
        client_secrets,
        scopes=[API_SCOPE],
        redirect_uri='urn:ietf:wg:oauth:2.0:oob')

    # Redirect the user to auth_url on your platform.
    auth_url, _ = flow.authorization_url()
    print('Please go to this URL: {}\n'.format(auth_url))

    # The user will get an authorization code. This code is used to get the
    # access token.
    code = input('Enter the authorization code: ')
    flow.fetch_token(code=code)
    credentials = flow.credentials

  # Save the credentials for the next run.
with open('token.pickle', 'wb') as token:
    pickle.dump(credentials, token)

  # Build the AdMob service.
admob = build(API_NAME, API_VERSION, credentials=credentials)

# admob api file 
result = admob.accounts().get(name='accounts/{}'.format("pub-1453406935552044")).execute()

# Print the result.
print('Name: ' + result['name'])
print('Publisher ID: ' + result['publisherId'])
print('Currency code: ' + result['currencyCode'])
print('Reporting time zone: ' + result['reportingTimeZone'])

df_empty_admob = pd.DataFrame()

# update time to crawl
time_list = [[2020,6,30],[2020,7,31],[2020,8,31],[2020,9,30],[2020,10,31],
             [2020,11,30],[2020,12,31],
             [2021,1,10], [2021,1,20], [2021,1,31],
             [2021,2,10], [2021,2,20], [2021,2,28],
             [2021,3,10], [2021,3,20], [2021,3,31],
             [2021,4,10], [2021,4,20], [2021,4,30],
             [2021,5,10], [2021,5,20], [2021,5,31],
             [2021,6,10], [2021,6,20]]

# create report  
for x in range(len(time_list)):
    try:
            if time_list[x][0] == 2020:
                date_range = {
                        'start_date': {'year':time_list[x][0],'month': time_list[x][1], 'day': 1},
                        'end_date' : {'year': time_list[x][0],'month': time_list[x][1], 'day': time_list[x][2]}
                        } 
            elif time_list[x][0] == 2021 and time_list[x][2] == 10:
                date_range = {
                        'start_date': {'year':time_list[x][0],'month': time_list[x][1], 'day': 1},
                        'end_date' : {'year': time_list[x][0],'month': time_list[x][1], 'day': 10}}
            elif time_list[x][0] == 2021 and time_list[x][2] == 20:
                date_range = {
                        'start_date': {'year':time_list[x][0],'month': time_list[x][1], 'day': 11},
                        'end_date' : {'year': time_list[x][0],'month': time_list[x][1], 'day': 20}}
            elif time_list[x][0] == 2021 and time_list[x][2] > 20:
                date_range = {
                        'start_date': {'year':time_list[x][0],'month': time_list[x][1], 'day': 21},
                        'end_date' : {'year': time_list[x][0],'month': time_list[x][1], 'day': time_list[x][2]}}

                
            dimensions = ['DATE','APP','PLATFORM','COUNTRY', 'FORMAT','AD_SOURCE']
            
            metrics = ['ESTIMATED_EARNINGS', 'IMPRESSIONS','OBSERVED_ECPM', 'AD_REQUESTS','MATCHED_REQUESTS','CLICKS']
            

            
            report_spec = {
                  'date_range': date_range,
                  'dimensions': dimensions,
                  'metrics': metrics
                  }
            
            request_body = {'report_spec': report_spec}
            
            # load report and ETL 
            response_admob = admob.accounts().mediationReport().generate(
                parent='accounts/{}'.format("pub-1453406935552044"), body=request_body).execute()
            
            
            df_admob = json_normalize(response_admob[:])
            
            df_admob = df_admob.iloc[:,7:-1]
            print(df_admob.shape)
            df_admob.columns = ['Date','App_code','App','Device_OS','Country','AdsType','AdSource_code','AdsSource','Revenue','Impression','eCPM','Ad_requests','Matched_requests','Clicks']
            df_admob = df_admob[['App','Date','Country','Device_OS','AdsSource','AdsType','Revenue','Impression','eCPM','Ad_requests','Matched_requests','Clicks']]
            
            df_empty_admob = df_empty_admob.append(df_admob)
            print(date_range)
    except:
        pass
df_admob_total = df_empty_admob            

df_admob_total['eCPM'] = df_admob_total['eCPM'].astype(float)/1000000
df_admob_total['Revenue'] = df_admob_total['Revenue'].astype(float)/1000000

df_admob_total['Date'] = pd.to_datetime(df_admob_total['Date']).dt.strftime('%Y-%m-%d')

df_admob_total['Mediation'] = 'Admob'

# ironsrc file
# authorize step
URL = 'https://platform.ironsrc.com/partners/publisher/auth'

authHeaders  = { 'secretkey' : 'f25c1821e542639d699a1bc74f416975', 'refreshToken' : '4d6a33267a2efc38994fa199d459aceb'}

response_ironsource = requests.get(url = URL, headers =  authHeaders)

key = str(response_ironsource.json())

print(key)

#update time to crawl
time_list_ironsource = [[2020,6,30],[2020,7,31],[2020,8,31],[2020,9,30],[2020,10,31],[2020,11,30],[2020,12,31], [2021,1,31], [2021,2,28], [2021,3,31],[2021,4,30],[2021,5,31],[2021,6,30]]

df_empty_ironsource = pd.DataFrame()

for x in range(len(time_list_ironsource)):
            URL_report = 'https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?startDate={0}-{1}-01&endDate={0}-{1}-{2}&\
            metrics=revenue,impressions,eCPM&breakdown=date,country,app,platform,adSource'.format(time_list_ironsource[x][0], 
            time_list_ironsource[x][1], time_list_ironsource[x][2])
            
            
            hed = {'Authorization': 'Bearer ' + key}
            
            response_report = requests.get(url = URL_report, headers = hed)
            
            # ETL data 
            
            df_ironsource  = response_report.json()
            
            df_ironsource = json_normalize(df_ironsource, 'data',['date','adUnits','platform','appName','providerName'],errors = 'ignore')
            
            df_ironsource.rename(columns =  {'revenue':'Revenue','impressions':'Impression', 'eCPM':'eCPM', 'providerName':'AdsSource','countryCode':'Country','date':'Date'
                                             , 'adUnits':'AdsType','platform':'Device_OS','appName': 'App', }, inplace = True)
            print(df_ironsource.shape)
            df_ironsource = df_ironsource[['App','Date','Country','Device_OS','AdsSource','AdsType','Revenue','Impression','eCPM']]
            df_empty_ironsource = df_empty_ironsource.append(df_ironsource)
            print('Done ironsource {}-{}-{}'.format(time_list_ironsource[x][0], time_list_ironsource[x][1], time_list_ironsource[x][2]))
            

df_ironsource_total = df_empty_ironsource

df_ironsource_total['AdsType'].replace({'Interstitial':'interstitial', 'Banner':'banner', 'Rewarded Video':'rewarded'}, inplace = True)

df_ironsource_total['Ad_requests'] = np.nan
df_ironsource_total['Matched_requests'] = np.nan
df_ironsource_total['Clicks'] = np.nan
df_ironsource_total['Mediation'] = 'Ironsource'

# main file

df_total = df_ironsource_total.append(df_admob_total)
df_total = df_total[df_total.Date != 'NaT']

df_total.to_csv('%s\Mediation.csv'%(os.getcwd()),index  = False)

def upload_to_bucket(blob_name, path_to_file, bucket_name):
    storage_client = storage.Client.from_service_account_json('%s/Tile King-e7870dbf5d81.json' %(os.getcwd()))
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    #returns a public url
    return blob.public_url

upload_to_bucket("Mediation.csv",'%s\Mediation.csv'%(os.getcwd()), "tile-king-56566468.appspot.com")