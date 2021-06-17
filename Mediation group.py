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

from datetime import datetime, timedelta

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

df_mediation = pd.DataFrame()

for i in range(3,65):
    date_time_today = datetime.strftime(datetime.today(), '%Y-%m-%d')
    
    time = datetime.strftime(datetime.today() - timedelta(i), '%Y-%m-%d').split('-')
    
    date_range = {
        'start_date': {'year':int(time[0]),'month': int(time[1]), 'day': int(time[2])},
        'end_date' : {'year': int(time[0]),'month': int(time[1]), 'day': int(time[2])}
    } 
    
    dimensions = ['DATE','APP','PLATFORM','COUNTRY', 'FORMAT','AD_SOURCE','AD_SOURCE_INSTANCE','AD_UNIT','MEDIATION_GROUP']
                
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
    
    df_admob.to_csv('%s\df_admob.csv'%(os.getcwd()),index  = False)
    df_admob.columns = ['Date','App_code','App','Device_OS','Country','AdsType','AdSource_code',
                        'AdsSource','AdsSource_instance_code','AdsSource_instance', 'Ad_unit_code','Ad_unit','Mediation_group_code', 'Mediation_group',
                        'Revenue','Impression','eCPM','Ad_requests','Matched_requests','Clicks']
    
    df_admob.dropna(subset = ['Date'], inplace = True)
    df_mediation = df_mediation.append(df_admob)
    print('append {},{},{}'.format(int(time[0]), int(time[1]), int(time[2])))
    print(df_admob.shape)
    print(df_mediation.shape)
    