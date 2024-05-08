#!/usr/bin/env python
# coding: utf-8

import os
import requests
import json
import pandas as pd
import pickle
from datetime import datetime, timedelta

def fetch_daily_quotes(headers, code:str, st:str=None, end:str=None) -> pd.DataFrame:
    params = {'code': code, 'from':st, 'to':end}
    r = requests.get("https://api.jquants.com/v1/prices/daily_quotes",params=params, headers=headers)
    r.raise_for_status()
    l = r.json()['daily_quotes']
    df = pd.DataFrame(l)

    return df

def add_one_day(date_str):
    return (datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')

def load_data(load_data_path):
    if os.path.exists(load_data_path):
        # load past data
        with open(load_data_path, 'rb') as f:
            df_past = pickle.load(f)
    else:
        raise NoFileError(f'Data does not exist at "{load_data_path}". Check if argument "load_data_path" is correct.')
    return df_past

def check_df_range(df):
    first = df['Date'].min()
    last  = df['Date'].max()
    return first, last


class NoFileError(Exception):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return self._msg


class myjquants():
    def __init__(self, mailaddress:str, password:str):
        """Generate access token from access key(mailaddress and password), and stores it in headers.

        Attributes
        ----------
        headers : str
            access key -- mail address
        password    : str
            access key -- password
        """

        data={"mailaddress":mailaddress, "password":password}
        r_post = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))

        REFRESH_TOKEN = r_post.json()['refreshToken']
        r_post = requests.post(f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={REFRESH_TOKEN}")

        idToken = r_post.json()['idToken']
        headers = {'Authorization': 'Bearer {}'.format(idToken)}
        
        self.headers = headers
    
    def save_quotes(self, code:str, st:str=None, end:str=None,
                    load_data_path:str=None, dump_data_path:str=None) -> pd.DataFrame:
        """Fetch daily quotes and merge with past downloaded data.

        Parameters
        ----------
        code : str
            Stock code (e.g. '27800' or '2780')
        st : str
            Download start date (e.g. '20240503')
        end : str
            Download end date (e.g. '20240503')
        load_data_path : str
            Path of past data. Data must be dumped using pickle.
            If null, past data will not be used. (e.g. './load_data.pickle')
        dump_data_path : str
            Path where to dump created data. Data will be dumped using pickle.
            If null, dump data will not be created. (e.g. './dump_data.pickle')

        Returns
        -------
        df : pandas.DataFrame
            Merged data of past downloaded data and downloaded data.
        """
        # load data and check first & last date of loaded data.
        if load_data_path is not None:
            df_past = load_data(load_data_path)
            first, last = check_df_range(df_past)
        
        # download data via jquants api.
        ## if load_data_path is defined, download only undownloaded data and merge with past downloaded data.
        if load_data_path is not None:
            before_past_data = {}
            after_past_data  = {}
            
            before_past_data['st'] = st
            before_past_data['end'] = first
            
            after_past_data['st'] = last
            after_past_data['end'] = end
            
            try:
                df_before = fetch_daily_quotes(self.headers, code, before_past_data['st'], before_past_data['end'])
            except requests.exceptions.RequestException as e:
                df_before = pd.DataFrame(columns=df_past.columns)
            else:
                df_before = df_before.drop(df_before.index[-1]) # delete last data, thus there are duplicates between df_before and df_past
            
            try:
                df_after = fetch_daily_quotes(self.headers, code, after_past_data['st'], after_past_data['end'])
            except requests.exceptions.RequestException as e:
                df_before = pd.DataFrame(columns=df_past.columns)
            else:
                df_after = df_after.drop(df_after.index[0]) # delete first data, thus there are duplicates between df_after  and df_past
            
            df = pd.concat([df_before,df_past,df_after], ignore_index=True)
        
        ## if load_data_path is not defined, simply download data from jquants api.
        else:
            df = fetch_daily_quotes(self.headers, code, st, end)
        
        # Pickle data
        if dump_data_path is not None:
            with open(dump_data_path, 'wb') as f:
                pickle.dump(df, f)
            
        return df
