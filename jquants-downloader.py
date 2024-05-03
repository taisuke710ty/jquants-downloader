#!/usr/bin/env python
# coding: utf-8

import requests
import json
import tqdm
import pandas as pd
import pickle
from datetime import datetime, timedelta

class myjquants():
    def __init__(self, mailaddress:str, password:str):
        """
        generate access token from access key(mailaddress and password), and stores it in headers.

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
    
    def fetch_quotes(self, code:str, st:str=None, end:str=None) -> pd.DataFrame:
        """
        fetch daily quotes from jquants api.

        Parameters
        ----------
        code : str
            stock code (e.g. '27800' or '2780')
        st   : str
            download start date (e.g. '20240503')
        end  : str
            download end date (e.g. '20240503')

        Returns
        -------
        df : pandas.DataFrame
            downloaded data
        """
        params = {'code': code, 'from':st, 'to':end}
        r = requests.get("https://api.jquants.com/v1/prices/daily_quotes",params=params, headers=self.headers)
        if not r.status_code == 200: raise RequestException
        l = r.json()['daily_quotes']
        df = pd.DataFrame(l)
            
        return df
    
    def save_quotes(self, codes:list, end:str=None, pickle_path:str=None, pickle_dump_path:str='quotes.pickle') -> dict:
        """
        fetch daily quotes and merge with past downloaded data.

        Parameters
        ----------
        codes       : list
            stock code list (e.g. ['13010', '27800'] or ['1301', '2780'])
        end         : str
            download end date (e.g. '20240503')
        pickle_path : str
            path of past downloaded data
        pickle_dump_path : str
            path for dumping merged data

        Returns
        -------
        df : dict
            dictionary of downloaded data.
                key   = stock code
                value = download data stored in pandas.DataFrame
        """
        # check download date range
        download_range = {}
        
        ## Check if past data exists.
        if not pickle_path == None:
            # load past data
            with open(pickle_path, 'rb') as f:
                qutoes = pickle.load(f)
            
            for code in codes:
                if code in quotes.values():
                    df = quotes[code]
                    st = df['Date'].max()
                    st = (datetime.strptime(st, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d') #add 1 day to 'st'
                else:
                    st = None
                
                if end == None:
                    download_range[code] = [st, None]
                elif end<st:
                    download_range[code] = ['skip', 'skip']
                else:
                    download_range[code] = [st, end]
            
        else:
            # set download date range
            download_range = {code:[None, end] for code in codes}
            
            # create dict object to store quotes data
            quotes = {}
        
        # Download Data and Merge with past data
        for code in tqdm.tqdm(codes, total=len(codes)):
            load_st, load_end = download_range[code]
            if not load_st == 'skip':
                df = self.fetch_quotes(code, load_st, load_end)
                if code in quotes.values():
                    merged_df = pd.concat([quotes[code],df])
                else:
                    merged_df = df.copy()
                quotes[code] = merged_df
        
        # Pickle
        with open(pickle_dump_path, 'wb') as f:
            pickle.dump(quotes, f)
            
        return quotes

