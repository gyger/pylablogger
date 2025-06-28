# -*- coding: utf-8 -*-
"""Provides functions to parse Bluefors log files to a telegraf server

Some code came from qnl/pyCryo under MIT License
"""

import datetime

from pathlib import Path
from functools import reduce

import dateutil
import numpy as np
import pandas as pd

channel_labels = {1:'T50K', 2:'T4K', 5:'Tstill', 6:'TMXC', 7: 'T7', 8: 'T8'}

def load_bluefors_singleday(logfolder, day, channels=[1, 2, 5, 6, 7, 8], fail_gracefully=False):
    """ Construct a single dataframe out of the log file from BlueFors """
    labels = [channel_labels[k] for k in channels]
    dfs = []

    # change day into a datetime format for easier manipulation
    if isinstance(day, str):
        day = datetime.date.fromisoformat(day)

    date_string = day.strftime('%Y-%m-%d')[2:]
    logfolder = Path(logfolder) / date_string
 
    # listing all the log files in the day folder
    if not logfolder.exists():
        return None
    
    log_files_T = [logfolder / f'CH{ch} T {date_string}.log' for ch in channels]
    log_file_channel = logfolder / f'Channels {date_string}.log'
    log_file_pressure = logfolder / f'maxigauge {date_string}.log'

    # Reading the Temperatures
    for fname, label in zip(log_files_T, labels):
        if fname.exists():
            df = pd.read_csv(fname,
                            sep=",",
                            header=None)
            # due to Bluefors notation of time, we needs to do a little hack:
            df.columns = ['date', 'time', label + '_temperature']
            df['datetime'] = pd.to_datetime(df['date'] +' '+ df['time'],
                                            format=r'%d-%m-%y %H:%M:%S').dt.tz_localize(dateutil.tz.tzlocal(), ambiguous='infer').dt.tz_convert('UTC')

            if not fail_gracefully and pd.isnull(df['datetime']).values.any():
                raise ValueError(f"Missing Date in a line in {str(fname)}")

            df = df.drop('date', axis=1)
            df = df.drop('time', axis=1)
            df = df.rename(columns={'datetime':'time'})
            dfs.append(df) 

    # Reading the Channel Settings
    if log_file_channel.exists():
        df = pd.read_csv(log_file_channel,
                        sep=",",
                        header=None)

        # Old log file, single turbo
        columns_channels_old = ['date', 'time', 'mode', None, 'v11_switch', None, 'v2_switch', None, 'v1_switch', None, 
                                'turbo1_switch', None, 'v12_switch', None, 'v3_switch', None, 'v10_switch', None, 'v14_switch', None, 
                                'v4_switch', None, 'v13_switch', None, 'compressor_switch', None, 'v15_switch', None, 'v5_switch', None, 
                                'hs-still_switch', None, 'v21_switch', None, 'v16_switch', None, 'v6_switch', None, 
                                'scroll1_switch', None, 'v17_switch', None, 'v7_switch', None, 'scroll2_switch', None,
                                'v18_switch', None, 'v8_switch', None, 'pulsetube_switch', None, 'v19_switch', None, 
                                'v20_switch', None, 'v9_switch', None, 'hs-mc_switch', None, 'ext_switch'
                                ]
        columns_channels_2_Turbo = ['date', 'time', 'mode', 
                                    None, 'v1_switch', None, 'v2_switch', None, 'v3_switch', None, 'v4_switch', 
                                    None, 'v5_switch', None, 'v6_switch', None, 'v7_switch', None, 'v8_switch', 
                                    None, 'v9_switch', None, 'v10_switch', None, 'v11_switch', None, 'v12_switch', 
                                    None, 'v13_switch', None, 'v14_switch', None, 'v15_switch', None, 'v16_switch', 
                                    None, 'v17_switch', None, 'v18_switch', None, 'v19_switch', None, 'v20_switch', 
                                    None, 'v21_switch', None,'v22_switch', None, 'v23_switch', 
                                    None, 'turbo1_switch', None, 'turbo2_switch', None, 'scroll1_switch', None, 'scroll2_switch', 
                                    None, 'compressor_switch', None, 'pulse_tube', 
                                    None, 'hs-still_switch', None, 'hs-mc_switch', None, 'ext_switch'
                                    ]        

        # due to Bluefors notation of time, we needs to do a little hack:
        df.columns = columns_channels_2_Turbo
        df = df.iloc[:, np.where(np.array(columns_channels_2_Turbo) != None)[0]]

        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'],
                                        format=r'%d-%m-%y %H:%M:%S').dt.tz_localize(dateutil.tz.tzlocal(), ambiguous='infer').dt.tz_convert('UTC')
        if not fail_gracefully and pd.isnull(df['datetime']).values.any():
            raise ValueError(f"Missing Date in a line in {str(log_file_channel)}")

        df = df.drop('date', axis=1)
        df = df.drop('time', axis=1)
        df = df.rename(columns={'datetime':'time'})

        dfs.append(df) 

    # Reading the Pressure Gauges
    if log_file_pressure.exists():
        df = pd.read_csv(log_file_pressure,
                        sep=",",
                        header=None)

        columns_channels = ['date', 'time',
                            None, None, 'P1_enable', 'P1_pressure', None, None, 
                            None, None, 'P2_enable', 'P2_pressure', None, None, 
                            None, None, 'P3_enable', 'P3_pressure', None, None, 
                            None, None, 'P4_enable', 'P4_pressure', None, None, 
                            None, None, 'P5_enable', 'P5_pressure', None, None,
                            None, None, 'P6_enable', 'P6_pressure', None, None, None
                        ]

        # due to Bluefors notation of time, we needs to do a little hack:
        df.columns = columns_channels
        df = df.iloc[:, np.where(np.array(columns_channels) != None)[0]]

        df['datetime'] = pd.to_datetime(df['date'] +' '+ df['time'],
                                        format=r'%d-%m-%y %H:%M:%S').dt.tz_localize(dateutil.tz.tzlocal(), ambiguous='infer').dt.tz_convert('UTC')
        if not fail_gracefully and pd.isnull(df['datetime']).values.any():
            raise ValueError(f"Missing Date in a line in {str(log_file_pressure)}")
        
        df = df.drop('date', axis=1)
        df = df.drop('time', axis=1)
        df = df.rename(columns={'datetime':'time'})
        
        dfs.append(df)    

    # merging all the dframes into one
    if(len(dfs) > 0):
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['time'], how='outer'), dfs)
    else:
        df_merged = None

    return df_merged

def load_bluefors_logfolder(logfolder, since = None, till = None, fail_gracefully=False):
    """ Loads the Bluefors logs since a given time stamp(exclusive) or day (inclusive) till a given timestamp (exclusive) or day (inclusive)
    """
    logfolder = Path(logfolder)
    
    if till is None:
        till = pd.Timestamp(datetime.datetime.today() + datetime.timedelta(days=1), tz=dateutil.tz.tzlocal()).tz_convert('UTC')
    elif isinstance(till, str):
        till = pd.Timestamp(datetime.date.fromisoformat(till) + datetime.timedelta(days=1), tz=dateutil.tz.tzlocal()).tz_convert('UTC')
    else:
        till = pd.Timestamp(till).tz_convert('UTC')

    if since is None:
        since = till - datetime.timedelta(days=2)
    elif isinstance(since, str):
        since = pd.Timestamp(datetime.date.fromisoformat(since), tz=dateutil.tz.tzlocal()).tz_convert('UTC')
    else:
        since = pd.Timestamp(since).tz_convert('UTC')

    days = pd.date_range(start=since, end=till)

    dfs = []
    for i, day in enumerate(days.to_pydatetime()):
        df = load_bluefors_singleday(logfolder, day, fail_gracefully=fail_gracefully)
        if df is not None:
            if i == 0:
                df = df[df['time'] > since]
            dfs.append(df)
            
    if len(dfs) > 0:
        dfs[-1] = dfs[-1][dfs[-1]['time'] < till]
    else:
        return pd.DataFrame()
 
    return pd.concat(dfs, axis=0, ignore_index=True).sort_values(by="time")