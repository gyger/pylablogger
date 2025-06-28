# -*- coding: utf-8 -*-
"""Provides functions to parse Attodry log files to a telegraf server
"""

import datetime
from datetime import datetime as dt
from pathlib import Path
from functools import reduce

import numpy as np
import pandas as pd

def load_attodry_singlefile(logfile, fail_gracefully=False):
    """ Construct a single dataframe out of the log file from an AttoDry System
        Supports:
          - Attodry800
          - Attodry2100
    """
    renamedict = {'time (s)' : 'time', 
              'Turbo Pump Frequency (Hz)' : 'Turbopump_frequency', 
              'Sample Heater Power (W)' : 'Sampleheater_power',
              'Exchange Heater Power (W)' : 'Exchangeheater_power',
              'Sample Temperature (K)' : 'Sample_temperature', 
              'Magnet Temperature (K)' : 'Coldhead_temperature',
              'User Temperature (K)' : 'User_temperature',
              'Cryo In Pressure (mbar)' : 'Cryo_pressure'}
    
    date = dt.strptime(
        pd.read_csv(logfile, sep = '\t', nrows = 0, parse_dates = True).columns[0], 
        '%d %b %Y_%H:%M:%S')
    
    df = pd.read_csv(logfile, sep = '\t', skiprows = 1).rename(columns = renamedict)
    df['Cryo_enable'] = True
    df['time'] = df['time'].apply(lambda x: dt.utcfromtimestamp(date.timestamp() + x))
    
    return df[list(renamedict.values())]


def load_attodry_logfolder(logfolder, since = None, till = None, fail_gracefully=False):
    """ Loads all the logfiles in the attodry folder, returning only the log data between since and till.
    """
    logfolder = Path(logfolder)
    
    if till is None:
        till = dt.now() + datetime.timedelta(days = 1)
    elif isinstance(till, str):
        till = dt.utcfromtimestamp(dt.fromisoformat(till).timestamp())

    if since is None:
        since = till - datetime.timedelta(days = 2)
    elif isinstance(since, str):
        since = dt.utcfromtimestamp(dt.fromisoformat(since).timestamp())

    df = load_attodry_singlefile(list(logfolder.glob('*.txt'))[-1], fail_gracefully=fail_gracefully)

    df = df[(df['time'] > pd.Timestamp(since)) & (df['time'] < pd.Timestamp(till))]
        
    return df.sort_values(by="time")