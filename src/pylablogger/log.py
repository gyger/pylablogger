#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entry Point for logging to Grafana using telegraf

 telegraf: https://github.com/influxdata/telegraf
 use uvx: https://docs.astral.sh/uv/guides/tools/
     uvx --from git+https://github.com/gyger/pylablogger@2843b87 lablogger
"""

import pickle
from pathlib import Path
import click

import pylablogger
import pylablogger.hardware.bluefors as bftools
import pylablogger.hardware.attodry as adtools

@click.group('log')
def cli():
    """ This command groups allows logging from different experimental systems as a telegraf data provider
    """
    pass

@cli.command()
@click.option('--logfolder', required=True)
@click.option('--since', default=None)
@click.option('--till', default=None)
@click.option('--device-name', default='bluefors')
@click.option('--override-stored/--no-override-stored', default=True)
@click.option('--verbose', is_flag=True, default=False)
@click.option('--fail-gracefully', default=False)
@click.option('--configfolder', required=False, default=None)
def bluefors(logfolder, since, till, device_name, override_stored, verbose, fail_gracefully, configfolder):
    """ Evaluates the log file folder of a bluefors system and prints timestamps to STDOUT.
    """

    timepickle = configfolder or pylablogger.config_path
    timepickle = Path(timepickle) / f"{device_name}_data.pickle"
    timepickle.parent.mkdir(parents=True, exist_ok=True)

    if not since:
        since = _get_last_timestamp(timepickle)
    if not since and verbose:
        #FIXME add message if verbose is on.
        return

    dfs = bftools.load_bluefors_logfolder(logfolder, since=since, till=till, fail_gracefully=fail_gracefully)
    if dfs.empty:
        return
    
    last_date = dfs['time'].max().to_pydatetime()

    _df_to_influxdb(dfs, device_name, fail_gracefully)
    if override_stored:
        _set_last_timestamp(last_date, timepickle)
        
@cli.command()
@click.option('--logfolder', required=True)
@click.option('--since', default=None)
@click.option('--till', default=None)
@click.option('--device-name', default='attocube')
@click.option('--override-stored/--no-override-stored', default=True)
@click.option('--verbose', is_flag=True, default=False)
@click.option('--fail-gracefully', default=False)
@click.option('--configfolder', required=False, default=None)
def attodry(logfolder, since, till, device_name, override_stored, verbose, fail_gracefully, configfolder):
    """ Evaluates the log file folder of a bluefors system and prints timestamps to STDOUT.
    """

    timepickle = configfolder or pylablogger.config_path
    timepickle = Path(timepickle) / f"{device_name}_data.pickle"
    timepickle.parent.mkdir(parents=True, exist_ok=True)

    if not since:
        since = _get_last_timestamp(timepickle)
    if not since and verbose:
        #FIXME add message if verbose is on.
        return

    dfs = adtools.load_attodry_logfolder(logfolder, since=since, till=till, fail_gracefully=fail_gracefully)
    if dfs.empty:
        return
    
    last_date = dfs['time'].max().to_pydatetime()

    _df_to_influxdb(dfs, device_name, fail_gracefully)
    if override_stored:
        _set_last_timestamp(last_date, timepickle)

def _get_last_timestamp(timepickle):
    if timepickle.exists() and timepickle.lstat().st_size > 0:
        with timepickle.open('rb') as pickles:
            return pickle.load(pickles)
    else:
        return None

def _set_last_timestamp(timestamp, timepickle):
    timepickle.touch(exist_ok=True)
    with timepickle.open('wb') as pickles:
        pickle.dump(timestamp, pickles)

def _df_to_influxdb(dfs, device_name, fail_gracefully=False):
    if dfs.empty:
        return

    for idx, row in dfs.iterrows():
        try:
            time_ns = int(row['time'].timestamp()*1E9)
            device_str = f"cryo_sensor,device={device_name}"
        
            for key, value in row[row.notnull()].items():
                if '_' in key:
                    sensor_id, sensor_type = key.split('_')

                    if sensor_type == "pressure":
                        if row.get(f'{sensor_id}_enable', True):
                            print(f'{device_str},sensor_id={sensor_id} {sensor_type}={value} {time_ns}')
                    elif sensor_type == "enable":
                        pass
                    else:
                        print(f'{device_str},sensor_id={sensor_id} {sensor_type}={value} {time_ns}')
                else:
                    pass
        except ValueError as err:
            if not fail_gracefully:
                print(f'Error was with row: {row}')
                raise
            else:
                pass
            