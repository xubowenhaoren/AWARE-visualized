import sys
from datetime import datetime

import pandas as pd
from flask import Flask, render_template

import json
import plotly

import df_clean_helper
import plot_generator

import os
import time
import multiprocessing as mp

import mysql.connector
from mysql.connector import Error

'''
This pipeline is the prototyping for the display panel based on Zongyuan's 
original visualization work. 
It adds visualization for location and activity data. 
Design: 
https://whiteboardfox.com/1627427-5577-3612

For location data, it supports mapping per-day movements, with colors by inter-sample interval
(e.g. 5 minute update interval is group 2, yellow) or by the time of the day (e.g. 8AM - 10PM is red). 
It also plots histograms with inter-sample interval, inter-sample distance, 
and distance traveled by the time of the day (e.g. 12AM - 6AM, 6AM - 12PM). 

For the activity data, it supports mapping motion groups colored by activity type. 
It also plots histograms with inter-sample interval, intersample distance by activity type, and activity type by the 
time of the day. 

The data format required: 
- Daily dumps of data in *.txt for each table and for each PID, e.g. PIDxxx_20200409_locations.txt
- The txt data is deduplicated, aggregated (meaning: joined from multiple devices), and cleaned. 
- There should be a background process for this. 

The pipeline lifecycle: 
- The backend gets the request for the data (PIDxxx, data range T)
- Then to aggregation (here means joining the activity and location data together) and the processing
- Then create the viz objects (the plots)
'''

device_id_list = []
apple_device_id_set = set()
android_device_id_set = set()


# reads the csv, cleans the df, slice the df by the frequency,
# and finally return the slide (or a placeholder) based on the request_param.
def df_prep_helper(table, device_id, start_date, end_date, slice_freq='1D'):
    print("received", device_id, start_date, end_date)
    df = get_plotting_data_from_mysql(table, device_id, start_date, end_date)

    if len(df) >= 5:
        df = df_clean_helper.clean_up_dataframe(df)
    return df


def datetime_to_timestamp(date_time_str):
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    timestamp = datetime.timestamp(date_time_obj)
    return int(timestamp * 1000)


# date_str must be in the format YYYY-MM-DD
def get_day_start_end_timestamp(start_date, end_date):
    # current date and time
    date_start_str = start_date + ' 00:00:00'
    date_end_str = end_date + ' 23:59:59'

    date_start_timestamp = datetime_to_timestamp(date_start_str)
    date_end_timestamp = datetime_to_timestamp(date_end_str)
    return date_start_timestamp, date_end_timestamp


def get_df_from_mysql(query, m_columns):
    with open('./connection_details.json') as f:
        data = json.load(f)
        host = data['host']
        database = data['database']
        user = data['user']
        password = data['password']
        connection = mysql.connector.connect(host=host,
                                             database=database,
                                             user=user,
                                             password=password)

    if connection.is_connected():
        cursor = connection.cursor()
        # Now run the query, generate the df, apply the columns and return the df
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        if len(df.columns) == len(m_columns):
            df.columns = m_columns
        else:
            df = pd.DataFrame(columns=m_columns)

        # close the connection and return
        connection.close()
        cursor.close()
        return df
    else:
        connection.close()
        print("Connect failed. MySQL connection is closed")
        return None


def download_date_list(device_id):
    query = 'SELECT timestamp FROM locations WHERE device_id = "{}";'.format(device_id)
    return get_df_from_mysql(query, ['timestamp'])


def get_plotting_data_from_mysql(table, device_id, start_date, end_date):
    current_device_id = device_id
    start_timestamp, end_timestamp = get_day_start_end_timestamp(start_date, end_date)
    if device_id in apple_device_id_set:
        print("current device is Apple")
    else:
        print("current device is Android")

    if table == "location":

        query = 'SELECT timestamp, device_id, double_latitude, double_longitude, ' \
                'double_bearing, double_speed, double_altitude, provider, accuracy, ' \
                'label FROM locations WHERE device_id = "{}" AND timestamp >= {} AND ' \
                'timestamp <= {};'.format(current_device_id, start_timestamp, end_timestamp)
        columns = ['timestamp', 'device_id', 'double_latitude', 'double_longitude', 'double_bearing',
                   'double_speed', 'double_altitude', 'provider', 'accuracy', 'label']
        return get_df_from_mysql(query, columns)
    elif device_id in apple_device_id_set:
        query = 'SELECT timestamp, device_id, activities, confidence, stationary, ' \
                'walking, running, automotive, cycling, unknown, label FROM ' \
                'plugin_ios_activity_recognition WHERE device_id = "{}" AND timestamp >= {} AND ' \
                'timestamp <= {};'.format(current_device_id, start_timestamp, end_timestamp)
        columns = ['timestamp', 'device_id', 'activities', 'confidence', 'stationary', 'walking',
                   'running', 'automotive', 'cycling', 'unknown', 'label']
        return get_df_from_mysql(query, columns)
    else:
        query = 'SELECT timestamp, device_id, activity_name, activity_type, confidence FROM ' \
                'plugin_google_activity_recognition WHERE device_id = "{}" AND timestamp >= {} AND ' \
                'timestamp <= {};'.format(current_device_id, start_timestamp, end_timestamp)
        columns = ['timestamp', 'device_id', 'activity_name', 'activity_type', 'confidence']
        return get_df_from_mysql(query, columns)


def download_device_id_list():
    # Now, export the device_id list from the locations table
    global device_id_list, apple_device_id_set, android_device_id_set
    query = 'SELECT device_id, board FROM aware_device;'
    columns = ['device_id', 'board']
    all_device_id_df = get_df_from_mysql(query, columns)
    device_id_list = all_device_id_df['device_id'].to_list()

    apple_device_id = all_device_id_df[all_device_id_df['board'] == 'Apple']['device_id'].to_list()
    android_device_id = all_device_id_df[all_device_id_df['board'] != 'Apple']['device_id'].to_list()

    if len(apple_device_id_set) == 0:
        apple_device_id_set = set(apple_device_id)
    if len(android_device_id_set) == 0:
        android_device_id_set = set(android_device_id)

    print("apple", apple_device_id_set)
    print("-" * 30)
    print("android", android_device_id_set)
    return device_id_list, android_device_id, apple_device_id


def get_date_list_helper(device_id, slice_freq='1D'):
    df_date_list = download_date_list(device_id)
    df_date_list = df_clean_helper.clean_up_dataframe(df_date_list)
    date_list = []
    for frequency_slice, df_slice in df_date_list.groupby(pd.Grouper(freq=slice_freq, key='timestamp')):
        date_list.append(str(frequency_slice.date()))
    return date_list


# With the task id, run the matching plotting tasks.
def multiprocessing_pool_helper(task_id, df, result_list):
    # location plotting tasks
    if task_id == 0:
        result_list.append((task_id,
                            plot_generator.update_interval_histogram(df.copy(), "Location Update Interval (minutes)")))
    elif task_id == 1:
        result_list.append((task_id, plot_generator.map_location_inter_sample_interval(df.copy())))
    elif task_id == 2:
        result_list.append((task_id, plot_generator.map_location_time_of_the_day(df.copy())))
    elif task_id == 3:
        result_list.append((task_id, plot_generator.distance_travelled_intraday(df.copy())))
    elif task_id == 4:
        result_list.append((task_id, plot_generator.inter_sample_distance_location(df.copy())))
    # activity plotting tasks
    elif task_id == 5:
        result_list.append((task_id, plot_generator.plot_activity_type_time_of_day(df.copy())))
    elif task_id == 6:
        result_list.append((task_id, plot_generator.update_interval_histogram(
            df.copy(), "Activity Update Interval (minutes)")))
    elif task_id == 7:
        result_list.append((task_id, plot_generator.map_activity_motion_group(df.copy())))
    elif task_id == 8:
        result_list.append((task_id, plot_generator.intersample_dist_activity(df.copy())))


def get_plotting_json(graph_mode, cache_mode, device_id, pid, start_date, end_date):
    if not cache_mode:
        # Now prepare to run the data analysis pipelines.
        print("RUNNING", graph_mode)
        df = df_prep_helper("location", device_id, start_date, end_date)
        manager = mp.Manager()
        result_list = manager.list()
        process_count = os.cpu_count()
        start = time.time()
        pool = mp.Pool(process_count)
        print("starting a pool with", process_count, "processes")
        if graph_mode == 'location':
            # location plotting task_id: 0-4
            if len(df) >= 5:
                for task_id in range(5):
                    pool.apply_async(multiprocessing_pool_helper, args=(task_id, df, result_list))
                pool.close()
                pool.join()

                result_list.sort()
                graphs = [item[1] for item in result_list]
            else:
                graphs = [[] for _ in range(5)]
        else:
            df_activity = df_prep_helper("activity", device_id, start_date, end_date)
            if len(df) >= 5 and len(df_activity) >= 5:
                df_merged = plot_generator.aggregate_location_and_activity_by_distance(
                    df.copy(), df_activity.copy(), frequency_setting='10Min')

                # activity plotting task_id: 5-8
                for task_id in range(5, 9):
                    if task_id >= 7:
                        pool.apply_async(multiprocessing_pool_helper, args=(task_id, df_merged, result_list))
                    else:
                        pool.apply_async(multiprocessing_pool_helper, args=(task_id, df_activity, result_list))
                pool.close()
                pool.join()

                result_list.sort()

                graphs = [item[1] for item in result_list]
            else:
                graphs = [[] for _ in range(4)]

        # Add "ids" to each of the graphs to pass up to the client
        # for templating
        ids = ['graph-{}'.format(i) for i, _ in enumerate(graphs)]

        # Convert the figures to JSON
        # PlotlyJSONEncoder appropriately converts pandas, datetime, etc
        # objects to their JSON equivalents
        graph_json = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
        # print("!!!about to return", graph_json)

        with open('graph_json_{}.json'.format(graph_mode), 'w') as outfile:
            json.dump(graph_json, outfile)
        with open('ids_{}.json'.format(graph_mode), 'w') as outfile:
            json.dump(ids, outfile)
        print("save cache OK")
        end = time.time()
        print("computation time required with", process_count, "processes:", end - start, "seconds")
    else:
        # The cache mode is True, now reading from local cache.
        with open('graph_json_{}.json'.format(graph_mode)) as json_file:
            graph_json = json.load(json_file)
        with open('ids_{}.json'.format(graph_mode)) as json_file:
            ids = json.load(json_file)
        print("read cache OK")
    return graph_json, ids


# generate the interactive graphs with various parameters.
# graph_mode: can be "location" or "activity". Returns the graphs with the respective type.
# cache_mode: if True, reads in the local json file for quick demo graphs. If False, it runs the entire data analysis
# pipeline from scratch.
# pid: the current pid to be displayed in the iframe. However since we are moving my graphs to Angular, then this is
# no longer a necessary variable.
def index(graph_mode, cache_mode, device_id, pid, start_date, end_date):
    device_id = '003623b2-ea08-4596-a8ad-6393a5c2843e' if device_id is None else device_id
    start_date = '2019-11-09' if start_date is None else start_date
    end_date = '2019-11-09' if end_date is None else end_date
    global device_id_list
    if len(device_id_list) == 0:
        device_id_list = download_device_id_list()[0]
    date_list = get_date_list_helper(device_id)
    graph_json, ids = get_plotting_json(graph_mode, cache_mode, device_id, pid, start_date, end_date)

    # dummy values below just to showcase current design.
    pid_arr = [123, 124, 125, 126]
    return render_template('layouts/index.html',
                           ids=ids,
                           graphJSON=graph_json,
                           title="'{}'".format(pid),
                           pid_arr=pid_arr,
                           device_id_arr=device_id_list,
                           date_arr=date_list)
