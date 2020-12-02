import datetime
import sys

import geopy.distance
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, HTML
import json

from flask import Flask, render_template

import json
import plotly
import plotly.graph_objects as go

from collections import Counter

from plotly.subplots import make_subplots

import df_clean_helper


# Note: I may use dataframe and df interchangeably in the comments.


# A helper function to pretty-print the df inside Jupyter Notebook. This has nothing to do with the visualization.
# # Prints 5 rows by default.
def pprint(df, rows=5):
    display(HTML(df[:rows].to_html()))


# Calculate the update interval for a df and return a histogram plot. Supports both location and activity df.
def update_interval_histogram(df, text):
    df['timestamp_shift'] = df.timestamp.shift(1)
    # removing first record
    df.drop(df.index[0], inplace=True)
    df['update_interval'] = (df.timestamp - df.timestamp_shift)

    print("the average difference in update_interval is", df.update_interval.mean())
    update_interval_minutes = (df['update_interval'].astype('timedelta64[s]') / 60)
    go_figure = \
        go.Histogram(
            x=update_interval_minutes,
            xbins=dict(  # bins used for histogram
                start=0.0,
                end=12.5,
                size=1.0
            ))
    fig = go.Figure()
    fig.add_trace(go_figure)
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0), title={'text': text})
    # fig.show()
    return fig


# A helper method to assign data rows with different timestamp delta to "groups" of update intervals.
# group 1 = update interval less than 2 minutes. Group 2 = 2 - 12 minutes. Group 3 = 12 minutes and beyond.
def interval_to_group(row):
    time_delta_1 = pd.to_timedelta('0 days 00:02:00.00000')
    time_delta_2 = pd.to_timedelta('0 days 00:12:00.00000')
    if row['update_interval'] < time_delta_1:
        return "group 1"
    elif row['update_interval'] < time_delta_2:
        return "group 2"
    else:
        return "group 3"


# This method prepares the data for the location update interval plot.
def map_inter_sample_interval_helper(df):
    res = []
    lookup_dict = {'group 1': '0 - 2 min', 'group 2': '2 - 12 min', 'group 3': '12 min+'}
    legend_status = {'group 1': True, 'group 2': True, 'group 3': True}
    df_groups = df.groupby(pd.Grouper(freq='1H', key='timestamp'))
    for name, df_slice in df_groups:
        for i in range(1, 4):
            group_str = "group " + str(i)
            df_temp = df_slice[df_slice['interval_group'] == group_str]
            lat_list = df_temp['double_latitude']
            long_list = df_temp['double_longitude']
            m_color = "green"
            if i == 2:
                m_color = "orange"
            if i == 3:
                m_color = "red"
            temp = go.Scattermapbox(
                lat=lat_list,
                lon=long_list,
                mode='markers+lines' if i == 1 else 'markers',
                marker=go.scattermapbox.Marker(
                    size=12,
                    color=m_color,
                ),
                legendgroup=lookup_dict[group_str],
                name=lookup_dict[group_str],
                showlegend=True
            )
            if legend_status[group_str] and len(df_temp) > 0:
                legend_status[group_str] = False
            else:
                temp.showlegend = False
            res.append(temp)
    return res


# This helper method generates map visualization plots; it takes a mode parameter, calls the respective data analysis
# helper methods to prepare data, and returns the respective figure object for rendering.
def graph_scatter_plot(df, mode):
    mapbox_access_token = "pk.eyJ1IjoiYm93ZW54dTA3NCIsImEiOiJjazVscW16" \
                          "cXYwYWdqM2pwdmhud2M3YTJyIn0.Lx6e9wfiRu4H1QGjaue_qA"

    lat_list = df['double_latitude']
    long_list = df['double_longitude']
    # print(lat_list)
    center = lat_list.iloc[0], long_list.iloc[0]
    res = []
    if mode == 'Map - Location - Update Interval (in minutes)':
        res = map_inter_sample_interval_helper(df)
    elif mode == 'Map - Activity Motion Group':
        res = map_activity_type(df)
    else:
        res = map_time_of_day_helper(df)

    fig = go.Figure(res)

    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0),
                      hovermode='closest',
                      mapbox=go.layout.Mapbox(
                          accesstoken=mapbox_access_token,
                          bearing=0,
                          center=go.layout.mapbox.Center(
                              lat=center[0],
                              lon=center[1]
                          ),
                          style='mapbox://styles/mapbox/streets-v11',
                          pitch=0,
                          zoom=13
                      ),
                      title={'text': mode}
                      )
    return fig


# This is a method that graphs a location df by the inter sample interval.
# e.g. 5 minute update interval is group 2, yellow.
def map_location_inter_sample_interval(df):
    df['timestamp_shift'] = df.timestamp.shift(1)
    # removing first record
    df.drop(df.index[0], inplace=True)
    df['update_interval'] = (df.timestamp - df.timestamp_shift)
    df['interval_group'] = df.apply(lambda row: interval_to_group(row), axis=1)
    # df['date'] = df.apply(lambda row: df_clean_helper.to_date(row), axis=1)
    # first_date = df.iloc[0]['timestamp'].date()
    last_date = df.iloc[-1]['timestamp'].date()
    # time_diff = int(str(last_date - first_date).split(" ")[0])

    # print("first date:", first_date, "last date:", last_date)
    return graph_scatter_plot(df, "Map - Location - Update Interval (in minutes)")


# This is a helper method that returns the time of the day (e.g. morning, afternoon, night, late night).
def get_time_period(row):
    night_to_morning = datetime.time(6, 0, 0, 0)
    noon = datetime.time(11, 59, 59, 0)
    afternoon = datetime.time(18, 0, 0, 0)
    if row['time'] > afternoon:
        return "night"
    elif row['time'] > noon:
        return "afternoon"
    elif row['time'] > night_to_morning:
        return "morning"
    else:
        return "late night"


# This is a helper method that prepares the data for the "Map - Location - Time Of Day".
def map_time_of_day_helper(df):
    res = []
    time_of_day = ['morning', 'afternoon', 'night', 'late night']
    df = df_get_date(df)
    legend_status = {item: True for item in time_of_day}
    color = ['green', 'orange', 'blue', 'red']
    df_groups = df.groupby(pd.Grouper(freq='1H', key='timestamp'))
    for name, df_slice in df_groups:
        for i in range(len(time_of_day)):
            group_str = time_of_day[i]
            df_temp = df_slice[df_slice['time_of_day'] == group_str]
            lat_list = df_temp['double_latitude']
            long_list = df_temp['double_longitude']
            m_color = color[i]
            temp = go.Scattermapbox(
                lat=lat_list,
                lon=long_list,
                mode='markers+lines',
                marker=go.scattermapbox.Marker(
                    size=12,
                    color=m_color
                ),
                name=time_of_day[i],
                legendgroup=time_of_day[i],
                showlegend=True
            )
            if legend_status[time_of_day[i]] and len(df_temp) > 0:
                legend_status[time_of_day[i]] = False
            else:
                temp.showlegend = False
            res.append(temp)
    return res


# This is a helper method that prepares the data for "Map - Activity Motion Group".
def map_activity_type(df_merged):
    res = []
    legend_status = {item: True for item in df_merged['activity_mode'].unique()}
    color_list = ['green', 'orange', 'blue', 'red', 'black', 'purple', 'yellow']
    df_groups = df_merged.groupby(pd.Grouper(freq='1H', key='timestamp'))
    for name, df_slice in df_groups:
        for i, activity_type in enumerate(df_merged['activity_mode'].unique()):
            df_temp = df_slice[df_slice['activity_mode'] == activity_type]
            lat_list = df_temp['double_latitude']
            long_list = df_temp['double_longitude']
            temp = go.Scattermapbox(
                lat=lat_list,
                lon=long_list,
                mode='markers+lines',
                marker=go.scattermapbox.Marker(
                    size=12,
                    color=color_list[i]
                ),
                name=activity_type,
                legendgroup=activity_type,
                showlegend=True
            )
            if legend_status[activity_type] and len(df_temp) > 0:
                legend_status[activity_type] = False
            else:
                temp.showlegend = False
            res.append(temp)
    return res


# Graphs a location df by the time of the day.
# (e.g. 12AM - 6AM, 6AM - 12PM) Note: this is not included in the final design.
def map_location_time_of_the_day(df):
    return graph_scatter_plot(df, "Map - Location - Time Of Day")


# Graphs an activity df on a map based on their motion groups. See the screenshot.
def map_activity_motion_group(df):
    return graph_scatter_plot(df, "Map - Activity Motion Group")


# A helper to calculate the distance based on the lat and long of the data points.
def calculate_distance(row):
    first_coord = (row['double_latitude'], row['double_longitude'])
    last_coord = (row['double_latitude_shift'], row['double_longitude_shift'])

    return geopy.distance.distance(first_coord, last_coord).m


# Helper function for inter_sample_distance(). Calculate the inter sample distance for a df.
def inter_sample_distance_helper(df, min_dist=0.0, max_dist=1000.0):
    df['double_latitude_shift'] = df.double_latitude.shift(1)
    df['double_longitude_shift'] = df.double_longitude.shift(1)
    df.drop(df.index[0], inplace=True)
    df['distance'] = df.apply(lambda row: calculate_distance(row), axis=1)
    return df


# A helper function used by inter_sample_distance().
# Calculate the inter sample distance for a location df and return a histogram plot.
def inter_sample_distance_location(df, min_dist=0.0, max_dist=1000.0):
    df = inter_sample_distance_helper(df, min_dist, max_dist)
    go_figure = \
        go.Histogram(
            x=df['distance'],
            xbins=dict(  # bins used for histogram
                start=min_dist,
                end=max_dist,
                size=100.0
            )
        )
    fig = go.Figure()
    fig.add_trace(go_figure)
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0),
                      title={'text': "Inter-sample distance (Location Table, m)"},
                      xaxis_title="Total number of records: " + str(len(df)))
    return fig


# Calculate the distance travelled within specific time intervals of one day and return a histogram.
def distance_travelled_intraday(df):
    # Example time intervals: 12AM - 6AM, 6AM - 12PM, 12PM - 6PM, 6PM - 12AM.
    time_of_day = ['morning', 'afternoon', 'night', 'late night']
    distances = []
    df = df_get_date(df)
    first_date = df.iloc[0]['timestamp'].date()
    last_date = df.iloc[-1]['timestamp'].date()
    df = inter_sample_distance_helper(df)
    for i in range(len(time_of_day)):
        df_temp = df[df['time_of_day'] == time_of_day[i]]
        distances.append(df_temp['distance'].sum())
    # print(distances)
    fig = go.Figure([go.Bar(x=time_of_day, y=distances)])
    # fig.show()
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0),
                      title={'text': "Distance Travelled Within a Day (meters)"})
    return fig


# A helper method to get the date based on the timestamp.
def df_get_date(df):
    df['time'] = df['timestamp'].dt.time
    # df['date'] = df.apply(lambda row: df_clean_helper.to_date(row), axis=1)
    df['time_of_day'] = df.apply(lambda row: get_time_period(row), axis=1)
    return df


# A helper method to generate the bar chart for "Activity type by the time of the day".
def plot_activity_type_time_of_day(df):
    time_of_day = ['morning', 'afternoon', 'night', 'late night']
    df = df_get_date(df)
    total_count_arr = []
    activity_column_name = 'activities' if 'activities' in df.columns else 'activity_name'
    for activity_type in df[activity_column_name].unique():
        df_temp = df[df[activity_column_name] == activity_type]
        # print("activity", activity_type)
        # sub_count_arr stores the sub count for activity_type corresponding to time_of_day
        sub_count_arr = []
        for i in range(len(time_of_day)):
            # print("time", time_of_day[i])
            df_time = df_temp[df_temp[activity_column_name] == time_of_day[i]]
            # print(activity_type, len(df_time))
            sub_count_arr.append(len(df_time))
        total_count_arr.append(sub_count_arr)
    fig = go.Figure(data=[
        go.Bar(name=df[activity_column_name].unique()[i],
               x=time_of_day,
               y=total_count_arr[i])
        for i in range(len(df[activity_column_name].unique()))
    ])
    # Change the bar mode
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0), barmode='group')
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0),
                      title={'text': "Activity type by the time of the day"})
    #     fig.show()
    return fig


# Returns whether the merged activity list is too short (only 1 or 0 items)
def is_activity_list_too_short(row):
    if len(str(row['activities'])) < 2:
        return True
    return False


# Return the most common activity based on the activity list of a merged dataframe.
# The activity df here is merged with the location df based on an 5-15 minute interval to obtain the
# best chances of matching with the location df.
def calculate_activity_mode(row):
    activity_list = row['activities'].split('"]["')
    activity_list[0] = activity_list[0][2:]
    activity_list[-1] = activity_list[-1][:-2]
    most_common, num_most_common = Counter(activity_list).most_common(1)[0]
    return most_common


# Same purpose but just for android
def is_activity_list_too_short_android(row):
    if len(str(row['activity_name'])) <= 2:
        return True
    return False


def add_comma(row):
    return row['activity_name'] + ", "


# Same purpose but just for android
def calculate_activity_mode_android(row):
    activity_list = row['activity_name'].split(', ')
    activity_list.pop()
    most_common, num_most_common = Counter(activity_list).most_common(1)[0]
    return most_common


# A helper method to decided whether the current row is worth keeping.
# Note: to calculate the distance for the activity-location merged df, we need to shift the lat and long columns by one.
# This creates an activity type mismatch for some rows of data.
def keep_curr_row(row):
    return row['activity_mode'] == row['activity_mode_shift']


# Merges the actvitiy df and the location df by a timely basis parameter. We need this to map the activity df
# with an actual Earth coordinate so that we can plot activity data on a map.
def aggregate_location_and_activity_by_distance(df_location, df_activity, frequency_setting='5Min'):
    print("running bad data aggregation with: ")
    print("- frequency setting:", frequency_setting)
    df_location_agg = df_location.groupby(pd.Grouper(freq=frequency_setting, key='timestamp')).aggregate(
        {"double_latitude": "mean", "double_longitude": "mean", "double_altitude": "mean"}).reset_index()

    if "activities" in df_activity.columns:
        # ios
        df_activity_agg = df_activity.groupby(pd.Grouper(freq=frequency_setting, key='timestamp')).aggregate(
            {"activities": "sum"}).reset_index()
        df_activity_agg['drop_this_row'] = df_activity_agg.apply(lambda row: is_activity_list_too_short(row), axis=1)
        df_activity_agg = (df_activity_agg[df_activity_agg['drop_this_row'] == False]).copy()
        df_activity_agg['activity_mode'] = df_activity_agg.apply(lambda row: calculate_activity_mode(row), axis=1)
        df_activity_agg = df_activity_agg.drop(columns=["drop_this_row", "activities"])
    else:
        # android
        df_activity['activity_name'] = df_activity.apply(lambda row: add_comma(row), axis=1)
        df_activity_agg = df_activity.groupby(pd.Grouper(freq=frequency_setting, key='timestamp')).aggregate(
            {"activity_name": "sum"}).reset_index()
        df_activity_agg['drop_this_row'] = df_activity_agg.apply(lambda row: is_activity_list_too_short_android(row),
                                                                 axis=1)
        df_activity_agg = (df_activity_agg[df_activity_agg['drop_this_row'] == False]).copy()
        df_activity_agg['activity_mode'] = df_activity_agg.apply(lambda row: calculate_activity_mode_android(row),
                                                                 axis=1)
        df_activity_agg = df_activity_agg.drop(columns=["drop_this_row", "activity_name"])
    df_merged = pd.merge(df_location_agg, df_activity_agg, on="timestamp", how='outer')
    df_merged.dropna(inplace=True)
    df_merged['double_latitude_shift'] = df_merged.double_latitude.shift(1)
    df_merged['double_longitude_shift'] = df_merged.double_longitude.shift(1)
    df_merged['activity_mode_shift'] = df_merged.activity_mode.shift(1)

    df_merged.drop(df_merged.index[0], inplace=True)
    df_merged['keep_curr_row'] = df_merged.apply(lambda row: keep_curr_row(row), axis=1)
    df_merged = df_merged[df_merged['keep_curr_row'] == True].copy()
    df_merged.drop(['keep_curr_row'], axis=1, inplace=True)
    df_merged['distance'] = df_merged.apply(lambda row: calculate_distance(row), axis=1)
    return df_merged


# A helper to draw multiple traces on the same figure (plot).
def add_trace_helper(fig, trace_arr, cols=2):
    row = 0
    col = 1
    for i in range(len(trace_arr)):
        if i % cols == 0:
            col = 1
            row += 1
        else:
            col += 1
        # print("about to add", row, col)
        fig.append_trace(trace_arr[i], row, col)


# A helper method to generate the figure for "Intersample distance by activity type (m)"
def intersample_dist_activity(df_merged, min_dist=0.0, max_dist=1000.0, num_cols=2):
    trace_arr = []
    for activity_type in df_merged['activity_mode'].unique():
        df_temp = df_merged[df_merged['activity_mode'] == activity_type]
        go_figure = go.Histogram(
            x=df_temp['distance'],
            xbins=dict(  # bins used for histogram
                start=min_dist,
                end=max_dist,
                size=100.0
            ),
            name=activity_type
        )
        trace_arr.append(go_figure)
    fig = make_subplots(rows=max(len(df_merged['activity_mode'].unique()) // num_cols, 2), cols=num_cols)
    add_trace_helper(fig, trace_arr, num_cols)
    fig.update_layout(autosize=True, margin=dict(l=0, r=0, t=0, b=0),
                      title={'text': "Intersample distance by activity type (m)"})
    #     fig.show()
    return fig
