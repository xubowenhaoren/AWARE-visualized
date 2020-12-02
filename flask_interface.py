import sys
from flask import Flask,render_template, request, jsonify
import display_panel_pipeline, df_clean_helper
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
app.debug = True
CORS(app)

cache_mode = False


@app.route('/')
def index():
    return "Visit http://localhost:12345/location or http://localhost:12345/activity"


@app.route('/GetDeviceIdList', methods=['GET'])
def get_device_id_list():
    device_id_list, android_list, ios_list = display_panel_pipeline.download_device_id_list()
    return jsonify(
        dict(device_id_list=[{'device_id': x} for x in device_id_list], android_list=android_list, ios_list=ios_list))


# YR: This api does not work with some device ids selected from the device id list, please debug it
@app.route('/GetDateList', methods=['POST'])
def get_date_list():
    request_body = request.form
    
    device_id = request_body['device_id']
    slice_freq = '1D'
    try:
        slice_freq = request_body['slice_freq']
    except KeyError:
        pass
    date_list = display_panel_pipeline.get_date_list_helper(device_id, slice_freq)
    return jsonify(dict(date_list=[{'date': x} for x in date_list]))


# YR: This api does not work, please debug it
@app.route('/GetDeviceIdAndDateList', methods=['POST'])
def get_device_and_date_list():
    device_id_list, android_list, ios_list = display_panel_pipeline.download_device_id_list()
    request_body = request.form
    slice_freq = '1D'
    try:
        slice_freq = request_body['slice_freq']
    except KeyError:
        pass
    response = {}
    for device_id in device_id_list:
        response[device_id] = display_panel_pipeline.get_date_list_helper(device_id, slice_freq)

    return jsonify(response)


def get_params():
    pid, device_id, start_date, end_date = \
        request.form.get('pid'), \
        request.form.get('device_id'), \
        request.form.get('start_date'), \
        request.form.get('end_date')
    return pid, device_id, start_date, end_date


"""
The following methods are disabled because they are no longer in use. The interactive visualization
has been moved to the Angular frontend.
"""
# Generates the location graphs
@app.route('/location', methods=['GET', 'POST'])
def get_location():
    pid, device_id, start_date, end_date = get_params()
    return display_panel_pipeline.index('location', cache_mode, device_id, str(pid), start_date, end_date)


# Generates the activity graphs
@app.route('/activity', methods=['GET', 'POST'])
def get_activity():
    pid, device_id, start_date, end_date = get_params()
    return display_panel_pipeline.index('activity', cache_mode, device_id, str(pid), start_date, end_date)


# This method assumes for valid device_id + date combo.
@app.route('/GetPlot', methods=['POST'])
def get_plot():
    request_body = request.form
    device_id = request_body['device_id']
    start_date = request_body['start_date']
    end_date = request_body['end_date']
    graph_mode = request_body['graph_mode']
    graph_json, ids = display_panel_pipeline.get_plotting_json(graph_mode, cache_mode, device_id, 'pid', start_date, end_date)
    return graph_json


if __name__ == '__main__':
    if len(sys.argv) != 2 or (sys.argv[1] not in ['true', 'false']):
        print("usage: python flask_interface.py CACHE_MODE_TRUE_OR_FALSE")

    cache_mode = sys.argv[1].lower() == 'true'
    app.run(host='localhost', port=12345)
