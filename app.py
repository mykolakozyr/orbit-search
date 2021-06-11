import streamlit as st
import numpy as np
import geojson
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import shape

from streamlit_folium import folium_static 
import folium

import requests
import json
from requests.auth import HTTPBasicAuth
import time
from dateutil.relativedelta import relativedelta
import datetime as dt


# DATE COMPONENT
st.sidebar.image('data/UP42_Logo_PlanetBlue_RGB.png')
# First Acquisition
st.sidebar.header('Select first acquisition')
first_defaultDate = dt.date(2020,12,1)
first_minDate = dt.date(2020,10,1)
first_min_date = st.sidebar.date_input("Start of first acquisition", first_defaultDate, first_minDate)
first_max_date = st.sidebar.date_input("End of first acquisition", first_defaultDate + dt.timedelta(days=14), first_minDate + dt.timedelta(days=14))
#st.write(first_min_date, first_max_date)

# Second Acquisition
st.sidebar.header('Select second acquisition')
second_defaultDate = dt.date(2021,6,1)
second_minDate = dt.date(2020,10,15)
second_min_date = st.sidebar.date_input("Start of second acquisition", second_defaultDate, second_minDate)
second_max_date = st.sidebar.date_input("End of second acquisition", second_defaultDate + dt.timedelta(days=14), second_minDate + dt.timedelta(days=14))
#st.write(second_min_date, second_max_date)

# average difference between two requested acquisitions
average_diff = (((second_max_date - first_max_date) + (second_min_date - first_min_date))/2).days
#st.write(average_diff)


# GEOMETRY COMPONENT
st.sidebar.header('Define Area of Interest')
#aoi = st.sidebar.text_input("GeoJSON")
uploaded_file = st.sidebar.file_uploader("Upload a GeoJSON file of the Area of Interest",type='geojson')


# SEARCH COMPONENT
# Defining UP42 credentials
backend_url = st.secrets["backend_url"]
project_id = st.secrets["project_id"]
project_api_key = st.secrets["project_api_key"]


# MAIN INFORMATION
# Display header.
st.markdown("<br>", unsafe_allow_html=True)

"""
# Search for scenes to run Ground Displacement analytics on UP42
"""
st.image('https://metadata.up42.com/23de425c-7954-45fb-84c5-f75abe00e2d2/1360_752_Ground_Displacement_CATALYST_Image11623092560633.jpg')
st.markdown("<br>", unsafe_allow_html=True)
"""
The [CATALYST](https://catalyst.earth/platform-analytics/deformation-maps/) Ground Displacement processing block makes use of repeat pass Sentinel-1 SLC images to calculate cumulative displacement using sequential InSAR processing techniques (A-B, B-C, C-D, etc) based on the number of images requested.

Output products generated include cumulative displacement and velocity measurements (relative to first image in the stack). A quality measure is also generated (average coherence) as well as reference backscatter image. All results are orthorectified, and measured in relative units, in the line of sight of the sensor).

## Comlpexity

To run a Ground Displacement analytics on UP42 you have to provide two scenes coming from Sentinel-1 L1 SLC data. 
The complexity of the case is that both of scenes should have the **same orbit relative number**.

## Preconditions
1. Access to [**Ground Displacement block**](https://marketplace.up42.com/block/23de425c-7954-45fb-84c5-f75abe00e2d2).

2. Workflow created on UP42 Platform: **Sentinel-1 L1 SLC (SAFE) - Ground Displacement**
---
"""



# Get token
def get_token():
  payload = {'grant_type': 'client_credentials'}

  response = requests.post(backend_url + '/oauth/token', 
                          headers = {"Content-Type" : "application/x-www-form-urlencoded"},
                          data = payload,
                          auth=HTTPBasicAuth(project_id, project_api_key))

  accessToken = response.json()['data']['accessToken']
  return accessToken

# Defining search body
def search(min_date, max_date, geometry):
  search =  '''
      {
          "datetime": "'''+str(min_date)+"T00:00:00Z/"+str(max_date)+'''T00:00:00Z",
          "intersects": '''+ str(geometry) +''', 
          "limit": 500,
          "query":{"dataBlock":{"in":[ "sobloo-s1-slc-fullscene" ]}}}
      '''
  return (search)

# Run search and return geodataframe of search responses
def run_search(min_date, max_date):
    # DataFrame to collect Search results
    search_results = pd.DataFrame(columns=['geometry','id','scene_id','acquisitionDate','collection','orbit','orbit_dir']) 
    search_results['geometry'] = search_results['geometry'].apply(wkt.loads)
    s1 = gpd.GeoDataFrame(search_results, geometry='geometry')

    accessToken = get_token()
    response = requests.post(backend_url + '/catalog/stac/search', 
                                headers = {
                                    "Authorization" : "Bearer " + accessToken,
                                    "Content-Type" : "application/json"},
                                data = search(min_date, max_date, aoi))

    data = response.json()
    data = json.loads(response.text)
    s1_data = data['features']

    for item in s1_data:
      geometry = shape(item['geometry'])
      id = item['properties']['id']
      scene_id = item['properties']['sceneId']
      acquisitionDate = item['properties']['acquisitionDate']
      collection = item['properties']['collection']
      orbit = item['properties']['providerProperties']['orbit']['relativeNumber']
      orbit_dir = item['properties']['providerProperties']['orbit']['direction']

      s1 = s1.append({
          'geometry': geometry,
          'id': id,
          'scene_id': scene_id,
          'acquisitionDate': acquisitionDate,
          'collection': collection,
          'orbit': orbit,
          'orbit_dir': orbit_dir,
          }, ignore_index=True)
    return(s1)


def return_matches(requested_diff):
    ac_1 = run_search(first_min_date, first_max_date)
    ac_2 = run_search(second_min_date, second_max_date)

    df = pd.merge(ac_1, ac_2, on='orbit' )
    df['acquisitionDate_x'] = pd.to_datetime(df.acquisitionDate_x)
    df['acquisitionDate_y'] = pd.to_datetime(df.acquisitionDate_y)
    df['scenes_acq_diff'] = (df['acquisitionDate_y'] - df['acquisitionDate_x']).astype('timedelta64[D]')
    df['prio_diff'] = abs(df['scenes_acq_diff'] - average_diff)
    
    df = df.sort_values(by='prio_diff')
    return(df)
    st.write(df.head())
    st.write(df.shape)

# Defining Job Configuration
def job_config(scene1, scene2, aoi):
    config = '''
    {
      "sobloo-s1-slc-fullscene:1": {
        "ids": ["'''+scene1+'''", "'''+scene2+'''"],
        "time": "2018-01-01T00:00:00+00:00/2021-12-31T23:59:59+00:00",
        "limit": 1,
        "time_series": null,
        "mission_code": null,
        "orbit_direction": null,
        "acquisition_mode": null,
        "orbit_relative_number": null,
        "intersects": '''+ str(aoi) +'''
      },
      "catalystpro-insstack:1": {
        "aoi_bbox": null,
        "aoi_geojson": '''+ str(aoi) +'''
      }
    }
    '''
    return(config)


st.markdown("<br>", unsafe_allow_html=True)
"""
# Search Parameters

You are preparing to search over the following AOI and acquisition parameters
"""

cols = st.beta_columns((1,1)) # number of columns in each row! = 2
cols[0].subheader('First Acquisition Range')
cols[0].write(str(first_min_date) + ' - ' + str(first_max_date))

cols[1].subheader('Second Acquisition Range')
cols[1].write(str(second_min_date) + ' - ' + str(second_max_date))


#Logic to generate search requests
if uploaded_file is not None:
    st.markdown("<br>", unsafe_allow_html=True)
    """
    ## Geometry
    """


    # To read file as bytes:
    gj = geojson.load(uploaded_file)
    aoi = gj['features'][0]['geometry']


    map_aoi = folium.Map(tiles="OpenStreetMap")
    folium.Choropleth(geo_data = gj,reset=True).add_to(map_aoi)
    map_aoi.fit_bounds(map_aoi.get_bounds())
    folium_static(map_aoi)

    st.markdown("<br>", unsafe_allow_html=True)
    """
    You're almost ready! :rocket: :rocket: :rocket: 

    Click on the "Run search button in the sidebar to proceed."
    """


    if st.sidebar.button('Run search'):
        with st.spinner('Wait for it...'):
            time.sleep(5)

        df = return_matches(average_diff)
        st.title('Search results')

        cols = st.beta_columns((1,3,3,5)) # number of columns in each row! = 2
        cols[0].subheader('Orbit')
        cols[1].subheader('First Acquisition Image')
        cols[2].subheader('Second Acquisition Image')
        cols[3].subheader('Job Configuration')

        num_results = 0
        if df.shape[0] == 0:
            st.warning('Search returned zero results, please change the search request')
        elif df.shape[0] <= 5:
            num_results = df.shape[0]
        else:
            num_results = 6

        for i in range(0, num_results): # number of rows in your table! = 2
            cols = st.beta_columns((1,3,3,5)) # number of columns in each row! = 2
            # first column of the ith row
            result = df.iloc[[i]]

            first_image_id = result['id_x'].values[0]
            second_image_id = result['id_y'].values[0]

            orbit = result['orbit'].values[0]
            first_id = result['scene_id_x'].str[:255].values[0]
            second_id = result['scene_id_y'].str[:255].values[0]
            first_acquisition = result['acquisitionDate_x'].values[0]
            second_acquisition = result['acquisitionDate_y'].values[0]
            config = job_config(first_id, second_id, aoi)

            cols[0].text(orbit)
            cols[1].text(first_acquisition)
            cols[1].image('https://sobloo.eu/api/v1/services/quicklook/' + str(first_image_id), use_column_width=True) 
            cols[2].text(second_acquisition)
            cols[2].image('https://sobloo.eu/api/v1/services/quicklook/' + str(second_image_id), use_column_width=True) 
            cols[3].text(config)
            #cols[3].text(second_acquisition, use_column_width=True 
    

uploaded_file = st.sidebar.empty()
aoi = st.sidebar.empty()