import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import requests
import lasio

# Setup AWS S3 connection
@st.cache_resource
def make_client_resource():
    client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    return client

client = make_client_resource()

# Load metadata
@st.cache_data
def load_metadata(bucket, file_key):
    obj = client.get_object(Bucket=bucket, Key=file_key)
    return pd.read_csv(obj['Body'])

# Load curves data
bucket_for_metadata = "for-metadata"
list_metadata_files = ['List_of_curves.csv', 'List_of_data-new.csv']

curves_data = load_metadata(bucket_for_metadata, list_metadata_files[0])
table_data = load_metadata(bucket_for_metadata, list_metadata_files[1])

# Map configuration
st.title("GDS-Viewer Dashboard")
map_token = "pk.eyJ1IjoieXVyaXlrYXByaWVsb3YiLCJhIjoiY2t2YjBiNXl2NDV4YzJucXcwcXdtZHVveiJ9.JSi7Xwold-yTZieIc264Ww"
px.set_mapbox_access_token(map_token)

wells_map = curves_data.drop_duplicates(subset=['lat', 'lon']).reset_index(drop=True)

# Sidebar for Geologic Time
geo_times = st.sidebar.multiselect("Geologic Time", options=table_data['Geological_Time'].unique(), default=table_data['Geological_Time'].unique())

# Filter wells based on selected Geologic Time
wells_map['intersection_time'] = wells_map['Age'].apply(lambda x: 0 if set(x.split('_')).intersection(set(geo_times))==set() else 1)
filtered_wells = wells_map[wells_map['intersection_time'] != 0]

# Map Visualization
fig_map = px.scatter_mapbox(filtered_wells, lat="lat", lon="lon", hover_name="Name", zoom=4, height=600, mapbox_style="satellite")
st.plotly_chart(fig_map, use_container_width=True)

# Selecting wells from map
st.subheader("Selected Wells")
selected_wells = st.multiselect("Select wells:", filtered_wells['Name'].tolist())

if selected_wells:
    selected_data = curves_data[curves_data['Name'].isin(selected_wells)]
    st.dataframe(selected_data)

    # Visualizing curves
    for well in selected_wells:
        well_data = selected_data[selected_data['Name'] == well]
        type_curve = well_data['Type'].iloc[0]

        # Load curve data from AWS
        bucket_for_visualization = "transformed-for-visualization-data-1"
        file_prefix = 'csv/' + type_curve
        keys_log = [obj['Key'] for obj in client.list_objects_v2(Bucket=bucket_for_visualization, Prefix=file_prefix)['Contents']]
        path_file = next((key for key in keys_log if type_curve in key), None)

        if path_file:
            curve_obj = client.get_object(Bucket=bucket_for_visualization, Key=path_file)
            curve_df = pd.read_csv(curve_obj['Body'])
            curve_df = curve_df[(curve_df['Well_name'] == well)]

            fig_curve = go.Figure()
            fig_curve.add_trace(go.Scatter(x=curve_df[curve_df.columns[1]], y=curve_df['DEPTH'], mode='lines', name=type_curve))
            fig_curve.update_layout(title=f"{type_curve} Curve for {well}", yaxis_autorange='reversed', height=500)
            st.plotly_chart(fig_curve, use_container_width=True)

    # Download LAS files
    st.subheader("Download LAS Files")
    bucket_for_download = "transformed-for-download-data"
    Keys_las = [obj['Key'] for obj in client.list_objects_v2(Bucket=bucket_for_download, Prefix='las/')['Contents']]

    for _, row in selected_data.iterrows():
        name = f"{row['lat']}_{row['lon']}_{row['Depth_start']:.1f}_{row['Depth_finish']:.1f}_{row['Name']}"
        las_key = next((key for key in Keys_las if name in key), None)

        if las_key:
            url = client.generate_presigned_url('get_object', Params={'Bucket': bucket_for_download, 'Key': las_key})
            response = requests.get(url)
            st.download_button(label=f"Download LAS file for {row['Name']}", data=response.content, file_name=f"{name}.las")
        else:
            st.warning(f"LAS file not found for {row['Name']}")
