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

# This is the event‐capture helper:
from streamlit_plotly_events import plotly_events

# --------------------------------------------------
# 1) AWS S3 CLIENT SETUP
# --------------------------------------------------
@st.cache_resource
def make_client_resource():
    client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    return client

client = make_client_resource()

# --------------------------------------------------
# 2) LOAD METADATA FROM S3
# --------------------------------------------------
@st.cache_data
def load_metadata(bucket: str, file_key: str) -> pd.DataFrame:
    obj = client.get_object(Bucket=bucket, Key=file_key)
    return pd.read_csv(obj['Body'])

bucket_for_metadata = "for-metadata"
list_metadata_files = ['List_of_curves.csv', 'List_of_data-new.csv']

curves_data = load_metadata(bucket_for_metadata, list_metadata_files[0])
table_data  = load_metadata(bucket_for_metadata, list_metadata_files[1])

# --------------------------------------------------
# 3) PAGE LAYOUT + MAP
# --------------------------------------------------
st.title("GDS-Viewer Dashboard")

# Mapbox token
map_token = "pk.eyJ1IjoieXVyaXlrYXByaWVsb3YiLCJhIjoiY2t2YjBiNXl2NDV4YzJucXcwcXdtZHVveiJ9.JSi7Xwold-yTZieIc264Ww"
px.set_mapbox_access_token(map_token)

# Dedupe wells for the map
wells_map = curves_data.drop_duplicates(subset=['lat', 'lon']).reset_index(drop=True)

# Sidebar filter: geological times
geo_times = st.sidebar.multiselect(
    "Geologic Time",
    options=table_data['Geological_Time'].unique(),
    default=table_data['Geological_Time'].unique()
)

# Mark which wells intersect the selected times
wells_map['intersection_time'] = wells_map['Age'].apply(
    lambda x: 1 if set(x.split('_')).intersection(geo_times) else 0
)
filtered_wells = wells_map[wells_map['intersection_time'] == 1]

# Build the Mapbox scatter with box-select enabled
fig_map = px.scatter_mapbox(
    filtered_wells,
    lat="lat",
    lon="lon",
    hover_name="Name",
    zoom=4,
    height=600,
    mapbox_style="satellite"
)
fig_map.update_layout(dragmode="select")

# --------------------------------------------------
# 4) CAPTURE BOX‐SELECT EVENTS
# --------------------------------------------------
selected_points = plotly_events(
    fig_map,
    click_event=False,
    hover_event=False,
    select_event=True,
    key="map-select"
)

# Extract the well names from the hovertext
if selected_points:
    selected_wells = [pt["hovertext"] for pt in selected_points]
else:
    selected_wells = []

# Let user refine via multiselect
st.subheader("Selected Wells")
selected_wells = st.multiselect(
    "Chosen wells:",
    options=filtered_wells["Name"].tolist(),
    default=selected_wells
)

# --------------------------------------------------
# 5) SHOW DATA + PLOT CURVES + DOWNLOAD LAS
# --------------------------------------------------
if selected_wells:
    selected_data = curves_data[curves_data['Name'].isin(selected_wells)]
    st.dataframe(selected_data)

    # Per‐well curve plotting
    for well in selected_wells:
        well_data = selected_data[selected_data['Name'] == well]
        type_curve = well_data['Type'].iloc[0]

        # List CSVs for this type in S3
        bucket_vis = "transformed-for-visualization-data-1"
        prefix    = f"csv/{type_curve}"
        resp      = client.list_objects_v2(Bucket=bucket_vis, Prefix=prefix)
        keys      = [o['Key'] for o in resp.get('Contents', [])]
        csv_key   = next((k for k in keys if type_curve in k), None)

        if csv_key:
            obj      = client.get_object(Bucket=bucket_vis, Key=csv_key)
            curve_df = pd.read_csv(obj['Body'])
            curve_df = curve_df[curve_df['Well_name'] == well]

            fig_curve = go.Figure()
            fig_curve.add_trace(go.Scatter(
                x=curve_df[curve_df.columns[1]],
                y=curve_df['DEPTH'],
                mode='lines',
                name=type_curve
            ))
            fig_curve.update_layout(
                title=f"{type_curve} Curve for {well}",
                yaxis_autorange='reversed',
                height=500
            )
            st.plotly_chart(fig_curve, use_container_width=True)

    # LAS downloads
    st.subheader("Download LAS Files")
    bucket_dl = "transformed-for-download-data"
    las_keys  = [o['Key'] for o in client.list_objects_v2(
        Bucket=bucket_dl, Prefix='las/'
    )['Contents']]

    for _, row in selected_data.iterrows():
        name    = f"{row['lat']}_{row['lon']}_{row['Depth_start']:.1f}_{row['Depth_finish']:.1f}_{row['Name']}"
        las_key = next((k for k in las_keys if name in k), None)

        if las_key:
            url      = client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_dl, 'Key': las_key}
            )
            resp     = requests.get(url)
            st.download_button(
                label=f"Download LAS for {row['Name']}",
                data=resp.content,
                file_name=f"{name}.las"
            )
        else:
            st.warning(f"LAS file not found for {row['Name']}")
