import streamlit as st
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus
import plotly.express as px

# ---------------- CONFIG ----------------
DB_CONFIG = {
    'username':'dashboardUser',
    'password':'D@shPas$w0rd'
}
MONGO_CONN = f"mongodb+srv://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}@test2-mongodb.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"   # Replace with your Cosmos DB/MongoDB URI
DB_NAME = "nyc_taxi"
COLLECTION_NAME = "trips"
ZONE_LOOKUP_PATH = "taxi_zone_lookup.csv"
# ----------------------------------------

@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_CONN)
    return client[DB_NAME][COLLECTION_NAME]

@st.cache_data
def load_trips(_collection, limit=None):
    projection = {
        "_id": 0,
        "tpep_pickup_datetime": 1,
        "tpep_dropoff_datetime": 1,
        "passenger_count": 1,
        "trip_distance": 1,
        "PULocationID": 1,
        "DOLocationID": 1,
        "fare_amount": 1,
        "tip_amount": 1,
        "total_amount": 1,
    }
    cursor = _collection.find({}, projection)
    if limit:
        cursor = cursor.limit(limit)
    df = pd.DataFrame(list(cursor))
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    return df

@st.cache_data
def load_zone_lookup(path):
    df_zones = pd.read_csv(path)
    df_zones.rename(columns={"LocationID": "LocationID"}, inplace=True)
    return df_zones

# ---------------- MAIN APP ----------------
st.title("NYC Taxi Trip Dashboard for September 2025")

collection = get_mongo_collection()
df_trips = load_trips(collection)
df_zones = load_zone_lookup(ZONE_LOOKUP_PATH)

# Merge pickup/dropoff zone info
df = df_trips.merge(df_zones, how="left", left_on="PULocationID", right_on="LocationID")
df = df.rename(columns={
    "Borough": "Pickup_Borough",
    "Zone": "Pickup_Zone",
    "service_zone": "Pickup_Service_Zone"
})
df = df.merge(df_zones, how="left", left_on="DOLocationID", right_on="LocationID", suffixes=("", "_Dropoff"))
df = df.rename(columns={
    "Borough_Dropoff": "Dropoff_Borough",
    "Zone_Dropoff": "Dropoff_Zone",
    "service_zone_Dropoff": "Dropoff_Service_Zone"
})

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", len(df))
col2.metric("Avg Fare ($)", round(df["fare_amount"].mean(), 2))
col3.metric("Avg Tip ($)", round(df["tip_amount"].mean(), 2))
col4.metric("Total Revenue ($)", round(df["total_amount"].sum(), 2))

st.markdown("---")

# Trips per day
trips_per_day = df.groupby("pickup_date").size().reset_index(name="trips")
fig_trips = px.line(trips_per_day, x="pickup_date", y="trips", title="Trips per Day")
st.plotly_chart(fig_trips, use_container_width=True)

# Average fare by pickup borough
avg_fare = df.groupby("Pickup_Borough")["fare_amount"].mean().reset_index().sort_values("fare_amount", ascending=False)
fig_fare = px.bar(avg_fare, x="Pickup_Borough", y="fare_amount", title="Average Fare by Pickup Borough", color="Pickup_Borough")
st.plotly_chart(fig_fare, use_container_width=True)

# Top 10 pickup zones
top_zones = df.groupby("Pickup_Zone").size().reset_index(name="count").sort_values("count", ascending=False).head(10)
st.subheader("Top 10 Pickup Zones")
st.dataframe(top_zones)

st.caption("Data sourced from NYC TLC Trip Records and Taxi Zone Lookup Table")
