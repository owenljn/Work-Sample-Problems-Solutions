from __future__ import print_function
from pyspark.sql.functions import lit
from pyspark.sql.types import *

import re
import os
import sys
from operator import add

from pyspark.sql import SparkSession

from math import sin, cos, sqrt, atan2, radians

# approximate radius of earth in km
R = 6373.0

def calculateDistance(POI_lat, POI_lng, lat, lng):
    
    # Convert coordinates to radians
    POI_lat = radians(float(POI_lat))
    POI_lng = radians(float(POI_lng))
    lat = radians(float(lat)) 
    lng = radians(float(lng))

    dlat = lat - POI_lat
    dlon = lng - POI_lng
    a = sin(dlat / 2)**2 + cos(POI_lat) * cos(lat) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def findNearestPOI(POIList_df, lat, lng):
    
    # Initialize minimum distance as well as POI id
    minDist = float("inf")
    POIID = None

    # Compare distance for each POI coordinates with a given request
    prev_POI_lat = None
    prev_POI_lng = None
    for row in POIList_df.rdd.collect():
        POI_lat = row[1]
        POI_lng = row[2]
        if POI_lat == prev_POI_lat and POI_lng == prev_POI_lng:
            continue
        prev_POI_lat = POI_lat
        prev_POI_lng = POI_lng

        tempDist = calculateDistance(POI_lat, POI_lng, lat, lng)
        if minDist > tempDist:
            minDist = tempDist
            POIID = row[0]
    return POIID, minDist

def insertPOIInfo(dataSample_df, POIList_df):

    POIID_list = []

    for row in dataSample_df.rdd.collect():
        lat = row[5]
        lng = row[6]
        ID = row[0]
        POIID, minDist = findNearestPOI(POIList_df, lat, lng)
        POIID_list.append((row[0],POIID, minDist)) # Store both label and distance info for later use
    return POIID_list

def mergeDataFrames(dataSample_df, POIID_list):
    POI_df = spark.createDataFrame(POIID_list,['ID', 'POIID', 'minDistance'])
    result = dataSample_df.join(POI_df, dataSample_df._ID == POI_df.ID, 'inner').drop(POI_df.ID)
    return result

if __name__ == "__main__":
    
    # Create a spark session
    spark = SparkSession\
        .builder\
        .appName("Label written in Python")\
        .getOrCreate()

    # Only display error messages in Spark console for easy debugging
    spark.sparkContext.setLogLevel("ERROR")

    # Load the csv into a dataframe
    dataSample_df = spark.read.format("csv").option("header", "true").load("/tmp/data/clean/clean_dataSample.csv")
    POIList_df = spark.read.format("csv").option("header", "true").load("/tmp/data/POIList.csv")
    
    # Store the POI info for each request
    POIID_list = insertPOIInfo(dataSample_df, POIList_df)
    result = mergeDataFrames(dataSample_df, POIID_list)

    # Store results into a new csv file using coalesce
    result.coalesce(1).write.option("header", "true").csv("/tmp/data/Label")

    # Rename the file for later use
    dir = "/tmp/data/Label"
    for filename in os.listdir(dir):
        if re.match(r'part.*\.csv$', filename):
            os.rename(os.path.join(dir, filename), os.path.join(dir, "Label_dataSample.csv"))

    # Stop the spark session
    spark.stop()
