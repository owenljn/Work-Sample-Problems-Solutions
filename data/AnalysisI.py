from __future__ import print_function
from pyspark.sql.functions import lit
from pyspark.sql.types import *
import numpy as np
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
        # Avoid calculating the same geo info, note this ignores POI2 since it's identical to POI1
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

    POIinfo_dict = {}

    # Store min distances for each request into a dictionary
    for row in dataSample_df.rdd.collect():
        lat = row[5]
        lng = row[6]
        ID = row[0]
        POIID, minDist = findNearestPOI(POIList_df, lat, lng)
        if not POIID in POIinfo_dict:
            POIinfo_dict[POIID] = []
        else:
            POIinfo_dict[POIID].append(minDist)
    
    # Return a list of POIs info that contains each of their average and standard deviation of stored minimum distances info
    for key, value in POIinfo_dict.items():
        arr = np.array(value)
        POIinfo_dict[key] = (key, float(np.mean(arr, axis=0)), float(np.std(arr, axis=0)))
    return POIinfo_dict.values()

def mergeDataFrames(POIList_df, POIID_list):
    
    # Now merge two dataframes into one result that contains POI info as well as average and standard deviation of request distances for each POI
    POI_df = spark.createDataFrame(POIID_list,['ID', 'average', 'standardDeviation'])
    POI_df.coalesce(1).write.option("header", "true").csv("/tmp/data/POIAnalysisI_df")
    result = POIList_df.join(POI_df, POIList_df.POIID == POI_df.ID, 'inner').drop(POI_df.ID)
    return result

if __name__ == "__main__":
    
    # Create a spark session
    spark = SparkSession\
        .builder\
        .appName("Analysis I written in Python")\
        .getOrCreate()

    # Only display error messages in Spark console for easy debugging
    spark.sparkContext.setLogLevel("ERROR")

    # Load the csv into a dataframe
    dataSample_df = spark.read.format("csv").option("header", "true").load("/tmp/data/clean/clean_dataSample.csv")
    POIList_df = spark.read.format("csv").option("header", "true").load("/tmp/data/POIList.csv")
    
    # Store the POI info as well as min distance info for each request
    POIinfo_list = insertPOIInfo(dataSample_df, POIList_df)

    # Merge dataframes
    result = mergeDataFrames(POIList_df, POIinfo_list)

    # Store results into a new csv file using coalesce
    result.coalesce(1).write.option("header", "true").csv("/tmp/data/Analysis_I")

    # Rename the file for later use
    dir = "/tmp/data/Analysis_I"
    for filename in os.listdir(dir):
        if re.match(r'part.*\.csv$', filename):
            os.rename(os.path.join(dir, filename), os.path.join(dir, "Analysis_I_dataSample.csv"))

    # Stop the spark session
    spark.stop()
