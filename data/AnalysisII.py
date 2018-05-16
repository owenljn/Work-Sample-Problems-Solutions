from __future__ import print_function
from pyspark.sql.functions import lit
from pyspark.sql.types import *

import numpy as np
import matplotlib
matplotlib.use('agg') # Ignore tKinter
import matplotlib.pyplot as plt
import re
import os
import sys

from math import sin, cos, sqrt, atan2, radians, pi
from operator import add
from pyspark.sql import SparkSession

def calculateCircleArea(radius):
    return np.pi*np.square(radius)

def getCircleInfo(POIID_df, label_df):
    # Create a temporary table for processing
    label_df.createOrReplaceTempView("label")

    CircleInfo = []
    for row in POIID_df.rdd.collect():
        POIID = row[0]
        # Get circle radius for a given POI
        radius_query = "SELECT MAX(minDistance) as circle_radius FROM label WHERE POIID = '{}'".format(POIID)
        radius = float(spark.sql(radius_query).collect()[0].circle_radius)
        
        # Get the number of requests for that POI
        request_query = "SELECT count(_ID) as request_count FROM label WHERE POIID = '{}'".format(POIID)
        num_requests = int(spark.sql(request_query).collect()[0].request_count)

        density = num_requests/calculateCircleArea(radius)

        # Store radius along with the request number
        CircleInfo.append((POIID, radius, float(density)))

    return CircleInfo

def mergeDataFrames(POIList_df, CircleInfo):
    
    # Now merge two dataframes into one result
    POI_df = spark.createDataFrame(CircleInfo,['ID', 'raidus', 'density'])
    result = POIList_df.join(POI_df, POIList_df.POIID == POI_df.ID, 'inner').drop(POI_df.ID)
    return result

def calculateRadius(lat, lng, center_lat, center_lng):
    radius = 0
    i = 0
    while i < len(lat):
        temp_radius = np.sqrt((lat[i] - center_lat)**2 + (lng[i] - center_lng)**2)
        if temp_radius > radius:
            radius = temp_radius
        i+=1
    return radius

def plotPOIinfo(label_df, POI_df):
    plotted_poi = []
    # Create a temporary table for processing
    POI_df.createOrReplaceTempView("POI")
    label_df.createOrReplaceTempView("label")

    circle = {}
    for row in POI_df.rdd.collect():
        if row[0] not in plotted_poi: # Plot a circle if it doesn't exist
            plotted_poi.append(row[0])
            center_lat = float(spark.sql("SELECT latitude as lat FROM POI WHERE POIID = '{}'".format(row[0])).collect()[0].lat)
            center_lng = float(spark.sql("SELECT longitude as lng FROM POI WHERE POIID = '{}'".format(row[0])).collect()[0].lng)
            
            # Get the request coordinates in lists
            request_lat = spark.sql("SELECT latitude as lat FROM label WHERE POIID = '{}' ORDER BY _ID".format(row[0])).collect()
            request_lat = list(map(float, list(sum(request_lat, ()))))
            request_lng = spark.sql("SELECT longitude as lng FROM label WHERE POIID = '{}' ORDER BY _ID".format(row[0])).collect()
            request_lng = list(map(float, list(sum(request_lng, ()))))

            # Calculate radius in degrees
            radius = calculateRadius(request_lat, request_lng, center_lat, center_lng)

            # Plot points and circle
            plt.plot(request_lat, request_lng, '.')
            circle[row[0]] = plt.Circle((center_lat, center_lng), radius, fill=False, linewidth=None)
            plt.gca().add_artist(circle[row[0]])
    
    # Save plot
    plt.title('x-axis is the latitude, y-axis is the longitude\n POI1/POI2: blue dots, POI3: green dots, POI4: yellow dots')
    plt.axis([-200, 300, -400, 200])
    #plt.axis([0, 70, -150, 150]) # Adjust these numbers for zoom-in and zoom-out
    plt.savefig('/tmp/data/plot.png')

if __name__ == "__main__":
    
    # Create a spark session
    spark = SparkSession\
        .builder\
        .appName("Analysis II written in Python")\
        .getOrCreate()

    # Only display error messages in Spark console for easy debugging
    spark.sparkContext.setLogLevel("ERROR")

    # Load the csv into a dataframe
    label_dataSample_df = spark.read.format("csv").option("header", "true").load("/tmp/data/Label/Label_dataSample.csv") 
    POIID_df = spark.read.format("csv").option("header", "true").load("/tmp/data/Analysis_I/Analysis_I_dataSample.csv") 

    # Get the radius of each circle and store them in the list
    CircleInfo = getCircleInfo(POIID_df, label_dataSample_df)

    # Store the POI info for each request
    result = mergeDataFrames(POIID_df, CircleInfo)

    # Store results into a new csv file using coalesce
    result.coalesce(1).write.option("header", "true").csv("/tmp/data/Analysis_II")

    # Plot and save the circles
    plotPOIinfo(label_dataSample_df, POIID_df)

    # Rename the file for later use
    dir = "/tmp/data/Analysis_II"
    for filename in os.listdir(dir):
        if re.match(r'part.*\.csv$', filename):
            os.rename(os.path.join(dir, filename), os.path.join(dir, "Analysis_II_dataSample.csv"))

    # Stop the spark session
    spark.stop()
