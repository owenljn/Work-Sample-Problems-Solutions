from __future__ import print_function
import os
import re
from pyspark.sql import Row
from pyspark.sql.types import *

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    
    # Create a spark session
    spark = SparkSession\
        .builder\
        .appName("Cleanup written in Python")\
        .getOrCreate()

    # Load the csv into a dataframe
    dataframe = spark.read.format("csv").option("header", "true").load("/tmp/data/DataSample.csv")
    
    # Remove suspicious rows
    result = dataframe.dropDuplicates([' TimeSt', 'Country', 'Province', 'City', 'Latitude', 'Longitude'])

    # Store results into a new csv file using coalesce
    result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").csv("/tmp/data/Clean")
    
    # Rename the file for later use
    dir = "/tmp/data/Clean"
    for filename in os.listdir(dir):
        if re.match(r'part.*\.csv$', filename):
            os.rename(os.path.join(dir, filename), os.path.join(dir, "clean_dataSample.csv"))

    # Stop the spark session
    spark.stop()
