# # Spark-SQL from PySpark
#
# This example shows how to send SQL queries to Spark.
from __future__ import print_function
import os, uuid, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

### PYSPARK SQL NOT REQUIRED

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()

# A list of Rows. Infer schema from the first row, create a DataFrame and print the schema
rows = [Row(name="John", age=19), Row(name="Smith", age=23), Row(name="Sarah", age=18)]
some_df = spark.createDataFrame(rows)
some_df.printSchema()

# A list of tuples
tuples = [("John", 19), ("Smith", 23), ("Sarah", 18)]

# Schema with two fields - person_name and person_age
schema = StructType([StructField("person_name", StringType(), False),
                    StructField("person_age", IntegerType(), False)])

# Create a DataFrame by applying the schema to the RDD and print the schema
another_df = spark.createDataFrame(tuples, schema)
another_df.printSchema()

for each in another_df.collect():
    print(each[0])

### MOVING DATA FROM THE CDE RESOURCE TO ADLS

##Your Storage Account Here
os.environ["storage_account_name"] = "abfs://data@demoazurego02.dfs.core.windows.net"
os.environ["account_name"] = "demoazurego02"
os.environ["storage_account_key"] = "<your storage account key>"

#hdfs dfs -mkdir -p $STORAGE/mydir
#hdfs dfs -copyFromLocal /app/mount/sample_resource/firstdag.py $STORAGE/mydir/firstdag.py

def initialize_storage_account(storage_account_name, storage_account_key):

    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print(e)

def create_file_system():
    try:
        global file_system_client

        file_system_client = service_client.create_file_system(file_system="my-file-system")

    except Exception as e:
        print(e)


def create_directory():
    try:
        file_system_client.create_directory("my-directory")

    except Exception as e:
     print(e)


def upload_file_to_directory():
    try:

        file_system_client = service_client.get_file_system_client(file_system="my-file-system")

        directory_client = file_system_client.get_directory_client("my-directory")

        file_client = directory_client.create_file("firstdag.py")
        local_file = open("/app/mount/firstdag.py",'r')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e)

def list_directory_contents():
    try:

        file_system_client = service_client.get_file_system_client(file_system="my-file-system")

        paths = file_system_client.get_paths(path="my-directory")

        for path in paths:
            print(path.name + '\n')

    except Exception as e:
     print(e)


try:
    initialize_storage_account(os.environ["account_name"], os.environ["storage_account_key"])
    print("Connection to ADLS Initialized")
except:
    print("Error During Connection Initialization")

try:
    create_file_system()
    print("File System Creation Successful")
except:
    print("File System Cretion Failed")

try:
    create_directory()
    print("ADLS Directory Creation Successful")
except:
    print("ADLS Directory Creation Failed")

try:
    upload_file_to_directory()
    print("File Upload Successful")
except:
    print("File Upload Failed")

try:
    print("ADLS Directory Contents:")
    list_directory_contents()
except:
    print("Listing ADLS Directory Contents Failed")
