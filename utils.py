#-----------------------------------------------------------------
#               HELPER METHODS TO CREATE REQUIRED ADLS RESOURCES
#-----------------------------------------------------------------
from __future__ import print_function
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

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

        file_client = directory_client.create_file("my_file1.py")
        local_file = open("/app/mount/my_file1.py",'r')

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

if __name__ == "__main__":
    pass
