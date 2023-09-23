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

def create_file_system(file_system_name):
    try:
        global file_system_client

        file_system_client = service_client.create_file_system(file_system=file_system_name)

    except Exception as e:
        print(e)


def create_directory(directory_name):
    try:
        file_system_client.create_directory(directory_name)

    except Exception as e:
     print(e)


def upload_file_to_directory(file_system_name, directory_name, cde_file):
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system_name)

        directory_client = file_system_client.get_directory_client(directory_name)

        file_client = directory_client.create_file(cde_file)
        local_file = open("/app/mount/"+cde_file,'r')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e)

def list_directory_contents(file_system_name):
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system_name)

        paths = file_system_client.get_paths(path="my-directory")

        for path in paths:
            print(path.name + '\n')

    except Exception as e:
     print(e)

if __name__ == "__main__":
    pass
