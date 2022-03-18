import os
import uuid
from dotenv import load_dotenv  # To access environment variables from a .env file
from pyairtable import Base, metadata  # To access Airtable
import pandas as pd  # To work with data from Airtable
import azure_helpers  # Helper functions
from datetime import datetime, timedelta
# to work with Azure
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


# Get variables from .env file
load_dotenv()
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID')
CONNECT_STR = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME')
ACCOUNT_KEY = os.environ.get('ACCOUNT_KEY')
CONTAINER_NAME = os.environ.get('CONTAINER_NAME')
DIRECTORIES = os.environ.get('DIRECTORIES').split(",")

# Load Azure Client
blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
container_client = container_client = blob_service_client.get_container_client(
    CONTAINER_NAME)

# Load Base
my_base = Base(AIRTABLE_API_KEY, AIRTABLE_BASE_ID)
schema = metadata.get_base_schema(my_base)
tables = schema['tables']

# Check if Each Folder Directory Exists, if not make one
for dir in DIRECTORIES:
    tablesPath = dir
    tablesFolderExists = os.path.isdir(tablesPath)

    if tablesFolderExists:
        pass
    else:
        os.makedirs(dir)

# For each table in a base
for table in tables:

    # Get Table Data and create CSV from JSON
    table_name = table['name']
    filename = 'Tables/'+table['id']+'.csv'
    azure_filename = f'Tables/{table_name}/'+table['id']+'.csv'
    print(f'Getting data for Table: {table_name}')

    # Format Table Data
    table_data_raw = my_base.all(table['id'], cell_format="string", user_locale='en-ie',
                                 time_zone='America/New_York')  # Update Locale and Timezone
    table_data = azure_helpers.add_record_ids(table_data_raw)
    table_df = pd.DataFrame(table_data)
    table_df.columns = table_df.columns.str.replace(' ', '_')
    table_df.columns = table_df.columns.str.lower()
    table_csv = table_df.to_csv(filename, index=False)

    # Check Azure Container for contents and instance of CSV
    print('Checking Azure for existing CSV')
    check_azure = container_client.list_blobs()
    blob_names = [x.name for x in check_azure]
    blob_count = len(blob_names)
    no_content = blob_count == 0
    file_exists = None
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, blob=filename)

    # If there aren't any contents in the container skip to next part
    if no_content:
        file_exists = False
    # If there are contents in the container check to see if file exists
    else:
        local_filename = 'Staging/'+table['id']+'.csv'
        file_exists = filename in blob_names

    # If File exists, download file, append, and upload to container then remove from staging
    if file_exists:
        print('Existing file found, uploading new version')
        with open(local_filename, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        existing_df = pd.read_csv(local_filename)
        frames = [existing_df, table_df]
        upload_df = pd.concat(frames).drop_duplicates(subset='airtable_id')
        upload_df.to_csv(filename, index=False)
        with open(filename, "rb") as upload_file:
            blob_client.upload_blob(data=upload_file, overwrite=True)
        os.remove(local_filename)
    # If no file exists with that table id/name in container uplpad the CSV Upload File
    else:
        print('No existing file found, uploading a new file to Azure')
        with open(filename, "rb") as upload_file:
            blob_client.upload_blob(data=upload_file, overwrite=True)

print('\n\n\n')
