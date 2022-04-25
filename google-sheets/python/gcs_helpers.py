import boto3
import logging, os, io
import pandas as pd
import gspread
import gspread_dataframe as gd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from oauth2client.service_account import ServiceAccountCredentials

'''
TO DO
 - Schema Check and Resolution for Table Updates
'''

def sheets_upload(table_df, table_name):

    # Use creds to create a client to interact with the Google Sheets API
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key('1BXwEN8bHFkYhmwdm6DbsXFtyGnMtNkkyPZoi9akX3fg')
    worksheet_titles = [worksheet.title for worksheet in spreadsheet] # Get worksheet titles
    # Check if worksheet exists
    worksheet_exists = table_name in worksheet_titles

    # If sheet exists open sheet and update data
    if worksheet_exists:
        print(f'Sheet for {table_name} found. appending table')
        worksheet = spreadsheet.worksheet(table_name)
        existing_df = gd.get_as_dataframe(worksheet)
        frames = [existing_df, table_df]
        upload_df = pd.concat(frames).drop_duplicates(subset='airtable_id')
        gd.set_with_dataframe(worksheet, upload_df)
    else:
    # If sheet doesn't exist, create sheet and add data
        print(f'No sheet found for {table_name}, creating new table')
        worksheet = spreadsheet.add_worksheet(title=table_name, rows=2, cols=2)
        gd.set_with_dataframe(worksheet, table_df)
    
    print(f'Successfully uploaded {table_name}! \n')


def gcp_upload(client,bucket_name,table_name,table_df,dataset_name,join=False):
    print('Checking GCP for existing json')
    
    bq_client = bigquery.Client()
    
    # Check GCS bucket for contents and instance of json
    bucket = client.get_bucket(bucket_name)
    check_gcs = [blob for blob in client.list_blobs(bucket_name)]
    no_content = len(check_gcs) == 0
    file_exists = None

    dataset = f'{bq_client.project}.{dataset_name}'

    filename = f'JSON/{table_name.replace(" ","_")}.json'
    local_filename = f'Staging/{table_name.replace(" ","_")}.json'

    # If there aren't any contents in the bucket skip to next part
    if no_content:
        file_exists = False

    # If there are contents in the bucket check to see if file exists
    else:
        blob = bucket.blob(filename)
        file_exists = blob.exists()

    # If File exists, download file, append, and upload to bucket then remove from staging
    if file_exists:
        print('Existing file found, uploading new version to Google Cloud Storage')
        download_blob(bucket_name, filename, local_filename)
        existing_df = pd.read_json(local_filename, orient='records',lines=True)
        frames = [existing_df, table_df]
        upload_df = pd.concat(frames).drop_duplicates(subset='airtable_id')
        upload_df.to_json(filename, orient="records", lines=True)
        upload_blob(bucket_name, filename, filename)
        uri = 'gs://{}/{}'.format(bucket_name,filename)
        os.remove(local_filename)

    # If no file exists with that table id/name in GCS bucket uplpad the json Upload File
    else:
        print('No existing file found, uploading a new file in Google Cloud Storage')
        upload_blob(bucket_name, filename, filename)
        uri = 'gs://{}/{}'.format(bucket_name,filename)

        # If Dataset exists == None then create dataset and set dataset to created dataset
        if dataset_name == None or "":
            print(f'Creating Dataset. Please input a name for the new dataset')
            dataset_name = input()
            dataset = create_dataset(dataset_name)
    
    # Check Dataset to see if tables exist
    bq_table_name = f'{dataset}.{table_name}'
    table_exists = table_check(bq_table_name,dataset)
    bq_table_id = f'{dataset}.{table_name}'

    # If Table exists Append Data
    print(f'Writing {table_name} data to Big Query')
    if table_exists:
        overwrite_bq_table(bq_table_id,uri)
    # Else Create Table
    else:
        new_bq_table(bq_table_id,uri)

    print(f'Successfully Uploaded {table_name} data to Google Cloud 👍🏾 \n\n\n')


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def add_record_ids(table_data):
    
    revised_data = []
    
    for record in table_data:

        fields = record['fields']
        airtable_id = record['id']
        fields['airtable_id'] = airtable_id
        revised_data.append(fields)
    
    return revised_data
    
def create_dataset(archive_name):

    bq_client = bigquery.Client()

    # Construct a full Dataset object to send to the API.
    dataset_id = f"{bq_client.project}.{archive_name}"
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    dataset = bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(bq_client.project, dataset.dataset_id))
    return "{}.{}".format(bq_client.project, dataset.dataset_id)


def new_bq_table(table_id,uri):
# Construct a BigQuery client object.
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def overwrite_bq_table(table_id, uri):
    # Construct a BigQuery client object.
    client = bigquery.Client()


    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def table_check(table_id,dataset):

    bq_client = bigquery.Client()
    tables = bq_client.list_tables(dataset)
    table_list = [table for table in tables]
    return
    check = table_id in bq_client.list_tables(dataset)  # Make an API request.
    if check == NotFound:
        return False
    else:
        return True

def compare_schemas(existing_df, table_df):

    '''
        This function is to help asborb schema changes. However, because of BigQuery's requirements, the following shcema changes are not allowed
            - Changing a column's name
            - Changing a column's data type
            - Changing a column's mode (aside from relaxing REQUIRED columns to NULLABLE)
    '''

    existing_schema = existing_df.columns
    new_schema = table_df.columns
    net_new_columns = [x for x in new_schema if x not in existing_schema]

def format_linked_records(linked_fields,table_df,tables,table_ids,my_base):
    for field in linked_fields:

        print(f"Formatting Linked Records for {field['name']} field")

        # Find Linked Table and Field Names from Airtable Schema
        linked_field_name = field['name']
        linked_table_id = field['options']['linkedTableId']
        linked_table_index = table_ids.index(linked_table_id)
        linked_table = tables[linked_table_index]
        linked_table_fields = linked_table['fields']
        linked_primary_field = linked_table_fields[0]['name']

        # Get Linked Table Data
        linked_table_data_raw = my_base.all(linked_table_id,fields=linked_primary_field)
        linked_table_data = add_record_ids(linked_table_data_raw)
        linked_table_df = pd.DataFrame(linked_table_data)
        linked_table_df.columns = [f'{x}_linked_record_x' for x in linked_table_df.columns]
        
        # Format Data Frame
        linked_df = table_df.explode(linked_field_name)
        linked_df = pd.merge(linked_df,linked_table_df,how='left',left_on=linked_field_name,right_on=linked_table_df.columns[1])
        linked_df = linked_df.groupby('airtable_id',as_index=False).agg({linked_table_df.columns[0]:lambda x: list(x)})
        table_df = table_df.drop(columns=[linked_field_name])
        table_df = pd.merge(table_df,linked_df,on='airtable_id')
        table_df = table_df.rename(columns={linked_table_df.columns[0]:linked_field_name})
    return table_df