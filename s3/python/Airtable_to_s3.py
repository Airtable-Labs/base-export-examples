import os
from dotenv import load_dotenv  # To Access Environment Variables

from s3_helpers import upload_file, add_record_ids, format_linked_records

from pyairtable import Base, metadata  # To Access Airtable
import boto3  # To work with AWS
import pandas as pd  # To work with Data from Airtable

# Declare Variables from .env
load_dotenv()
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
DIRECTORIES = os.environ.get('DIRECTORIES').split(",")

# Declare Amazon S3 Variables
s3 = boto3.client('s3')
bucket_name = S3_BUCKET_NAME

# Load Base and Base Schema
my_base = Base(AIRTABLE_API_KEY, AIRTABLE_BASE_ID)
schema = metadata.get_base_schema(my_base)
tables = schema['tables']
table_ids = [x['id'] for x in tables]

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

    # Set Variables for Table Data
    table_name = table['name']
    filename = 'Tables/'+table['id']+'.csv'
    s3_filename = f'Tables/{table_name}/'+table['id']+'.csv'
    print(f'Getting data for Table: {table_name}')

    # Get Table Data and create CSV from JSON
    table_data_raw = my_base.all(table['id'])
    table_data = add_record_ids(table_data_raw)
    table_df = pd.DataFrame(table_data)

    # Get Array Fields
    table_index = tables.index(table)
    table_fields = tables[table_index]['fields']
    linked_fields = [x for x in table_fields if x['type']
                     in 'multipleRecordLinks']
    array_field_types = ['multipleRecordLinks', 'multipleCollaborators',
                         'multipleSelects', 'multipleAttachments', 'mutipleLookupValues']
    array_fields = [x['name']
                    for x in table_fields if x['type'] in array_field_types]
    non_array_fields = [x['name'] for x in table_fields if x['type']
                        not in array_field_types and table_fields.index(x) != 0]

    # Format Linked Records
    table_df = format_linked_records(linked_fields, table_df, tables, my_base)

    # Format Table Data
    table_df.columns = table_df.columns.str.replace(
        ' ', '_')  # replace spaces in headers with "_"
    # transform all characters to lowercase
    table_df.columns = table_df.columns.str.lower()
    table_df = table_df.set_index('airtable_id', drop=False)

    # Create CSV
    table_csv = table_df.to_csv(filename, index=False)

    # Check s3 bucket for contents and instance of CSV
    print('Checking s3 for existing CSV')
    check_s3 = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix="Table"
    )
    no_content = 'Contents' not in check_s3
    file_exists = None

    if no_content:
        file_exists = False

    # If there are contents in the bucket check to see if file exists
    else:
        local_filename = 'Staging/'+table['id']+'.csv'
        file_exists = len([object_summary['Key'] for object_summary in check_s3['Contents']
                          if s3_filename in object_summary['Key']]) > 0

    # If File exists, download file, append, and upload to bucket then remove from staging
    if file_exists:
        print('Existing file found, uploading new version')
        s3.download_file(bucket_name, s3_filename, local_filename)
        existing_df = pd.read_csv(local_filename)
        frames = [existing_df, table_df]
        upload_df = pd.concat(frames).drop_duplicates(
            subset='airtable_id')  # appends new data
        upload_df = upload_df.set_index('airtable_id', drop=False)
        upload_df.update(table_df)  # updates existing columns
        upload_df.to_csv(filename, index=False)
        upload_file(filename, bucket_name, s3_filename)
        os.remove(local_filename)

    # If no file exists with that table id/name in s3 bucket uplpad the CSV Upload File
    else:
        print('No existing file found, uploading a new file in s3')
        upload_file(filename, bucket_name, s3_filename)
