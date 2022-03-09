import boto3
import logging, os
import pandas as pd
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def upload_file(file_name, bucket, object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def add_record_ids(table_data):
    
    revised_data = []
    
    for record in table_data:

        fields = record['fields']
        airtable_id = record['id']
        fields['airtable_id'] = airtable_id
        revised_data.append(fields)
    
    return revised_data

def format_linked_records(linked_fields,table_df,tables,my_base):

    # This function replaces the array of Record IDs stored in Linked Record fields
    # with an array of Record Names. (aka the value in the primary field of the linked table) by:
        #  - Identifying the Linked Table and Primary Field
        #  - Retrieving the Linked Table Data in a DataFrame
        #  - Isolating the Record IDs of each linked record by exploding the original dataframe
        #  - Merging the Linked Table and Original Table DataFrames and Regrouping the table by Record ID
        #  - Delteting the Linked Column and renaming the merged column to match the expected schema.

    # Table A
    # Name                          Publish Date    Author(s) [Linked Field] 
    # Through the Looking Glass     12/17/1871      ['rec12345678']
    # All the President's Men       06/15/1974      ['rec09876543', 'rec546372829']

    # Table B
    # Author            Birthdate   Birthplace      RecordId
    # Lewis Carroll     01/27/1832  London          'rec12345678'
    # Carl Bernstein    02/14/1944  Washington, DC  'rec09876543'
    # Bob Woodward      03/26/1946  Geneva, IL      'rec546372829'

    # Table A (After Formula)
    # Name                          Publish Date    Author(s) [Linked Field] 
    # Through the Looking Glass     12/17/1871      [Lewis Carroll]
    # All the President's Men       06/15/1974      [Carl Bernstein, Bob Woodward]

    for field in linked_fields:

        print(f"Formatting Linked Records for {field['name']} field")

        # Find Linked Table and Field Names from Airtable Schema
        linked_field_name = field['name']
        linked_table_id = field['options']['linkedTableId'] # Get linked table
        table_ids = [x['id'] for x in tables]
        linked_table_index = table_ids.index(linked_table_id)
        linked_table = tables[linked_table_index]
        linked_table_fields = linked_table['fields']
        linked_primary_field = linked_table_fields[0]['name'] # Get primary field of linked table

        # Get Linked Table Data
        linked_table_data_raw = my_base.all(linked_table_id,fields=linked_primary_field) # Get linked table data only with Primary Field (pyAirtable)
        linked_table_data = add_record_ids(linked_table_data_raw) #Add Record IDs to the dataframe to compare between linked field and the linked table records
        linked_table_df = pd.DataFrame(linked_table_data)
        linked_table_df.columns = [f'{x}_linked_record_x' for x in linked_table_df.columns] # Change the name of the columns in the linked table so they don't overlap with column names in original table
        
        # Format Data Frame
        linked_df = table_df.explode(linked_field_name) # Because multiple linked records are stored as an array of record ids, we'll need to explode the table to isolate each value 
        linked_df = pd.merge(linked_df,linked_table_df,how='left',left_on=linked_field_name,right_on=linked_table_df.columns[1]) # Now that each linked record id is isolated, merge the Orignal Table and Linked Table based on the Linked Field record ids
        linked_df = linked_df.groupby('airtable_id',as_index=False).agg({linked_table_df.columns[0]:lambda x: list(x)}) # Regroup table so linked record values are stored as arrays (aka unexplode)
        table_df = table_df.drop(columns=[linked_field_name]) # Drop the linked field which only has Record Ids
        table_df = pd.merge(table_df,linked_df,on='airtable_id') 
        table_df = table_df.rename(columns={linked_table_df.columns[0]:linked_field_name}) # Rename new columns so they match original table schema
    return table_df
