## Send Airtable data to Amazon s3 via pyAirtable and Boto3

This Python script uses [pyAirtable](https://pyairtable.readthedocs.io/en/latest/) to connect wiht Airtable, and [boto3](https://aws.amazon.com/sdk-for-python/) to connect with AWS.

Expected Functionality:

- For each table in the selected base this script will...
  - Copy table data
  - If a csv file does not exist for the table in s3...
    - Create a csv file in s3 from the table data
  - If a csv file exists for the table in s3...
    - Append net new records to the CSV
    - Update chages to existing records in the CSV

#### Quick Start

- Setup and configure authentication credentials for [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration)
- Get the name of your s3 Bucket
- Get your Airtable API Key from your [account page](https://airtable.com/account)
- Get your Base ID from your Base Hyperlink, it should look like 'appXXXXXXXXX'
- Update the .env.example and save as a .env file
- Run the notebook
