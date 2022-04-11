This is an example python script for exporting data from a Base to Azure Blob Storage.

The script will export each table in a base as a CSV and save the CSV to a Blob Storage Container. If a CSV for a table already exists, the script will append update existing records in the CSV, and append new records as well.

## To use this example you'll need an:
- Airtable Base
- Airtable API Key
- Azure Subscription
- Azure Blob Storage Account
- Azure Blob Storage Container

To configure this script, input the following variables in your .env file:

##Azure Variables
- AZURE_STORAGE_CONNECTION_STRING: 
- ACCOUNT_NAME
- ACCOUNT_KEY
- CONTAINER_NAME

##Airtable Variables
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID

