This is an example python script for exporting data from a Base to Azure Blob Storage.

The script will export each table in a base as a CSV and save the CSV to a Blob Storage Container. If a CSV for a table already exists, the script will append update existing records in the CSV, and append new records as well. 
