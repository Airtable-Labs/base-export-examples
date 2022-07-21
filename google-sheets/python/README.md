## Send Airtable data to Google Sheets via pyAirtable and GSpread

This Python script uses [pyAirtable](https://pyairtable.readthedocs.io/en/latest/) to connect wiht Airtable, and [gspread](https://docs.gspread.org/en/latest/) to connect with Google Sheets.

Expected Functionality:

- For each table in the selected base this script will...
  - Copy table data
  - If a csv file does not exist for the table in a Google Sheet...
    - Create a table in Google Sheets from the table data
  - If a Sheet exists for the table in Google Sheets...
    - Append net new records to the Sheets
    - Update chages to existing records in the Sheets

#### Quick Start

- Create [OAuth 2.0 Client IDs](https://developers.google.com/identity/protocols/oauth2) in Google Cloud Console Credentials Section
- Download the JSON file of your client secret, and add it to your project folder
- Get your Airtable API Key from your [account page](https://airtable.com/account)
- Get your Base ID from your Base Hyperlink, it should look like 'appXXXXXXXXX'
- Get your the key of your google spreadsheet it should look be the XXXX portion of your google sheet URL 'https://docs.google.com/spreadsheets/d/XXXXXXX/edit#gid=123456'
- Update the .env.example with the API Key, Base ID, and Sheet Key and save as a .env file
- Run the notebook
