import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# based on https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html
# read that file for how to generate the creds and how to use gspread to read and write to the spreadsheet

# use creds to create a client to interact with the Google Drive API
scopes = ['https://spreadsheets.google.com/feeds']
json_creds = os.getenv("GOOGLE_SHEETS_CREDS_JSON")

creds_dict = json.loads(json_creds)
creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
client = gspread.authorize(creds)

# Find a workbook by url
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1RcQuetbKVYRRf0GhGZQi38okY8gT1cPUs6l3RM94yQo/edit#gid=704459328")
sheet = spreadsheet.sheet1

# Extract and print all of the values
rows = sheet.get_all_records()
print(rows)