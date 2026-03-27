import json
import requests
import os
from dotenv import load_dotenv
from vitro_cad_api import get_mp_token, update_mp_list
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = service_account.Credentials.from_service_account_file(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    scopes=SCOPES
)

gc = gspread.authorize(creds)

drive_service = build('drive', 'v3', credentials=creds)

def main():

    return

if __name__ == '__main__':
    main()