import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

def get_mp_token():
    mp_url = os.getenv('VITRO_CAD_API_BASE_URL')
    mp_login = {
        "login": os.getenv('VITRO_CAD_ADMIN_USERNAME'),
        "password": os.getenv('VITRO_CAD_ADMIN_PASSWORD')
    }
    url_string = f"{mp_url}/api/security/login"
    try:
        with requests.post(url=url_string, json=mp_login) as response:
            response.raise_for_status()
            response_json = response.json()
            token = response_json.get('token')
            return token
    except requests.exceptions.RequestException as e:
        print(f"Error getting MP token: {e}")
        return None

def update_mp_list(mp_token, data):
    mp_url = os.getenv('VITRO_CAD_API_BASE_URL')
    url_string = f"{mp_url}/api/item/update"
    item_list_json = json.dumps(data)
    item_update_request = {'itemListJson': item_list_json}
    headers = {'Authorization': mp_token}
    try:
        with requests.post(url=url_string, headers=headers, data=item_update_request) as response:
            response.raise_for_status()
            response_json = response.json()
            return response_json
    except requests.exceptions.RequestException as e:
        print(f"Error updating MP list: {e}")
        return None