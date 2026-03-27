import json
import requests
import dotenv

def get_mp_token(mp_url, mp_login):
    url_string = f"{mp_url}/api/security/login"
    try:
        with requests.post(url=url_string, json=mp_login) as response:
            response.raise_for_status()
            response_json = json.loads(response.text)
            value = response_json['token']
            return value
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None

def update_mp_list(mp_url, mp_token, data):
    url_string = f"{mp_url}/api/item/update"
    item_list_json = json.dumps(data)
    item_update_request = {'itemListJson': item_list_json}
    try:
        with requests.post(url=url_string, headers={'Authorization': mp_token}, data=item_update_request) as response:
            response.raise_for_status()
            response_json = json.loads(response.text)
            return response_json
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None