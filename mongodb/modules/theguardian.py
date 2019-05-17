import requests
import json
import time
from pymongo import MongoClient

def run(mongo_url, reload, start_date, end_date, api_key):
    api_url = 'https://content.guardianapis.com/search'
    
    db_name = 'TheDonald'
    db_collection = 'theguardian'

    time_to_sleep = 5
    page_number = 1
    
    client = MongoClient(mongo_url)
    db = client[db_name]

    if reload == True:
         db.drop_collection(db_collection)
    
    collection = db.get_collection(db_collection)

    while True:         
        params = {
            'api-key' : api_key,
            'from-date' : start_date,
            'to-date' : end_date,
            'page' : page_number
        }

        response = requests.get(url = api_url, params = params)

        if response.status_code == 200:
            articles = response.json()

            if articles['response']['status'] == 'ok':                
                for record in articles['response']['results']:
                    collection.insert_one(record)

                time.sleep(time_to_sleep)
                page_number = page_number + 1

            else:
                return f'Error loading api response : {articles["response"]["status"]}'

        elif response.status_code == 400:
            # requested page is beyond the number of available pages
            # end of the records
            return 'Process completed without issues'

        else:
            return f'Error loading api {api_url} - status code : {response.status_code}'

        










