import requests
import json
import time
from elasticsearch import Elasticsearch

def run(es_url, reload, start_date, end_date, api_key):
    api_url = 'https://content.guardianapis.com/search'
    
    index_name = 'theguardian'
    document_type = 'news'

    time_to_sleep = 5
    page_number = 1
    
    es = Elasticsearch(hosts=es_url)

    if reload == True:
        es.indices.delete(index=index_name, ignore=[400, 404])
    
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
                    res = es.index(index=index_name, doc_type=document_type, body=record)

                    if 'failed' in res['result']:
                        return f'Erros during tweets load : {res}'
                
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

        










