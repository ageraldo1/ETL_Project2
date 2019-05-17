import csv
from datetime import datetime
from elasticsearch import Elasticsearch

def run(es_url, csv_source_file, reload):
    document_type = 'tweets'
    index_name = 'tweets'

    es = Elasticsearch(hosts=es_url)

    if reload == True:
        es.indices.delete(index=index_name, ignore=[400, 404])

    status_message = None    

    with open(file=csv_source_file, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
    
        try:
            for line in csv_reader:
                row = {}

                row['timestamp'] = datetime.strptime(line['Date'] + ' ' + line['Time'], '%y-%m-%d %H:%M:%S')
                row['tweet_text'] = line['Tweet_Text']
                row['type'] = line['Type']
                row['media_type'] = line['Media_Type']
                row['hashtags'] = line['Hashtags']
                row['tweet_id'] = line['Tweet_Id']
                row['tweet_url'] = line['Tweet_Url']
                row['tweet_favorites'] = line['twt_favourites_IS_THIS_LIKE_QUESTION_MARK']
                row['retweets'] = line['Retweets']

                res = es.index(index=index_name, doc_type=document_type, body=row)
                
                if 'failed' in res['result']:
                    status_message = f'Erros during tweets load : {res}'
                    break                     
                    
        except Exception as e :
            status_message = f'Erros during tweets load : {e} '
        
        if not status_message:
            status_message = 'Process completed without issues'
        
        return status_message





