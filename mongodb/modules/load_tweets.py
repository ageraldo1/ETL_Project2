import csv
from pymongo import MongoClient
from datetime import datetime

def run(mongo_url, csv_source_file, reload ):    
    db_name = 'TheDonald'
    db_collection = 'tweets'

    with open(file=csv_source_file, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')

        try:
            client = MongoClient(mongo_url)
            db = client[db_name]

            if reload == True:
                db.drop_collection(db_collection)

            collection = db.get_collection(db_collection)

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

                collection.insert_one(row)

        except Exception as e :
            return f'Erros during tweets load : {e} '

    return 'Tweets load completed without issues'
