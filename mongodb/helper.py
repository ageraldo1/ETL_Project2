import argparse
import os
import datetime

import modules.load_tweets as lt
import modules.top_activity as ta
import modules.theguardian as tg
import modules.nyt as nyt

from config import guardian_api_key
from config import nyt_api_key

def init():

    global csv_source_path    
    global rebuild_indexes
    global mongo_url
    global start_exec

    start_exec = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='TheDonald ETL Application')
    parser.add_argument('--csv_source_path',type=str, metavar='', required=False, help='path for source CSV dataset(tweets.csv)', default='../resources/tweets.csv')
    parser.add_argument('--rebuild_indexes',type=lambda x: (str(x).lower() in ['true','1', 'yes']), metavar='', required=False, help='Rebuild indexes/collections before loading', default=True)
    parser.add_argument('--mongo_url',type=str, metavar='', required=False, help='MongoDB url', default='mongodb://localhost:27017/')


    args = vars(parser.parse_args())

    csv_source_path = args['csv_source_path']
    rebuild_indexes = args['rebuild_indexes']
    mongo_url = args['mongo_url']

    splash_screen()

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def splash_screen():
    clear()
    
    print (f'''
The Donald ETL Application - Data Science BootCamp 2019

Parameters:
    Path for source CSV dataset..................: {csv_source_path}
    Rebuild indexes/collections before loading...: {rebuild_indexes}
    MongoDB URL..................................: {mongo_url}
''')

def process():
    print('Processing:')
    
    print(f'    Loading Tweets dataset................: {lt.run(mongo_url, csv_source_path, rebuild_indexes)}')

    date_range = ta.run(mongo_url)
    print(f'    Determining most activity date........: {date_range}')
    print(f'    Loading The Guardian articles.........: {tg.run(mongo_url, rebuild_indexes, date_range[0], date_range[0], guardian_api_key)}')
    print(f'    Loading The New York Times articles...: {nyt.run(mongo_url, rebuild_indexes, nyt_api_key, date_range[0], date_range[0])}')
        

def end():
    seconds_elapsed = (datetime.datetime.now() - start_exec).total_seconds()
    print (f"\n\nProcess completed in {seconds_elapsed} second(s)")


