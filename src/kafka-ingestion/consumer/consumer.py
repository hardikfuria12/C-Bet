import json
from time import sleep

from kafka import KafkaConsumer
import boto3
import heapq
import json
import time
import os

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import constants as const
import pandas as pd
import Event
s3=boto3.resource('s3')

# returns list of events that are already present in the database.
def get_event_in_db(credential_path='~/.my.cnf'):
    myDB = URL(drivername='mysql+pymysql', username='root',
               password=const.AMAZON_PWD,
               host=const.AMAZON_EP,
               database=const.AMAZON_DB,
               query={'read_default_file': credential_path})
    engine = create_engine(myDB)
    event_q = (
        f'''
            select eventid from event;
            '''
    )
    result = pd.read_sql(sql=event_q,con=engine)
    d=set(result['eventid'].tolist())
    engine.dispose()
    return d


def process_kafka_messages(parsed_topic_name):

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'],
                             api_version=(0, 9), group_id='t9') # group id is parsed to ensure all the consumner nodes are within the same group for a given topic
    for msg in consumer:
        event_in_db=get_event_in_db()
        event_id = str(msg.key.decode('utf-8'))
        if event_id in event_in_db:
            continue
        value = str(msg.value.decode('utf-8'))
        list_event_files= value.split('_')[1:]
        e=Event() # Refer to Event.py to get detailed explanation of what all the functions do
        e.event_directory = event_id
        e.event_files =list_event_files
        e.id_event = str(event_id)
        e.build_event_heapque()
        e = e.process_timestamp_heapque()
        e.upload_data()

    if consumer is not None:
        consumer.close()



# gets messages from the parsed kakfa-topic
def main():
    parsed_topic_name = 'final-test'
    process_kafka_messages(parsed_topic_name)



if __name__ == '__main__':
    main()
