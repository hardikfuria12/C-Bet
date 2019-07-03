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

import pandas as pd
from Event import *
s3=boto3.resource('s3')


def get_event_in_db(credential_path='~/.my.cnf'):
    myDB = URL(drivername='mysql+pymysql', username='root',
               password='12345678',
               host='cbet.c0jjgccwckol.us-east-1.rds.amazonaws.com',
               database='cbet',
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
                             bootstrap_servers=['localhost:9092'], api_version=(0, 9), group_id='t9')
    for msg in consumer:

        event_in_db=get_event_in_db()
        start_time=time.time()
        event_id = str(msg.key.decode('utf-8'))
        if event_id in event_in_db:
            print(event_id, ' already present in db - SKIPPED', time.time()-start_time)
            continue
        value = msg.value.decode('utf-8')
        value = str(value)
        list_event_files= value.split('_')[1:]
        print(event_id)
        e=Event()
        e.event_directory = event_id
        e.event_files =list_event_files
        e.id_event = str(event_id)
        e.build_event_heapque()
        e = e.process_timestamp_heapque()
        # print(e.associated_market_instances)

        e.upload_data()

        print(event_id, ' takes ', time.time()-start_time, ' to get processed')


    if consumer is not None:
        consumer.close()




def main():
    parsed_topic_name = 'final-test'
    process_kafka_messages(parsed_topic_name)



if __name__ == '__main__':
    main()

    # parsed_topic_name = 's3bucket'
    #
    # s3=boto3.resource('s3')
    #
    # consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
    #                          bootstrap_servers=['localhost:9092'], api_version=(0, 10),group_id='g6')
    #
    # for msg in consumer:

        # print(type(s3.Object('s3bucket', str(value)).get()['Body'].read()))





        # record = json.loads(msg.value)
        # calories = int(record['calories'])
        # title = record['title']
        #
        # if calories > calories_threshold:
        #     print('Alert: {} calories count is {}'.format(title, calories))
        # sleep(3)


# import boto3
# s3 = boto3.resource('s3')
# print s3.Object('mybucket', 'beer').get()['Body'].read()