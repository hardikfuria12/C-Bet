import heapq
import json
import time
import os
import boto3
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import exc
import constants as const
import pandas as pd

event_ds={}

s3=boto3.resource('s3')

class Event:

    # initilaizes properties of an Event
    def __init__(self):
        self.name_event=None
        self.id_event=None
        self.event_directory=None
        self.event_files=None
        self.event_timestap_heapque=[]
        self.event_country_code=None
        self.event_date=None
        self.regulators=None
        self.associated_market_instances={}
        self.earliest_md=float('inf')
        self.last_md=float('-inf')
        self.earliest_rc=float('inf')
        self.last_rc=float('-inf')
        self.list_odds=set()
        self.list_match_odds=set()
        self.event_count=0

    # gets data content from all the data files that is related to an event and then sorts the data based on their time stamp value.
    def build_event_heapque(self):
        for file in self.event_files:
            data_read=s3.Object('hardik-testing', self.id_event+'/'+file).get()['Body'].read().decode('utf-8')
            sample_file=data_read.split("\n")[:-1]
            for line in sample_file:
                json_line = json.loads(line)
                ts = time.gmtime(json_line['pt'] / 1000.0)
                self.event_timestap_heapque.extend([(float(json_line['pt']), json_line)])
                self.event_count+=1
        self.event_timestap_heapque = sorted(self.event_timestap_heapque, key=lambda x: x[0])

    # this function process the complex json dumps and retrieves the relevant information.
    def process_timestamp_heapque(self):
        for tuple in self.event_timestap_heapque:
            json_line = tuple[1]
            for mc in json_line['mc']:
                if 'marketDefinition' in mc:
                    mar_def=mc['marketDefinition']
                    if mar_def['eventId'] not in event_ds:
                        self.name_event=mar_def['eventName']
                        self.event_country_code=(mar_def['countryCode'] if 'countryCode' in mar_def else "")
                        self.event_date=mar_def['openDate']
                        self.regulators=mar_def['regulators']
                        for runner in mar_def['runners']:
                            aug_key = str(mc['id']) + '_' + str(runner['id'])
                            id_market = mc['id']
                            event_id = mar_def['eventId']
                            event_type_id = mar_def['eventTypeId']
                            betting_type = mar_def['bettingType']
                            market_type = (mar_def['marketType'] if 'marketType' in mar_def else 'None')
                            market_start_time = mar_def['marketTime']
                            runner_name = runner['name']
                            runner_id = runner['id']
                            runner_status = runner['status']
                            if aug_key not in self.associated_market_instances:
                                new_market_instance=MarketRunnerInstance(id_market,event_id,event_type_id,betting_type,market_type,market_start_time,runner_id,runner_name,runner_status)
                                new_market_instance.earliest_md=min(new_market_instance.earliest_md,tuple[0])
                                new_market_instance.last_md=max(new_market_instance.earliest_md,tuple[0])
                                new_market_instance.runner_status=runner_status
                                self.associated_market_instances[aug_key]=new_market_instance
                            else:
                                existing_instance=self.associated_market_instances[aug_key]
                                existing_instance.earliest_md = min(existing_instance.earliest_md,tuple[0])
                                existing_instance.last_md = max(existing_instance.earliest_md, tuple[0])
                                existing_instance.runner_status=runner_status
                        event_ds[mar_def['eventId']]=self
                        self=event_ds[mar_def['eventId']] # this is done to ensure that the changes in the event object is propogated in the for loop.
                    else:
                        existing_event=event_ds[mar_def['eventId']]
                        for runner in mar_def['runners']:
                            aug_key = str(mc['id']) + '_' + str(runner['id'])
                            id_market = mc['id']
                            event_id = mar_def['eventId']
                            event_type_id = mar_def['eventTypeId']
                            betting_type = mar_def['bettingType']
                            market_type = (mar_def['marketType'] if 'marketType' in mar_def else 'None')
                            market_start_time = (mar_def['marketTime'] if 'marketTime' in mar_def else '')
                            bet_delay = mar_def['betDelay']
                            runner_name = runner['name']
                            runner_id = runner['id']
                            runner_status = runner['status']
                            if aug_key not in existing_event.associated_market_instances:
                                new_market_instance = MarketRunnerInstance(id_market,event_id,event_type_id,betting_type,market_type,market_start_time,runner_id,runner_name,runner_status)
                                new_market_instance.runner_status=runner_status
                                new_market_instance.earliest_md = min(new_market_instance.earliest_md,tuple[0])
                                new_market_instance.last_md = max(new_market_instance.earliest_md, tuple[0])
                                existing_event.associated_market_instances[aug_key] = new_market_instance
                            else:
                                existing_instance = existing_event.associated_market_instances[aug_key]
                                existing_instance.earliest_md = min(existing_instance.earliest_md,tuple[0])
                                existing_instance.last_md = max(existing_instance.earliest_md, tuple[0])
                                existing_instance.runner_status=runner_status
                                existing_event.associated_market_instances[aug_key]=existing_instance
                        event_ds[mar_def['eventId']] = existing_event
                        self=existing_event # this is done to ensure that the changes in the event object is propogated in the for loop.
                if 'rc' in mc:
                    for rc in mc['rc']:
                        aug_key = str(mc['id']) + '_' + str(rc['id'])
                        if aug_key not in self.associated_market_instances:
                            print('we need to do something about this')
                        else:
                            existing_instance = self.associated_market_instances[aug_key]
                            existing_instance.earliest_rc=min(existing_instance.earliest_rc,tuple[0])
                            existing_instance.last_rc=max(existing_instance.last_rc,tuple[0])
                            existing_instance.market_instance_actions['instanceid']=existing_instance.market_instance_actions['instanceid']+[aug_key]
                            existing_instance.market_instance_actions['timestamp']=existing_instance.market_instance_actions['timestamp']+[time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(tuple[0]/1000.0))]
                            existing_instance.market_instance_actions['rate']=existing_instance.market_instance_actions['rate']+[rc['ltp']]

        return self

    # credentials for connection to MySQL database in RDS
    def connect_database(self,credential_path = '~/.my.cnf'):
        myDB = URL(drivername='mysql+pymysql', username='root',
                   password=const.AMAZON_PWD,
                   host=const.AMAZON_EP,
                   database=const.AMAZON_DB,
                   query={'read_default_file': credential_path})
        engine = create_engine(myDB)
        return engine

    # uploads all the relevant information that we have processed earlier in to the database in a clear manner.
    def upload_data(self,credential_path = '~/.my.cnf'):
        try:
            engine = self.connect_database()
            event_df = pd.DataFrame(data={'eventId': [self.id_event],'eventName':[self.name_event],'eventDate':[self.event_date.replace("T"," ").replace(".000Z","")]})
            event_df.to_sql('event',con = engine,schema='cbet',if_exists = "append",index = False)
            for inst in self.associated_market_instances:
                mi_data = {'instanceId': [], 'eventId': [], 'bettingType': [], 'marketType': [], 'startTime': [],
                           'endtime': [], 'instancename': [], 'result': [], 'eventdate': []}
                mi_data['instanceId']=mi_data['instanceId']+[self.associated_market_instances[inst].market_instance_id]
                mi_data['eventId']=mi_data['eventId']+[self.associated_market_instances[inst].event_id]
                mi_data['bettingType']=mi_data['bettingType']+[self.associated_market_instances[inst].betting_type]
                mi_data['marketType']=mi_data['marketType']+[self.associated_market_instances[inst].market_type]
                mi_data['startTime']=mi_data['startTime']+[time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.associated_market_instances[inst].earliest_md/1000.0))]
                mi_data['endtime']=mi_data['endtime']+[time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.associated_market_instances[inst].last_md/1000.0))]
                mi_data['instancename']=mi_data['instancename']+[self.associated_market_instances[inst].market_instance_name]
                mi_data['result']=mi_data['result']+[self.associated_market_instances[inst].runner_status]
                mi_data['eventdate']=mi_data['eventdate']+[self.event_date.replace("T", " ").replace(".000Z", "")]
                mid_df = pd.DataFrame(data=mi_data)
                mid_df.to_sql('marketinstance', con=engine, schema='cbet', if_exists="append", index=False)
                mi_transaction=pd.DataFrame(data=self.associated_market_instances[inst].market_instance_actions)
                mi_transaction.drop_duplicates(keep='first',inplace=True)
                mi_transaction.to_sql('instancerate',con = engine,schema='cbet',if_exists = "append",index = False)
            engine.dispose()
        except exc.SQLAlchemyError:
            print('error is raised')

class MarketRunnerInstance:
    def __init__(self,id_market,event_id,event_type_id,betting_type,market_type,market_start_time,runner_id,runner_name,runner_status):
        self.id_market=id_market
        self.event_id=event_id
        self.event_type_id=event_type_id
        self.betting_type=betting_type
        self.market_type=market_type
        self.market_start_time=market_start_time
        self.market_instance_name = runner_name
        self.market_instance_id = runner_id
        self.market_instance_actions={'instanceid':[],'timestamp':[],'rate':[]}
        self.earliest_md = float('inf')
        self.last_md = float('-inf')
        self.earliest_rc = float('inf')
        self.last_rc = float('-inf')
        self.runner_status=runner_status

