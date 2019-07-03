import boto3
import time

from kafka import KafkaProducer

s3 = boto3.client('s3')
s3.list_objects_v2(Bucket='hardik-testing')

EVENT_SET=set()
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def get_all_s3_keys(bucket):

    """Get a list of all keys in an S3 bucket."""

    keys = []
    producer=connect_kafka_producer()
    kwargs = {'Bucket': bucket}
    i=0
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            s=obj['Key']
            if s==".DS_Store":
                continue
            l=s.split("/")
            key=l[0]
            if key not in EVENT_SET:
                EVENT_SET.add(key)
                str1=""
                for obj in boto3.resource('s3').Bucket('hardik-testing').objects.filter(Prefix = key+'/'):
                    if obj.size:
                        str1=str1+"_"+(str(obj.key)).split('/')[1]
                publish_message(producer,'cbet',key,str1)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

if __name__ == '__main__':
    get_all_s3_keys('hardik-testing')