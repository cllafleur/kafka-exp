from kafka import KafkaConsumer, TopicPartition
import kafka
import logging
from logging import config
import json
import threading
import getopt, sys

log_config = {
        "version": 1,
        "root":{
            "handlers": ["console"],
            "level": "INFO"
            },
        "handlers": {
            "console":{
                "formatter": "std_out",
                "class": "logging.StreamHandler",
                "level": "INFO"
                }
            },
        "formatters":{
            "std_out":{
                "format": "%(asctime)s [%(levelname)s][%(threadName)s] %(name)s: %(message)s"
                }
            }
        }

config.dictConfig(log_config)
kafkaLogger = logging.getLogger('consumer')

conf=None
consumer=None

partitionNumber=None
topicName=None

def getoptions():
    global partitionNumber
    global topicName
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hp:t:', ['help', 'partitions=', 'topic='])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            print('usage')
            sys.exit()
        elif o in ('-p', '--partitions'):
            try:
                partitionNumber = [int(i) for i in a.split(',')]
            except:
                print('Error: partitions has no valid parameters', file= sys.stderr)
                sys.exit(2)
        elif o in ('-t', '--topic'):
            topicName = a
        else:
            assert False, "unhandled option"


def load_config():
    with open('settings.json', 'r') as f:
        c = json.load(f)
    return c

def initialize_consumer():
    global consumer
    consumer = KafkaConsumer(bootstrap_servers=conf['kafka']['servers'], group_id=conf['kafka']['consumers']['group_id'])
    if partitionNumber:
        consumer.assign([TopicPartition(topicName, p) for p in partitionNumber])
    else:
        consumer.subscribe(topicName)
    kafkaLogger.info(f'bootstrap is connected {consumer.bootstrap_connected()}')
    kafkaLogger.info(f'Current subscription {consumer.subscription()}')
    kafkaLogger.info(f'Current assignment {consumer.assignment()}')

prevcpt=0
timer=1
speed=0
cpt=0
def printStatus():
    partition= [i.partition for i in consumer.assignment()]
    try:
        print(f'[partition: {partition}] {cpt}  {speed: >10} msg/s \r', end='')
    except:
        print('\r', end='')

def update_counter():
    global prevcpt
    global speed
    c=cpt-prevcpt
    speed=c/timer
    prevcpt=cpt
    printStatus()
    threading.Timer(timer, update_counter).start()

conf=load_config()
topicName=conf['kafka']['topic']
getoptions()
initialize_consumer()
handled=set()
threading.Timer(timer, update_counter).start()

for msg in consumer:
    if not msg.key:
        continue
    key = msg.key.decode('utf-8')
    cpt=cpt+1
    if key not in handled:
        handled.add(key)	
        kafkaLogger.info(f'[partition: {msg.partition}] {key}')
        printStatus()
