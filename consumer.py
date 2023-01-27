from abc import abstractmethod
from kafka import KafkaConsumer, TopicPartition
import pika
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

partitionNumber=None
topicName=None
broker=None
consumer=None

def getoptions():
    global partitionNumber
    global topicName
    global broker
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hp:t:b:', ['help', 'partitions=', 'topic=', 'broker='])
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
        elif o in ('-b', '--broker'):
            broker = a
        else:
            assert False, "unhandled option"


def load_config():
    with open('settings.json', 'r') as f:
        c = json.load(f)
    return c

prevcpt=0
timer=1
speed=0
cpt=0
def printStatus():
    partition= consumer.getAssignment()
    try:
        print(f'[partition: {partition}] {cpt}  {speed: >10} msg/s \r', end='')
    except:
        print('\r', end='')

class MessageBroker:
    def __init__(self, settings):
        self.settings= settings

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def getMessages(self):
        pass

    def getAssignment(self):
        return ['']

class KafkaBroker(MessageBroker):
    def initialize(self):
        self.consumer = KafkaConsumer(bootstrap_servers=self.settings['kafka']['servers'], group_id=self.settings['kafka']['consumers']['group_id'])
        if partitionNumber:
            self.consumer.assign([TopicPartition(topicName, p) for p in partitionNumber])
        else:
            self.consumer.subscribe(topicName)
        kafkaLogger.info(f'bootstrap is connected {self.consumer.bootstrap_connected()}')
        kafkaLogger.info(f'Current subscription {self.consumer.subscription()}')
        kafkaLogger.info(f'Current assignment {self.consumer.assignment()}')
    
    def getMessages(self):
        for msg in self.consumer:
           yield (msg.key, msg.value, msg.partition) 
    
    def getAssignment(self):
        return [i.partition for i in self.consumer.assignment()]

class RabbitmqBroker(MessageBroker):
    def initialize(self):
        parameters = pika.ConnectionParameters(host=self.settings['rabbitmq']['host'])
        self.connection = pika.BlockingConnection(parameters)        
        self.channel = self.connection.channel()

    def getMessages(self):
        queue_name = self.settings['rabbitmq']['queueName']
        for method_frame, properties, body in self.channel.consume(queue_name, True):
            #self.channel.basic_ack(method_frame.delivery_tag)
            yield (None, body, None)

brokers = {
    'kafka': KafkaBroker,
    'rabbitmq': RabbitmqBroker
}

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
consumer=brokers[broker](conf)
consumer.initialize()
handled=set()
threading.Timer(timer, update_counter).start()

for k, b, p in consumer.getMessages():
    if not k:
        cpt=cpt+1
        #print(b)
        continue
    key = k.decode('utf-8')
    cpt=cpt+1
    if key not in handled:
        handled.add(key)	
        kafkaLogger.info(f'[partition: {p}] {key}')
        printStatus()
