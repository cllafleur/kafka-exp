import sys, getopt
from abc import abstractmethod
from decimal import Decimal
import json
from psycopg2 import connect
from kafka import KafkaProducer
import pika
from model import Market, DecimalEncoder
import logging
from logging import config
from multiprocessing import Process
from sourceStorage import get_market_names, getMarketInfo, getConnection

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
brokerLogger =  logging.getLogger('kafka-producer')

settings=None
pageSize=10000
connection=None
selected_broker='kafka'

def getoptions():
    broker=None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hb:', ['help', 'broker='])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            print('usage')
            sys.exit()
        elif o in ('-b', '--broker'):
            broker = a
        else:
            assert False, 'unhandled option'
    return broker

def load_settings():
    with open('settings.json', 'r') as f:
        conf = json.load(f)
    return conf

def getConnectionString():
    return settings['connectionString']

class MessageBroker:
    def __init__(self, settings):
        self.settings = settings

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def publish(self, payload):
        pass

class KafkaBroker(MessageBroker):

    def initialize(self):
        conf = { 
                'bootstrap.servers': self.settings['kafka']['servers'],
                'client.id': 'producer1'
                }
        self.producer = KafkaProducer(
                bootstrap_servers=conf['bootstrap.servers'],
                value_serializer=lambda v: json.dumps(v.__dict__, cls=DecimalEncoder).encode('utf-8'))
        brokerLogger.info(f'{self.producer.bootstrap_connected()} {self.producer.metrics()}')

    def publish(self, marketdetail):
        self.producer.send(settings['kafka']['topic'], key=bytearray(marketdetail.market, 'utf-8'), value=marketdetail)

class RabbitmqBroker(MessageBroker):
    def initialize(self):
        parameters=pika.ConnectionParameters(host=self.settings['rabbitmq']['host'])
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.queue_name = self.settings['rabbitmq']['queueName']
        self.channel.queue_declare(self.queue_name)
    
    def publish(self, payload):
        p= json.dumps(payload.__dict__, cls=DecimalEncoder)
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=p)

brokers= {
    'kafka': KafkaBroker,
    'rabbitmq': RabbitmqBroker
}

def emitEventFromMarketList(markets, jobIndex, jobCount, broker):
    global settings
    global connection
    settings=load_settings()
    connection=getConnection(getConnectionString())
    producer = brokers[broker](settings)
    producer.initialize()
    marketSet=dict()
    alldone = False
    while not alldone:
        for i in range(jobIndex, len(markets), jobCount):
            if markets[i] not in marketSet:
                marketSet[markets[i]] = (0, True)
            cursor, hasRecord = marketSet[markets[i]]
            if hasRecord:
                hasRecord=False
                r=getMarketInfo(markets[i], cursor)
                for msg in r:
                    producer.publish(msg)
                    cursor=msg.id
                    hasRecord=True
            marketSet[markets[i]] = (cursor, hasRecord)
        alldone = not any([v[1] for v in marketSet.values() ])

settings=load_settings()
connection=getConnection(getConnectionString())
selected_broker=getoptions()
#initializeProducer()

if __name__ == '__main__':
    markets = list(get_market_names())
    jobs = []
    jobCount = 4
    for i in range(0, jobCount):
        t=Process(name=f'{i}', target=emitEventFromMarketList, args=(markets, i, jobCount, selected_broker), daemon=True)
        jobs.append(t)
        t.start()

    for job in jobs:
        job.join()
#markets = list(get_market_names())
#emitEventFromMarketList2(markets, 0, 1)
