from decimal import Decimal
import json
from psycopg2 import connect
from kafka import KafkaProducer
from model import Market, DecimalEncoder
import logging
from logging import config
from multiprocessing import Process


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
psqlLogger = logging.getLogger('psqlstore')
kafkaLogger =  logging.getLogger('kafka-producer')

settings=None
pageSize=10000
producer=None
connection=None

def load_settings():
    with open('settings.json', 'r') as f:
        conf = json.load(f)
    return conf

def getConnectionString():
    return settings['connectionString']

def getConnection():
    return connect(getConnectionString())

def get_market_names():
    cmd='SELECT DISTINCT market FROM marketshistory ORDER BY market;'
    with connection.cursor() as cur:
        cur.execute(cmd)
        r = cur.fetchall()

    for i in r:
        yield i[0]

def getMarketInfo(market, cursor):
    psqlLogger.info(f'Fetching market {market} at {cursor}')
    cmd=''' SELECT id, date, market, initialprice, price, high, low, volume, bid, ask
            FROM marketshistory
            WHERE market = %s AND id > %s
            LIMIT %s;'''
    with connect(getConnectionString()) as con, con.cursor() as cur:
        cur.execute(cmd, (market, cursor, pageSize))
        r = cur.fetchall()
    for i in r:
        yield Market(*i)

def initializeProducer():
    global producer
    conf = { 
            'bootstrap.servers': settings['kafka']['servers'],
            'client.id': 'producer1'
            }
    producer = KafkaProducer(
            bootstrap_servers=conf['bootstrap.servers'],
            value_serializer=lambda v: json.dumps(v.__dict__, cls=DecimalEncoder).encode('utf-8'))
    kafkaLogger.info(f'{producer.bootstrap_connected()} {producer.metrics()}')
    kafkaLogger.info(

def kafkaCallback(err, msg):
    if err is not None:
        kafkaLogger.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        kafkaLogger.info("Message produced: %s" % (str(msg)))

def emitEvent(marketdetail):
    #kafkaLogger.info('pushed to kafka')
    producer.send(settings['kafka']['topic'], key=bytearray(marketdetail.market, 'utf-8'), value=marketdetail)

def emitEventFromMarketList(markets, jobIndex, jobCount):
    for i in range(jobIndex, len(markets), jobCount):
        cursor = 0
        hasRecord=True
        while hasRecord:
            hasRecord=False
            r=getMarketInfo(markets[i], cursor)
            for msg in r:
                emitEvent(msg)
                cursor=msg.id
                hasRecord=True

def emitEventFromMarketList2(markets, jobIndex, jobCount):
    global settings
    global connection
    settings=load_settings()
    connection=getConnection()
    initializeProducer()
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
                    emitEvent(msg)
                    cursor=msg.id
                    hasRecord=True
            marketSet[markets[i]] = (cursor, hasRecord)
        alldone = not any([v[1] for v in marketSet.values() ])

settings=load_settings()
connection=getConnection()
#initializeProducer()

if __name__ == '__main__':
    markets = list(get_market_names())
    jobs = []
    jobCount = 4
    for i in range(0, jobCount):
        t=Process(name=f'{i}', target=emitEventFromMarketList2, args=(markets, i, jobCount), daemon=True)
        jobs.append(t)
        t.start()

    for job in jobs:
        job.join()
#markets = list(get_market_names())
#emitEventFromMarketList2(markets, 0, 1)
