from model import DecimalEncoder
from producer import load_settings
from sourceStorage import get_market_names, getMarketInfo, getConnection
import sourceStorage
import pika, json

def load_settings():
    with open('settings.json', 'r') as f:
        conf = json.load(f)
    return conf

settings=load_settings()
sourceStorage.connection = getConnection(settings['connectionStringUrl'])

connection = pika.BlockingConnection(
	pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='market')

market_names = list(get_market_names())
for msg in getMarketInfo(market_names[0], 0):
	payload= json.dumps(msg.__dict__, cls=DecimalEncoder)
	channel.basic_publish(exchange='', routing_key='market', body=payload)

connection.close()

