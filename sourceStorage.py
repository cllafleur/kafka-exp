from decimal import Decimal
from model import Market, DecimalEncoder
import logging
from logging import config
import pugsql


psqlLogger = logging.getLogger('psql')

pageSize=10000
connection=None

queries = pugsql.module('queries/')

def getConnection(connectionString):
	return queries.connect(connectionString)

def get_market_names():
	r=queries.get_markets()
	for i in r:
		yield i['market']

def getMarketInfo(market, cursor):
	psqlLogger.info(f'Fetching market {market} at {cursor}')
	r=queries.get_markethistory(market=market, offset=cursor, page_size=pageSize)
	for i in r:
		yield Market(**i)
