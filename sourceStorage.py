from decimal import Decimal
import json
from psycopg2 import connect
from model import Market, DecimalEncoder
import logging
from logging import config

psqlLogger = logging.getLogger('psql')

config=None
pageSize=10000
connection=None

def getConnectionString():
	return config['connectionString']

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
