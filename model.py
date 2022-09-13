from datetime import datetime
import json
from decimal import *

class Market:
	id=None
	date=None
	market=None
	initialprice=None
	price=None
	high=None
	low=None
	volume=None
	bid=None
	ask=None

	def __init__(self, id, date, market, initialprice, price, high, low, volume, bid, ask):
		self.id=id
		self.date=date
		self.market=market
		self.initialprice=initialprice
		self.price=price
		self.high=high
		self.low=low
		self.volume=volume
		self.bid=bid
		self.ask=ask

	@property
	def getDate(self):
		return datetime.fromtimestamp(self.date)
	
	def __repr__(self):
		return f'{self.id} {self.market} {self.price}'


class DecimalEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, Decimal):
			return str(obj)
		return json.JSONEncoder.default(self,obj)