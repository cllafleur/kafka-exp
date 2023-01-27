from setuptools import setup, find_packages

setup(name='kafka-exp',
	version='0.1',
	packages=find_packages(),
	install_requires=[
		'psycopg2-binary',
		'kafka-python',
		'pugsql',
		'pika'
	])