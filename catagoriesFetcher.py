from __future__ import print_function

import json
from collections import defaultdict
import argparse
import json
import pprint
import requests
import sys
import urllib
import multiprocessing
import random

from threading import Lock

from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode

lock = Lock()

def getAuth():
	keys = ["...(PUT API KEYS)"]
	return random.choice(keys)

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'


SEARCH_LIMIT = 30

def request(host, path, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % getAuth(),
    }
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()

def get_business(business_id):
    business_path = BUSINESS_PATH + business_id
    return request(API_HOST, business_path)


businesses = open("business.json").readlines()

fetchedResults = { json.loads(i)["business_id"]:0 for i in open("result.json").readlines()  }

a = open("processed").read().split(";")

for i in a:
	fetchedResults[i] = 0

def work(bid):
	response = get_business(bid)
	if(not 'error' in response):
		obj = {"business_id": bid,"categories":  response["categories"]}
		try:
			lock.acquire()
			print(json.dumps(obj))
		finally :
			lock.release()
		
		return ""
	else:

		return bid

i = 0

bids = []
for line in businesses:
	bid = json.loads(line)['business_id']
	if not (bid in fetchedResults):
		bids.append(bid)

bids = bids
pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
l = pool.map(work, bids)