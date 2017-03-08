#!/usr/bin/python
import sys, json, os, gzip
from datetime import datetime, date, timedelta
from pprint import pprint, pformat
from bson import json_util

BIN_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'
EXPORT_PATH = '/data/elastic/'

TABLE = 'events'

from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING, errors
client = MongoClient()
db = client.gomlog

today = date.today()
startday = today - timedelta(days=1)
if 1 < len(sys.argv) :
  startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
delta = today - startday

today_ymd = today.strftime('%y%m%d')

class DatetimeEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, datetime):
      return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif isinstance(obj, date):
      return obj.strftime('%Y-%m-%d')
    # Let the base class default method raise the TypeError
    return json.JSONEncoder.default(self, obj)

match = {'req_dir':'/player', 'req_base':{'$ne':''} }
projection_base = { 
  '_id':0 , 'req_dir':1, 'req_base':1, 'ip':1, 'dt':1, 'ua':1, 
  'os':1, 'version':1, 'build':1, "lang":1
} #, 'cc2':1 }
projection = { '_id':0, 'req':0, 'host':0, 'ref':0, 'code':0, 'protocol':0, 'method':0, 'byte':0, 'cc2':0, 'guid':0 }

prod_params = {# {{{
  #'totalpromotion' : '/totalpromotion',
  'cam'    : { "step":1, "license":1, "mode":1, "type":1 },
  'audio'  : { "step":1, "totalusage":1, "mainmenucall":1, "preferencecall":1, "eqcall":1, "viscall":1 },
  'studio' : { "license":1, "mode":1, "type":1 },
  'mix'    : { "step":1 },
  'player' : { "bit":1, "skin":1, "launching":1, "browser":1, "type":1, "font":1 },
}# }}}

prod_req_dirs = {# {{{
  #'totalpromotion' : '/totalpromotion',
  'cam'    : [ '/cam', '/gomcam' ],
  'audio'  : '/audio',
  'studio' : '/studio',
  'mix'    : '/mix',
  #'player' : '/player',
}# }}}

for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
  d_ymd = d.strftime('%y%m%d')
  print ' ---=== '+d_ymd+' ===--- '+str(datetime.now())
  d_dir = EXPORT_PATH + d_ymd + '/'
  if not os.path.exists(d_dir):
    os.makedirs(d_dir)

  for prod in prod_req_dirs :
    ts_start = datetime.now()
    table='_'.join((TABLE,'player' if prod=='player' else 'etc',d_ymd))
    events= db[table]

    json_filepath = d_dir + table + '.'+ prod+'.json.gz'
    file_json = gzip.open(json_filepath, 'w')

    # match
    if str == type(prod_req_dirs[prod]) :
      match['req_dir'] = prod_req_dirs[prod]
    else :
      match['req_dir'] = { '$in': prod_req_dirs[prod] }

    # projection
    #projection = projection_base.copy()
    #projection.update( prod_params[prod] )

    print match,projection
    docs = events.find(match,projection) #.sort("dt",ASCENDING)
    for one in docs :
      #print json.dumps(one, default=json_util.default, cls=DatetimeEncoder)
      one['req_dir'] = prod
      file_json.write(json.dumps(one,cls=DatetimeEncoder)+'\n')

    file_json.close()

    pprint( [prod, str(datetime.now()-ts_start)] )

