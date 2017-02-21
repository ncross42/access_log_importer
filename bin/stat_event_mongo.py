#!/usr/bin/python
import sys, json, glob, os, mysql.connector, re, pymongo
from datetime import datetime, date, timedelta
from mysql.connector import errorcode
from pprint import pprint, pformat
import logging

def setup_logger(logger_name, log_file, level=logging.INFO) :# {{{
  l = logging.getLogger(logger_name)
  formatter = logging.Formatter('%(asctime)s : %(message)s')
  fileHandler = logging.FileHandler(log_file, mode='w')
  fileHandler.setFormatter(formatter)
  streamHandler = logging.StreamHandler()
  streamHandler.setFormatter(formatter)

  l.setLevel(level)
  l.addHandler(fileHandler)
  l.addHandler(streamHandler)    
# }}}

mongo = pymongo.MongoClient()
gomlog = mongo.gomlog

BIN_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'

config = {# {{{
  'host': '127.0.0.1',
  'user': 'root',
  'password': '',
  'database': '',
  'autocommit' : True,
  'get_warnings': True,
  'raise_on_warnings': True,
  #'use_pure': False,
}# }}}

tableLog = 'archive_event'
tableStat = 'prd_daily_stat'

with open(BIN_PATH+'../config/default.json') as default_file:    
  default = json.load(default_file)

try:# {{{
  config['host'] = default['mysql']['host']
  config['user'] = default['mysql']['user']
  config['password'] = default['mysql']['password']
  config['database'] = default['mysql']['database']
  cnx = mysql.connector.connect(**config)
  cursor = cnx.cursor() #prepared=True
  cursor.execute('SET sql_log_bin = 0')
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
    exit(1)
# }}}

def import_stat (d, prod, pat) :
  global gomlog
  table = '_'.join( ('events', 'player' if prod=='player' else 'etc', d_ymd ) )
  collection = gomlog[table]

  # default : player
  if prod == 'player' :
    pipe = [
      {'$group':{
        '_id':{'base':'$req_base','c2':'$cc2','ip':'$ip'},
        'cntbyip':{'$sum':1}
      }},
      {'$group':{
        '_id':{'base':'$_id.base','c2':'$_id.c2'},
        'uni':{'$sum':1},
        'tot':{'$sum':'$cntbyip'}
      }},
      {'$sort':{'tot':-1}},
    ]
  else :
    pipe = [
      {'$match':{'req_dir':{'$in': [pat] if str==type(pat) else pat } }},
      {'$group':{
        '_id':{'base':'$req_base','c2':'$cc2','ip':'$ip'},
        'cntbyip':{'$sum':1}
      }},
      {'$group':{
        '_id':{'base':'$_id.base','c2':'$_id.c2'},
        'uni':{'$sum':1},
        'tot':{'$sum':'$cntbyip'}
      }},
      {'$sort':{'tot':-1}},
    ]

  arr_values = []
  for row in collection.aggregate(pipeline=pipe, allowDiskUse=True ) :
    arr_values.append( (d, prod, row['_id']['base'], row['_id']['c2'], row['tot'], row['uni']) )

  sql='''INSERT INTO prd_daily_stat 
    ( d, product, action, cc2, tot, uni ) VALUES
    ( %s,     %s,     %s,  %s,  %s,  %s )'''
  try :
    cursor.executemany(sql, arr_values)
  except mysql.connector.Error as err:
    log_daily.error( pformat([err,sql,arr_values], indent=4) )

prod_pattern = {# {{{
  'player'         : [ 'install', 'cancel', 'uninstall', 'playing', 'family', 'font', 'setting', 'update' ],
  'audio'          : [ 'install', 'cancel', 'uninstall', 'playing', 'action', 'playlog', 'synclyriceditor' ],
  'cam'            : [ 'install', 'cancel', 'uninstall' ],
  'gomcam'         : [ 'action', 'play' ],
  'studio'         : [ 'install', 'cancel', 'uninstall', 'playing', 'action' ],
  'mix'            : [ 'install', 'cancel', 'uninstall' ],
  'totalpromotion' : [ 'install', 'cancel', 'view' ]
}# }}}

prod_pattern = {# {{{
  #'totalpromotion' : '/totalpromotion',
#  'cam'    : [ '/cam', '/gomcam' ],
#  'audio'  : '/audio',
#  'studio' : '/studio',
#  'mix'    : '/mix',
  'player' : '/player',
}# }}}

if __name__ == "__main__":
  today = date.today()
  today_ymd = today.strftime('%y%m%d')
  filename = os.path.basename(__file__)
  log_file = BIN_PATH + "log/%s.%s"%(filename[:filename.rfind('.')],today_ymd)

  startday = today - timedelta(days=1)
  if 1 < len(sys.argv) :
    startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
  delta = today - startday

  collections = gomlog.collection_names()

  for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
    d_ymd = d.strftime('%y%m%d')

    # check exists gomlog.events_player_(d_ymd)
    if 'events_player_'+d_ymd not in collections :
      print( '---=== '+d_ymd+' : SKIPPED ===---' )
      continue

    log_daily_file = log_file+'.'+d_ymd
    setup_logger('log'+d_ymd, log_daily_file )
    log_daily = logging.getLogger('log'+d_ymd)
    print d_ymd

    print( '---=== '+d_ymd+' ===---' )
    for prod in prod_pattern :
      ts_start = datetime.now()
      import_stat ( d, prod, prod_pattern[prod] )
      log = "end: %s\t%s" % (prod, str(datetime.now()-ts_start) )
      print log
    ## END for prod

    if 0 == os.path.getsize(log_daily_file) :
      os.remove(log_daily_file)
  ## END for d

cursor.close()
cnx.close()
