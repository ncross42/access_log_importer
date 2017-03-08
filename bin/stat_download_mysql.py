#!/usr/bin/python
import sys, json, glob, os, mysql.connector
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

BIN_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'

config = {
  'host': '127.0.0.1',
  'user': 'root',
  'password': '',
  'database': '',
  'autocommit' : True,
  'get_warnings': True,
  'raise_on_warnings': True,
  #'use_pure': False,
}

tableLog = 'access_download_archive'
tableStat = 'prd_daily_stat'

with open(BIN_PATH+'../config/production.json') as default_file:    
  default = json.load(default_file)

try:
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

def normalize_stat (ymd, prod, pat) :
  sql='''
  INSERT INTO %s ( d, product, action, cc2, tot, uni )
  SELECT '%s', '%s', 'download', cc2, COUNT(1) tot, COUNT(DISTINCT ip) uni 
    FROM %s partition(p%s)
    WHERE req_base LIKE '%s' 
    GROUP BY cc2''' % (tableStat, ymd, prod, tableLog, ymd, pat)
  try :
    cursor.execute(sql)
  except mysql.connector.Error as err:
    log_batch.error( pformat([err,sql], indent=4) )

prod_pattern = {# {{{
  'player'  : '%gomplayer%.exe',
  'audio'   : '%gomaudio%.exe',
  'pack'    : '%gompack%.exe',
  'recoder' : '%gomrecoder%.exe',
  'encoder' : '%gomencoder%.exe',
  'tv'	    : '%gomtv%.exe',
  'remote'  : '%gomremote%.exe',
  'cam'	    : '%gomcam%.exe',
  'studio'  : '%gomstudio%.exe',
  'mix'	    : '%gommix%.exe',
  'anti'    : '%gomanti%.exe',
  'bridge'  : '%gombridge%.exe',
  'helper'  : '%gomhelper%.exe'
}# }}}

if __name__ == "__main__":
  today = date.today()
  today_ymd = today.strftime('%y%m%d')
  filename = os.path.basename(__file__)
  log_file = BIN_PATH + "log/%s.%s"%(filename[:filename.rfind('.')],today_ymd)
  setup_logger('log_batch', log_file )
  log_batch = logging.getLogger('log_batch')

  startday = today - timedelta(days=1)
  if 1 < len(sys.argv) :
    startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
  delta = today - startday

  for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
    ymd = d.strftime('%y%m%d')
    print ymd
    for prod in prod_pattern :
      ts_start = datetime.now()
      normalize_stat ( ymd, prod, prod_pattern[prod] )
      log = "end: %s\t%s" % (prod, str(datetime.now()-ts_start) )
      print log

cursor.close()
cnx.close()
