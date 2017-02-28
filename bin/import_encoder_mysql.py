#!/usr/bin/python
import sys, json, glob, os, mysql.connector, re, urlparse, gzip
from datetime import datetime, date, timedelta
from mysql.connector import errorcode
from ua_parser import user_agent_parser
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

import GeoIP
gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)

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

TABLE = 'archive_encoder'

with open(BIN_PATH+'../config/tf.json') as default_file:    
  default = json.load(default_file)

try:
  config['host'] = default['mysql']['host']
  config['user'] = default['mysql']['user']
  config['password'] = default['mysql']['password']
  config['database'] = default['mysql']['database']
  cnx = mysql.connector.connect(**config)

  cursor = cnx.cursor() #prepared=True
  cursor.execute('SET sql_log_bin = 0')
  #cursor.execute('LOCK TABLE '+TABLE+' WRITE')

except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
    exit(1)
#else:
#  print('nothing')
#  cnx.close()
#  exit(1)

sql_format = '''INSERT IGNORE INTO _TABLE_ 
  ( host, ip, dt, method, req, protocol, code, byte, ref, ua, cc2, req_dir, req_base, req_query ) 
VALUES 
  (   %s, %s, %s,     %s,  %s,       %s,   %s,   %s,  %s, %s,  %s,      %s,       %s,        %s )'''
sql_insert = sql_format.replace('_TABLE_',TABLE)

pat = re.compile( '([(\d\.)]+) - - \[(.*?)\] "([^\s]*?) ([^\s]*?)( [^\s]*?)?" (\d+) (-|\d+) "(-|.*?)" "(.*?)"' )

def analyze (host,line):
  found = pat.findall(line)
  #print found
  if not found :
    print ('failed to parse req_url : ' + line);
    return
  elif len(found[0]) < 9 :
    print ('insufficient req_url : ' + line);
    print found[0]
    return

  (remote,dt_old,method,req,protocol,code,byte,ref,ua) = found[0]
  protocol = protocol.strip()
  code = int(code)
  cc2 = gi.country_code_by_addr(remote)
  if not cc2 :
    cc2 = 'KR'
  dt = datetime.strptime(dt_old[:dt_old.find(' ')],'%d/%b/%Y:%H:%M:%S')
  if byte == '-' :
    byte = None

  oReq = urlparse.urlparse(req)
  req_dir, req_base = os.path.split(oReq.path)
  req_query = oReq.query if oReq.query else None

  param = ( host, remote, dt, method, req, protocol, code, byte, ref, ua, cc2, req_dir, req_base, req_query )
  try :
    cursor.execute(sql_insert, param )
  except mysql.connector.Error as err:
    log_daily.warning( pformat([err,param], indent=4) )

today = date.today()
startday = today - timedelta(days=1)
if 1 < len(sys.argv) :
  startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
delta = today - startday

today_ymd = today.strftime('%y%m%d')
filename = os.path.basename(__file__)
log_file = BIN_PATH + "log/%s.%s"%(filename[:filename.rfind('.')],today_ymd)

for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
  d_ymd = d.strftime('%y%m%d')
  log_daily_file = log_file+'.'+d_ymd
  setup_logger('log'+d_ymd, log_daily_file )
  log_daily = logging.getLogger('log'+d_ymd)

  sql="SELECT 1 FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND PARTITION_NAME=%s";
  try :
    cursor.execute(sql, (config['database'],TABLE,'p'+d_ymd) )
    if not cursor.fetchone() :
      d1 = d + timedelta(days=1)
      sql="ALTER TABLE "+TABLE+" ADD PARTITION ( PARTITION p"+d_ymd+" VALUES LESS THAN ('"+d1.strftime('%y%m%d')+"') )";
      cursor.execute(sql)
  except mysql.connector.Error as err:
    print(err)
    exit(1)

  file_pattern = '/data/log/encoder.gomtv.com/*' + d_ymd + '.access_log.encoder.gomtv.com'
  for onefile in sorted(glob.glob(file_pattern)) :
    filename = os.path.basename(onefile)
    host = filename[: filename.find('.2017') ]
    with open(onefile, 'r+') as f:
      print( '---=== '+filename+' ===---' )
      i = 0
      ts_tmp = ts_start = datetime.now()
      arrVals = []
      for line in f:
        analyze(host,line)
        i+=1
        if 0 == (i % 100000) :
          print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
          ts_tmp = datetime.now()
      log = "end: %s\t%d\t%s" % (filename, i, str(datetime.now()-ts_start) )
      print log
    # end with open
    # os.system( "gzip "+onefile )  

  if 0 == os.path.getsize(log_daily_file) :
    os.remove(log_daily_file)

cursor.close()
cnx.close()
