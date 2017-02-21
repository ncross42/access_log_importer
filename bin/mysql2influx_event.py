#!/usr/bin/python
import sys, json, glob, os, mysql.connector, re
from datetime import datetime, date, timedelta
from mysql.connector import errorcode
import urlparse
from ua_parser import user_agent_parser
from pprint import pprint, pformat
import GeoIP
import logging
def setup_logger(logger_name, log_file, level=logging.ERROR ) :# {{{
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

from influxdb import InfluxDBClient

BIN_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'

gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)

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

with open(BIN_PATH+'../config/default.json') as default_file:    # {{{
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
#else:
#  print('nothing')
#  cnx.close()
#  exit(1)
# }}}

prod_pattern = {# {{{
  'player' : '/player',
  'audio'  : '/audio',
  'cam'    : [ '/cam', '/gomcam' ],
  'studio' : '/studio',
  'mix'    : '/mix'
}# }}}

influx = InfluxDBClient('localhost', 8086, '', '', 'gomlog')

today = date.today()
startday = today - timedelta(days=1)
if 1 < len(sys.argv) :
  startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
delta = today - startday

filename = os.path.basename(__file__)
log_file = BIN_PATH + "log/%s.%s"%(filename[:filename.rfind('.')],today.strftime('%y%m%d'))

for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
  d_ymd = d.strftime('%y%m%d')
  setup_logger('log'+d_ymd, log_file+'.'+d_ymd )
  log_daily = logging.getLogger('log'+d_ymd)

  sql="SELECT * FROM archive_event PARTITION (p%s)" % d_ymd
  try :
    print 'start: ' + d_ymd
    ts_tmp = ts_start = datetime.now()
    i = 0
    json_array = []
    cursor.execute(sql)
    for ( host, remote, dt, method, req, protocol, code, byte, ref, ua, cc2, req_dir, req_base, req_query, ref_host, ref_path, ref_query, ua_fam_maj, ua_full, os_fam_maj, os_full, dev_full, ua_gom ) in cursor:
      if not req_dir in ['/audio','/cam','/gomaudio','/gomcam','/mix','/player','/player64','/studio','/subtitle','/totalpromotion'] :
        log_daily.warning( pformat([str(dt),remote,req], indent=4) )
        continue

      json_one = { 
        'measurement': 'events',
        'tags': { 'req_dir':req_dir, 'req_base':req_base, 'cc2':cc2 },
        'time': int(dt.strftime('%s')),
        'fields': { 'req_dir':req_dir, 'req_base':req_base, 'cc2':cc2, 'host':host, 'remote':remote, 'ua_gom':ua_gom, 'byte':byte, 'ref':ref, 'ua':ua }
      }
      if req_query :
        qs = dict( (k, v if len(v)>1 else v[0] ) for k, v in urlparse.parse_qs(req_query).iteritems() )
        json_one['fields'].update( qs )

      json_array.append(json_one)
      i+=1
      if 0 == (i % 10000) :
        if not influx.write_points(json_array,'s') :
          pprint(json_array)
        json_array = []
        print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
        ts_tmp = datetime.now()
    if json_array :
      influx.write_points(json_array)
      print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
      ts_tmp = datetime.now()
    log = "end: %s\t%d\t%s" % (filename, i, str(datetime.now()-ts_start) )
    print log
  except mysql.connector.Error as err:
    print err, sql
    exit(1)

cursor.close()
cnx.close()
