#!/usr/bin/python
import sys, json, glob, os, re, urlparse, gzip
from datetime import datetime, date, timedelta
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

TABLE = 'events'

from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING, errors
client = MongoClient()
db = client.gomlog

pat = re.compile( '([(\d\.)]+) - - \[(.*?)\] "([^\s]*?) ([^\s]*?)( [^\s]*?)?" (\d+) (-|\d+) "(-|.*?)" "(.*?)"' )

def analyze (host,line):# {{{
  global dt_last, dt_seq
  found = pat.findall(line)
  #print found
  if not found :
    print ('failed to parse req_url : ' + line);
    return None
  elif len(found[0]) < 9 :
    print ('insufficient req_url : ' + line);
    print found[0]
    return None

  (ip,dt_old,method,req,protocol,code,byte,ref,ua) = found[0]

  dt = datetime.strptime(dt_old[:dt_old.find(' ')],'%d/%b/%Y:%H:%M:%S')

  oReq = urlparse.urlparse(req)
  req_dir, req_base = os.path.split(oReq.path)
  req_query = oReq.query if oReq.query else None
  cc2 = gi.country_code_by_addr(ip)
  if not cc2 :
    cc2 = 'KR'

  dic_mongo = {
    'host'     : host,
    'ip'       : ip,
    'method'   : method,
    'req'      : req,
    'protocol' : protocol.strip(),
    'code'     : int(code),
    'byte'     : int(byte) if byte!='-' else None,
    'ref'      : ref,
    'ua'       : ua,
  }

  if dt_last != dt :
    dt_seq = 0
    dt_last = dt
  else :
    dt_seq += 1

  dic_inf = { 
    'measurement': '_'.join( (TABLE, 'player' if req_dir=='/player' else 'etc', dt.strftime('%y%m%d')) ),
    'tags': { },
    'time': int( (dt+timedelta(hours=9)).strftime('%s') + "%03d%03d"%(dt_seq,int(host[host.rfind('.')+1:])) ), # force revert KST->UTC
    'fields': dic_mongo
  }
  if req_dir :
    if req_dir == '/player' :
      dic_inf['fields']['req_dir'] = req_dir
    else :
      dic_inf['tags']['req_dir'] = req_dir
  if req_base :
    dic_inf['tags']['req_base'] = req_base
  if cc2 :
    dic_inf['tags']['cc2'] = cc2

  dic_mongo.update( {
    'dt'       : dt,
    'cc2'      : cc2,
    'req_dir'  : req_dir,
    'req_base' : req_base,
  } )

  if req_query :
    dic_query = dict( ( k.replace('.','_'), v if type(v)==str else v[0] ) for k, v in urlparse.parse_qs(req_query).iteritems() )
    dic_mongo.update( dic_query )
    dic_inf['fields'].update( dic_query )

  line = "%s,%s %s %s\n"% ( 
    dic_inf['measurement'], 
    ','.join( [ k+'='+str(v) for k, v in dic_inf['tags'].iteritems() ] ),
    ','.join( [ k+'='+(str(v) if int==type(v) else '"'+str(v)+'"') for k, v in dic_inf['fields'].iteritems() ] ),
    str(dic_inf['time'])+'000', 
  )
  influx_gz.write(line);

  return dic_mongo
# }}}

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

  events_player = db['_'.join((TABLE,'player',d_ymd))]
  IDX_req_base = IndexModel([("req_base", ASCENDING),("cc2", ASCENDING)], name="IDX_req_base_cc2")
  events_player.create_indexes ( [IDX_req_base] )

  events_etc = db['_'.join((TABLE,'etc',d_ymd))]
  IDX_req_base = IndexModel([("req_dir", ASCENDING),("req_base", ASCENDING),("cc2", ASCENDING)], name="IDX_req_dir_base_cc2")
  events_etc.create_indexes ( [IDX_req_base] )

  influx_file = BIN_PATH + "log/influx_event.%s.gz" % d_ymd
  influx_gz = gzip.open(influx_file, 'wb')
  influx_gz.write("# DDL\nCREATE DATABASE gomlog\n# DML\n# CONTEXT-DATABASE: gomlog\n")

  file_pattern = '/data/log/log.gomlab.com/*' + d_ymd + '.access_log.log.gomlab.com'
  for onefile in sorted(glob.glob(file_pattern)) :
    filename = os.path.basename(onefile)
    host = filename[: filename.find('.2017') ]
    with open(onefile, 'r+') as f:
      try :
        print( '---=== '+filename+' ===---' )
        i = 0
        dt_last = None
        dt_seq = -1
        ts_start = datetime.now()
        ts_tmp = datetime.now()
        arr_player = []
        arr_etc = []
        for line in f:
          one = analyze(host,line)
          if not one :
            continue

          if one['req_dir'] == '/player' :
            arr_player.append( one )
          else :
            arr_etc.append( one )

          i+=1
          if 0 == (i % 1000) :
            events_player.insert_many(arr_player)
            events_etc.insert_many(arr_etc)
            arr_player = []
            arr_etc = []
          if 0 == (i % 100000) :
            print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
            ts_tmp = datetime.now()
        if len(arr_player) :
          events_player.insert_many(arr_player)
        if len(arr_etc) :
          events_etc.insert_many(arr_etc)
        log = "end: %s\t%d\t%s" % (filename, i, str(datetime.now()-ts_start) )
        print log
        #influx_file.close()
      except errors.PyMongoError as err :
        log_daily.warning( pformat([err,arrVals], indent=4) )
    ## END : with gzip.open(onefile, 'r+') as f:
    os.system( "gzip "+onefile )  

  ## END : one day
  influx_gz.close()
  if 62 == os.path.getsize(influx_file) :
    os.remove(influx_file)
  if 0 == os.path.getsize(log_daily_file) :
    os.remove(log_daily_file)

    #os.system( "influx -import -path=influx.event.170122 -precision=s &" )

  exit(1) # only one day

