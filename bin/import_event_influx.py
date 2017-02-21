#!/usr/bin/python
import sys, json, glob, os, re, urlparse ,influxdb, gzip
from datetime import datetime, date, timedelta
from pprint import pprint, pformat
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

BIN_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'

import GeoIP
gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)

TABLE = 'events'

influx = influxdb.InfluxDBClient('localhost', 8086, '', '', 'gomlog')

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

  if req_query :
    qs = dict( (k, v if len(v)>1 else v[0] ) for k, v in urlparse.parse_qs(req_query).iteritems() )
    dic_inf['fields'].update( qs )

  #dic_mongo.update( {
  #  'dt'       : dt,
  #  'req_dir'  : req_dir,
  #  'req_base' : req_base,
  #} )

  if req_query :
    dic_query = dict( ( k.replace('.','_'), v if type(v)==str else v[0] ) for k, v in urlparse.parse_qs(req_query).iteritems() )
    #dic_mongo.update( dic_query )
    dic_inf['fields'].update( dic_query )

  line = "%s,%s %s %s\n"% ( 
    dic_inf['measurement'], 
    ','.join( [ k+'='+str(v) for k, v in dic_inf['tags'].iteritems() ] ),
    ','.join( [ k+'='+(str(v) if int==type(v) else '"'+str(v)+'"') for k, v in dic_inf['fields'].iteritems() ] ),
    str(dic_inf['time'])+'000', 
  )
  influx_gz.write(line);

  return dic_inf
# }}}

today = date.today()
startday = today - timedelta(days=1)
if 1 < len(sys.argv) :
  startday = datetime.strptime( sys.argv[1], '%y%m%d' ).date()
delta = today - startday

filename = os.path.basename(__file__)
log_file = BIN_PATH + "log/%s.%s"%(filename[:filename.rfind('.')],today.strftime('%y%m%d'))
setup_logger('log_batch', log_file )
log_batch = logging.getLogger('log_batch')

for d in [startday + timedelta(days=x) for x in range(0,delta.days)] :
  d_ymd = d.strftime('%y%m%d')
  log_daily_file = log_file+'.'+d_ymd
  setup_logger('log'+d_ymd, log_daily_file )
  log_daily = logging.getLogger('log'+d_ymd)

  influx_file = BIN_PATH + "log/influx_event.%s.gz" % d_ymd
  influx_gz = gzip.open(influx_file, 'wb')
  influx_gz.write("# DDL\nCREATE DATABASE gomlog\n# DML\n# CONTEXT-DATABASE: gomlog\n")
  print 'start: ' + d_ymd
  ts_start = datetime.now()

  file_pattern = '/data/log/log.gomlab.com/*' + d_ymd + '.access_log.log.gomlab.com.gz'
  for onefile in sorted(glob.glob(file_pattern)) :
    filename = os.path.basename(onefile)
    host = filename[: filename.find('.2017') ]
    with gzip.open(onefile, 'r+') as f:
      log_batch.info( '---=== '+filename+' ===---' )
      i = 0
      #json_array = []
      dt_last = None
      dt_seq = -1
      ts_tmp = datetime.now()

      for line in f:
        one = analyze(host,line)
        if not one :
          continue
        i+=1
        ## write to influxdb
        #json_array.append(one)
        #if 0 == (i % 1000) :
        #  if not influx.write_points(json_array,'u') :
        #    log_daily.warning( pformat(json_array, indent=4) )
        #  json_array = []

        # profile
        if 0 == (i % 100000) :
          print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
          ts_tmp = datetime.now()

      ## write remained json
      #if json_array :
      #  if not influx.write_points(json_array,'u') :
      #    log_daily.warning( pformat(json_array, indent=4) )
      #  print "{:,}".format(i), "\t", str(datetime.now()-ts_tmp)
      #  ts_tmp = datetime.now()

    # close 1 file
    log = "end: %s\t%d\t%s" % (filename, i, str(datetime.now()-ts_start) )
    #print log
    log_batch.info(log)

  influx_gz.close()
  if 62 == os.path.getsize(influx_file) :
    os.remove(influx_file)
  if 0 == os.path.getsize(log_daily_file) :
    os.remove(log_daily_file)

  #except influxdb.client.InfluxDBClientError as e:
  #  print "Error (InfluxDB): ", e.message
