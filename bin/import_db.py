#!/usr/bin/python
import glob, os, mysql.connector, re, datetime
from mysql.connector import errorcode
from urlparse import urlparse
from ua_parser import user_agent_parser

config = {
  'unix_socket' : '/var/run/mysqld/mysqld.sock',
  'host': '127.0.0.1',
  'user': 'root',
  'password': '',
  'database': '',
  'autocommit' : True,
  'get_warnings': True,
  'raise_on_warnings': True,
  #'use_pure': False,
}

try:
  cnx = mysql.connector.connect(**config)
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

cursor = cnx.cursor() #prepared=True
cursor.execute('SET sql_log_bin = 0; /*LOCK TABLE access_logs WRITE;*/', multi=True)

sql = ("INSERT INTO access_logs_myisam ("
    "host, ip, dt, method, req, protocol, code, byte, ref, ua, req_dir, req_base, req_query, req_frag, ref_host, ref_path, ref_query, ua_fam_maj, ua_full, os_fam_maj, os_full, dev_full"
    ") VALUES ("
    "  %s, %s, %s,     %s,  %s,       %s,   %s,   %s,  %s, %s,      %s,       %s,        %s,       %s,       %s,       %s,        %s,         %s,      %s,         %s,      %s,       %s"
    ")")

pat = re.compile( '([(\d\.)]+) - - \[(.*?)\] "([^\s]*?) ([^\s]*?) ([^\s]*?)" (\d+) (-|\d+) "(-|.*?)" "(.*?)' )

def analyze (line, host):
  global pat, sql, cursor
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
  dt = datetime.datetime.strptime(dt_old[:dt_old.find(' ')],'%d/%b/%Y:%H:%M:%S')
  dt_new = dt.__str__()
  if byte == '-' :
    byte = None
  #print (remote,dt_new,method,req,protocol,code,byte,ref,ua)

  oReq = urlparse(req)
  req_dir, req_base = os.path.split(oReq.path)
  req_query = oReq.query
  req_frag = oReq.fragment
  #print (req_dir, req_base,req_query,req_frag)

  oRef = urlparse(ref)
  ref_host = oRef.netloc
  ref_path = oRef.path
  ref_query = oRef.query # oRef.params
  #print (ref_host, ref_path,ref_query)

  if ''==ua.strip() :
    ua_fam_maj = ua_full = os_fam_maj = os_full = dev_full = None
  else :
    oUA = user_agent_parser.Parse(ua)
    ua_fam_maj = oUA['user_agent']['family']or'' + ' ' +oUA['user_agent']['major']or'' 
    ua_full    = ( oUA['user_agent']['family']or'' + ' ' +oUA['user_agent']['major']or'' +'.'+ oUA['user_agent']['minor']or'' +'.'+ oUA['user_agent']['patch']or'' ).strip('.')
    os_fam_maj = oUA['os']['family']or'' + ' ' +oUA['os.major']
    os_full    = ( oUA['os']['family']or'' + ' ' +oUA['os.major']or''  +'.'+ oUA['os.minor']or'' +'.'+ oUA['os.patch']or'' ).strip('.')
    dev_full   = ( oUA['device']['family']or'' + ' ' +oUA['device']['brand']or'' +' '+ oUA['device']['model']or'' ).strip()
  #print ua,':',ua_fam_maj,ua_full,os_fam_maj,os_full,dev_full

  param = ( host, remote, dt, method, req, protocol, code, byte, ref, ua, req_dir, req_base, req_query, req_frag, ref_host, ref_path, ref_query, ua_fam_maj, ua_full, os_fam_maj, os_full, dev_full )
  #print param

  cursor.execute(sql, param )

  return

file_pattern = '../../access/183.110.11.212*2017010500*-access_log'
for onefile in glob.glob(file_pattern) :
  basename = onefile[ 1+onefile.rfind('/'): ]
  host = basename[: basename.find('-') ]
  #print (onefile,basename,host)
  with open(onefile, 'r+') as f:
    i = 0
    for line in f:
      #print line
      analyze (line,host)
      i+=1
      if 0 == (i % 5000) :
        print i

cursor.close()
cnx.close()

