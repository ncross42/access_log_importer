#!/usr/bin/env node
'usr strict';

var microtime = function () {
  return new Date().valueOf()/1000; //: new Date().toISOString().replace(/[-T:Z]/g,'');
}

var _proc_mem = process.memoryUsage();
var _start_ts = microtime();

var glob     = require("glob"),  // github.com/isaacs/node-glob.git
    readline = require('readline'),
    fs       = require('fs'), 
    util     = require('util'), 
    url      = require('url'), 
    uap      = require('node-uap'),
    mysql    = require('mysql');

var con = mysql.createConnection({
  socketPath : '/var/run/mysqld/mysqld.sock',
  host     : 'localhost',
  user     : '',
  password : '',
  database : ''
});

con.connect(function(err) {
  if (err) {
    console.error('error connecting: ' + err.stack);
    return;
  }
  console.log('connected as id ' + con.threadId);
  con.query('SET sql_log_bin = 0; /*LOCK TABLE access_logs WRITE;*/');
});

var sql = 'INSERT INTO access_logs_myisam ( \
	host, ip, dt, method, req, protocol, code, byte, ref, ua, req_dir, req_base, req_query, req_frag, ref_host, ref_path, ref_query, ua_fam_maj, ua_full, os_fam_maj, os_full, dev_full \
) VALUES ( \
		 ?,  ?,  ?,      ?,   ?,        ?,    ?,    ?,   ?,  ?,       ?,        ?,         ?,        ?,        ?,        ?,         ?,          ?,       ?,          ?,       ?,        ? \
)';

// analyze
var re = /([(\d\.)]+) - - \[(.*?)\] "([^\s]*?) ([^\s]*?) ([^\s]*?)" (\d+) (-|\d+) "(-|.*?)" "(.*?)"/i;
var analyze_line = function (host,line) {
  //console.log(line);
  var found = line.match(re);
  var param = found.slice(0);
  //console.log( found.length );
  if ( 10 <= found.length ) {
    param[0] = host;
    //param[1] = remote   = found[1];
    var d = found[2].substr(0, found[2].indexOf(':'));
    var t = found[2].substr(1+found[2].indexOf(':'));
    param[2] = dt       = new Date(Date.parse(d+' '+t)).toISOString();
    //param[3] = method   = found[3];
    //param[4] = req      = found[4];
    //param[5] = protocol = found[5];
    //param[6] = code     = found[6];
    //param[7] = byte     = found[7];
    //param[8] = ref      = found[8];
    //param[9] = ua       = found[9];

    req = url.parse(found[4]);
    param[10] = req.pathname.substr(0,req.pathname.lastIndexOf('/'));
    param[11] = req.pathname.substr(1+req.pathname.lastIndexOf('/')); //.split(/[\/]/).pop();
    param[12] = req.search; // default : null
    param[13] = req.hash;   // default : null

    ref = url.parse(found[4]);
    param[14] = ref.host;
    param[15] = ref.pathname; // default : null
    param[16] = ref.search;   // default : null

    // Parse everything
    var objUA = uap.parse(found[9]);
    param[17] = objUA.ua.family + (null==objUA.ua.major ? '' : ' '+objUA.ua.major);
    param[18] = param[17] + (null==objUA.ua.minor ? '' : '.'+objUA.ua.minor) +(null==objUA.ua.patch ? '' : '.'+objUA.ua.patch);
    param[19] = objUA.os.family + (null==objUA.os.major ? '' : ' '+objUA.os.major);
    param[20] = param[19] + (null==objUA.os.minor ? '' : '.'+objUA.os.minor) +(null==objUA.os.patch ? '' : '.'+objUA.os.patch);
    param[21] = objUA.device.family + (null==objUA.device.brand ? '' : ' '+objUA.device.brand) +(null==objUA.device.model ? '' : ' '+objUA.device.model);

    con.query(sql,param, function(err,results) {
      console.log( 'FAILED : '+util.inspect(err) );
    });
  }

  //console.log( util.inspect(found) );
  //console.log( util.inspect(param) );
}

// search and list files
glob("../../access/183.110.11.212*2017010400*-access_log", {}, function (er, files) {
  files.forEach( function (file) {
    var basename = file.substr( 1+file.lastIndexOf('/'));
    var host = basename.substr(0, basename.indexOf('-') );
    var rd = readline.createInterface({
      input: fs.createReadStream(file),
      output: process.stdout,
      terminal: false
    });

    var i = 0;
    rd.on('line', function(line) {
      analyze_line(host,line);
      if ( 0 == (++i%1000) ) {
        console.log(i);
      }
    });
    console.log( "process.memoryUsage().rss : " + new Intl.NumberFormat().format(process.memoryUsage().rss - _proc_mem.rss) );
    console.log( "process.memoryUsage().headUsed : " + new Intl.NumberFormat().format(process.memoryUsage().heapUsed - _proc_mem.heapUsed) );
  });
});

//con.end();
