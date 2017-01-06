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

var connection = mysql.createConnection({
  socketPath : '/var/run/mysqld/mysqld.sock',
  host       : 'localhost',
  user       : 'root',
  password   : '',
  database   : ''
  //,debug    : true//;['ComQueryPacket', 'RowDataPacket']
});

connection.connect(function(err){
  if(err) {
    console.log("Error connecting database ...");    
    console.error(err);
  } else {
    console.log('mysql connected as id ' + connection.threadId);
    glob_file();
  }
});
connection.query('SET sql_log_bin = 0; /*LOCK TABLE access_logs WRITE;*/');

var i=0, rd_close=false;
var glob_file = function () { //{{{
  // search and list files
  glob("../../access/183.110.11.212*2017010500*-access_log", {}, function (err, files) {
    files.forEach( function (file) {
      console.log(file);
      var basename = file.substr( 1+file.lastIndexOf('/'));
      var host = basename.substr(0, basename.indexOf('-') );
      var rs = fs.createReadStream(file);
      rs.on('end', function () {
        rd_close = true;
        console.log('All the data in the file has been read');
        if ( global.gc ) {
          global.gc();
          console.log('garbage collected');
        }
      })
      var rd = readline.createInterface({
        input: rs/*,
        output: process.stdout,
        terminal: false,
        historySize: 0*/
      });

      rd.on('line', function(line) {
        ++i;
        //console.log('i:'+i);
        //if ( 0 == (i%5000) ) {
        //  console.log( "rd_line : "+i);
        //  console.log( "process.memoryUsage().rss : " + new Intl.NumberFormat().format(process.memoryUsage().rss - _proc_mem.rss) );
        //  console.log( "process.memoryUsage().headUsed : " + new Intl.NumberFormat().format(process.memoryUsage().heapUsed - _proc_mem.heapUsed) );
        //}
        //console.log(line);
        analyze_line(line,host);
      });
    });
  });
} //}}}

// analyze
var sql = 'INSERT INTO access_logs_myisam ( \
	host, ip, dt, method, req, protocol, code, byte, ref, ua, req_dir, req_base, req_query, req_frag, ref_host, ref_path, ref_query, ua_fam_maj, ua_full, os_fam_maj, os_full, dev_full \
) VALUES ( \
		 ?,  ?,  ?,      ?,   ?,        ?,    ?,    ?,   ?,  ?,       ?,        ?,         ?,        ?,        ?,        ?,         ?,          ?,       ?,          ?,       ?,        ? \
)';
var re = /([(\d\.)]+) - - \[(.*?)\] "([^\s]*?) ([^\s]*?) ([^\s]*?)" (\d+) (-|\d+) "(-|.*?)" "(.*?)"/;
var j=0, k=0;
var analyze_line = function (line,host) { //{{{
  var found = line.match(re);
  if ( found!==null && 10 <= found.length ) {
    var param = found.slice(0);
    param[0] = host;
    //param[1] = remote   = found[1];
    var d = found[2].substr(0, found[2].indexOf(':'));
    var t = found[2].substring( 1+found[2].indexOf(':'), found[2].indexOf('+') );
    param[2] = dt = new Date(Date.parse(d+' '+t+'UTC')).toISOString().substr(0,19).replace('T',' ');
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

    connection.query(sql,param, function(err,result) {
      ++j;
      //console.log('j:'+j);
      //if ( 0 == (j%5000) ) {
      //  console.log( "query: "+j+", skip: "+k );
      //  console.log( "process.memoryUsage().rss : " + new Intl.NumberFormat().format(process.memoryUsage().rss - _proc_mem.rss) );
      //  console.log( "process.memoryUsage().headUsed : " + new Intl.NumberFormat().format(process.memoryUsage().heapUsed - _proc_mem.heapUsed) );
      //}

      if ( err ) {
        console.log(line);
        console.log( 'FAILED : '+util.inspect(err) + "\n\tresults : " + util.inspect(result)+ "\n\tparam : " + util.inspect(param) );
        process.exit();
      } else if ( rd_close && i == (j+k) ) {
        console.log('connection.end');
        connection.end();
      }
    });
  } else {
    ++k;
    console.log('k:'+k);
    console.log(line);
    console.log('failed to parse req_url : '+line);
  }
} //}}}
