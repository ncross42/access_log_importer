/**
 * Gom Product Stat Summary
 */
var express = require('express');
var router = express.Router();
var moment = require('moment');

var mysql = require('mysql');
var config = require('config');
var dbConfig = config.get('mysql');

var mycon = mysql.createConnection(dbConfig);
mycon.connect(function(err) {
  if (err) {
    console.error('mysql connection error');
    console.error(err);
    throw err;
  }
});

function microtime () {
  return new Date().valueOf()/1000; //: new Date().toISOString().replace(/[-T:Z]/g,'');
}

// index
router.get(['/','/:basis/:start/:end'], function(req, res, next) {
	var basis = req.param.basis || 'daily';
	var start = req.param.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	var end   = req.param.end   || moment().subtract(1,'days').format('YYYY-MM-DD');
  var now = null;
  mycon.query('SELECT NOW()',function(err,rows){
    for (const[j,r] of enumerate(rows)) { 
      console.log(r);
      now = r['NOW()'];
      break;
    }
  });
  res.render('compare', { title:'Stat Compare : '+now, basis:basis, start:start, end:end });
});


function get_sql_yearly ( year=0 ) {//{{{
	return `SELECT 
      d, product, action, SUM(tot) pv, SUM(uni) uv
		FROM prd_daily_stat 
		WHERE d BETWEEN ?? - INTERVAL ${year} YEAR AND ?? - INTERVAL ${year} YEAR
      AND product IN ('player','audio')
		GROUP BY d, product, action`;
}//}}}

Date.prototype.addDays = function(days) {//{{{
  var dat = new Date(this.valueOf())
    dat.setDate(dat.getDate() + days);
  return dat;
}//}}}

function getDates(startDate, stopDate) {//{{{
  var dateArray = new Array();
  var currentDate = startDate;
  while (currentDate <= stopDate) {
    dateArray.push( new Date (currentDate).toISOString().substr(5,5) )
      currentDate = currentDate.addDays(1);
  }
  return dateArray;
}//}}}

// data-api
router.get(['/data','/data/:basis/:start/:end'], function(req, res, next) {

	const basis = req.param.basis || req.query.basis || 'daily';
	const start = req.param.start || req.query.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	const end   = req.param.end   || req.query.end   || moment().subtract(1,'days').format('YYYY-MM-DD');
  
  // xAxis.categories = [/*'12-30','12-31','01-01','01-02'*/];
  const date_keys = getDates( new Date(start), new Date(end) ); 
  console.log( date_keys );
  var series_keys = [/*'2017-player-download', ... , '2016-audio-install'*/];
  var series = [
   /*{ name: '2017-player-download', data: [7.0, 6.9, 9.5, 14.5] },
     { name: '2016-player-download', data: [-0.2, 0.8, 5.7, 11.3] },
     { name: '2017-audio-install', data: [-0.9, 0.6, 3.5, 8.4] }, 
     { name: '2016-audio-install', data: [3.9, 4.2, 5.7, 8.5] }*/
  ];

  var summary = {
    /*'2017' : {
      'player' : { 'download' : 0, 'uninstall' : 0 },
      'audio' : { 'download' : 0, 'uninstall' : 0 }
    }, '2016' : { ...  }*/
  };

  var tasksToGo = 3;
  for ( var i=0; i<3; i++ ) {
    var sql = mycon.format ( get_sql_yearly(i), [ start, end ] );
    var _ts_ = microtime();
    mycon.query( sql, function(err,rows){
      console.log(sql + "\n fetched count : " + rows.length + "\n exec-time(sec) : " + (microtim()-_ts_) );

      // rows.forEach( function(r,i) { console.log(i,r); });
      for (const[j,r] of enumerate(rows)) { 
        //console.log(j,r); 
        var md = r.d.substr(5,5);
        var d_key = date_keys.indexOf(md);
        if ( d_key == -1 ) {
          console.log( `cannot find md : ${md}` );
          console.log( r );
          continue;
        }

        const year = r.d.substr(0,4);
        const series_name = year+'-'+r.product+'-'+r.action;
        var s_key = series_keys.indexOf(series_name);
        if ( s_key == -1 ) {
          series.push({
            'name' : series_name,
            'data' : Array(date_keys.length).fill(0)
          });
          s_key = series.length - 1;
        }

        // for graph
        series[s_key].data[d_key] = r.pv;

        // for summary
        if ( ! summary[year] ) summary[year] = {};
        if ( ! summary[year][r.product] ) summary[year][r.product] = {};
        if ( ! summary[year][r.product][r.action] ) summary[year][r.product][r.action] = 0;
        summary[year][r.product][r.action] += r.pv;
      }

      tasksToGo--;
      if ( ! tasksToGo ) {
        res.json( {
          'x_categories' : date_keys,
          'data_series' : series,
          'summary' : summary
        });
      }
    });
  }
});

module.exports = router;
