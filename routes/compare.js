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
	var basis = req.params.basis || 'daily';
	var start = req.params.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	var end   = req.params.end   || moment().subtract(1,'days').format('YYYY-MM-DD');
  res.render('compare', { title:'곰제품 연간 비교', basis:basis, start:start, end:end });
});

function get_sql_yearly ( year=0 ) {//{{{
	return `SELECT 
      d, product, action, SUM(tot) pv, SUM(uni) uv
		FROM prd_daily_stat 
		WHERE d BETWEEN ? - INTERVAL ${year} YEAR AND ? - INTERVAL ${year} YEAR
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

	const basis = req.params.basis || 'daily';
	const start = req.params.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	const end   = req.params.end   || moment().subtract(1,'days').format('YYYY-MM-DD');

  // xAxis.categories = [/*'12-30','12-31','01-01','01-02'*/];
  const date_list = getDates( new Date(start), new Date(end) ); 
// console.log( date_list );
  var series_keys = []; /*'2017-player-download', ... , '2016-audio-install'*/
  var series = [];
   /*{ name: '2017-player-download', data: [7.0, 6.9, 9.5, 14.5] },
          { name: '2016-player-download', data: [-0.2, 0.8, 5.7, 11.3] },
     { name: '2017-audio-install', data: [-0.9, 0.6, 3.5, 8.4] }, 
     { name: '2016-audio-install', data: [3.9, 4.2, 5.7, 8.5] }*/

  var summary = {};
    /*'2017' : {
      'player' : { 'download' : 0, 'uninstall' : 0 },
      'audio' : { 'download' : 0, 'uninstall' : 0 }
    }, '2016' : { ...  }*/

  var tasksToGo = 3;
  for ( let i=0; i<3; i++ ) {
    let sql = mycon.format ( get_sql_yearly(i), [ start, end ] );
// console.log(sql);
    let _ts_ = microtime();
    mycon.query( sql, function(err,rows){
      if ( err ) return console.log(err);
      //else console.log(sql + "\n fetched count : " + rows.length + "\n exec-time(sec) : " + (microtime()-_ts_) );

      rows.forEach( function(r,j) { 
// console.log(j,r); 
        let md = r.d.substr(5,5);
        let d_key = date_list.indexOf(md);
        if ( d_key == -1 ) {
          console.log( `cannot find md : ${md}` );
          console.log( r );
          return;
        }

        const year = r.d.substr(0,4);
        const series_name = year+'-'+r.product+'-'+r.action;
        var s_key = series_keys.indexOf(series_name);
        if ( s_key == -1 ) {
          series.push({
            'name' : series_name,
            'data' : Array(date_list.length).fill(0)
          });
          s_key = series.length - 1;
          series_keys.push(series_name);
        }

        // for graph
        series[s_key].data[d_key] = r.pv;

        // for summary
        if ( ! summary[year] ) summary[year] = {};
        if ( ! summary[year][r.product] ) summary[year][r.product] = {};
        if ( ! summary[year][r.product][r.action] ) summary[year][r.product][r.action] = 0;
        summary[year][r.product][r.action] += r.pv;
      });

// console.log(summary);

      tasksToGo--;
      if ( ! tasksToGo ) {
// console.log( series_keys );
        res.json( {
          'x_categories' : date_list,
          'series' : series,
          'summary' : summary
        });
      }
    });
  }
});

module.exports = router;
