/**
 * Gom Product Stat Summary
 */
var express = require('express');
var router = express.Router();
var moment = require('moment');

// index
router.get(['/','/:basis/:start/:end'], function(req, res, next) {
	var basis = req.param.basis || 'daily';
	var start = req.param.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	var end   = req.param.end   || moment().subtract(1,'days').format('YYYY-MM-DD');
  res.render('summary', { title:'Stat Summary', basis:basis, start:start, end:end });
});

// data-api
router.get(['/data','/data/:basis/:start/:end'], function(req, res, next) {
	var data = [
		[1,12], [2,5], [3,18], [4,13], [5,7], [6,4], [7,9], [8,10], [9,15], [10,22]
	];

	var basis = req.param.basis || 'daily';
	var start = req.param.start || moment().subtract(1,'weeks').format('YYYY-MM-DD');
	var end   = req.param.end   || moment().subtract(1,'days').format('YYYY-MM-DD');

	var sql = `SELECT * 
		FROM prd_daily_stat 
		WHERE d BETWEEN ? AND ?
		GROUP BY d, product, action
	`;
	mycon.query( sql, [start,end], function(err,rows){
		console.log(rows);

		res.json(data);
	});
});

module.exports = router;
