var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var morgan = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var fs = require('fs');
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

//var auth = require('http-auth');
//var basic = auth.basic({
//  realm: "Simon Area.",
//  file: __dirname + "./config/users.htpasswd"
//});

var index = require('./routes/index');
var users = require('./routes/users');
var summary = require('./routes/summary');
var compare = require('./routes/compare');

var app = express();

// setup morgan logger, ensure log directory exists
var logDirectory = path.join(__dirname, 'log')
fs.existsSync(logDirectory) || fs.mkdirSync(logDirectory)
// create a rotating write stream
var accessLogStream = require('file-stream-rotator').getStream({
  date_format: 'YYYYMMDDHH',
  filename: path.join(logDirectory, 'access-%DATE%.log'),
  frequency: '1h',
  verbose: false
})
app.use(morgan('combined', {stream: accessLogStream}))

// view engine setup
app.locals.pretty = true;
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');
app.set('view cache', 'true');

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', index);
app.use('/users', users);
app.use('/summary', summary);
app.use('/compare', compare);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
