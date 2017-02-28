#!/bin/bash

BIN_PATH='/home/folder/logalyzer/bin'
TODAY=`date +%y%m%d`

$BIN_PATH/import_event_mongo.py > $BIN_PATH/log/import_event_mongo.$TODAY 2> $BIN_PATH/log/import_event_mongo.$TODAY.err
$BIN_PATH/stat_event_mongo.py > $BIN_PATH/log/stat_event_mongo.$TODAY 2> $BIN_PATH/log/stat_event_mongo.$TODAY.err
