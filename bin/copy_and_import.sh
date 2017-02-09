#!/bin/bash

SRC_IP=''
if [ $1 ] ; then
  SRC_IP=$1
else
  echo 'NO SRC IP'
  exit;
fi

BIN_PATH='/home/folder/logalyzer/bin'
LOG_PATH='/data/prdlog'

TODAY=`date +%y%m%d`
YESTERDAY=`date -d "-1 days" +%y%m%d`
START=$YESTERDAY
if [ $2 ] ; then
  START=$2
fi

NSTART=$(date -d "$START" +'%s')
NEND=$(date -d "$TODAY" +'%s')
DIFF=$(( ( $NEND - $NSTART ) / (24*3600) ))

while [ $DIFF -gt 0 ] ;
do
  COPYDAY=`date -d "-$DIFF days" +%y%m%d`
  COPYDIR=$LOG_PATH/$COPYDAY

  if [ ! -d "$COPYDIR" ]; then
    mkdir $COPYDIR
  fi

  SRC="${SRC_IP}:/data/prdlog/access/*-20${COPYDAY}*-access_log.gz"
  scp $SRC $COPYDIR/

  DIFF=$(( $DIFF - 1 ))
done

$BIN_PATH/import_download.py > $BIN_PATH/log.import_download.$TODAY

$BIN_PATH/stat_download.py > $BIN_PATH/log.stat_download.$TODAY
