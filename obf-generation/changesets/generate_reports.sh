#!/bin/bash
cd /home/changesets/dumps
if [ -z "$PERIOD" ]; then 
  PERIOD=$(date -d "$(date +%Y-%m-01) -1 day" "+%Y-%m")
fi
if [ -z "$EUR_VALUE" ]; then 
  curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=$PERIOD"
else 
  echo "EUR $EUR_VALUE / BTC $BTC_VALUE"
  curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=${PERIOD}&eurValue=${EUR_VALUE}&btcValue=${BTC_VALUE}"
  DB_NAME=changeset_${PERIOD//-/_}
  DUMPNAME=${DB_NAME}_$(date +%s)
  #-U $DB_USER
  pg_dump -v -F t -f $DUMPNAME.tar $DB_NAME
  gzip $DUMPNAME.tar 
fi