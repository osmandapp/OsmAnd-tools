#!/bin/bash
if [ -z "$PERIOD" ]; then 
  PERIOD=$(date -d "$(date +%Y-%m-01) -1 day" "+%Y-%m")
fi
cd /var/www-download/reports/
if [ -z "$EUR_VALUE" ]; then 
  # curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=${PERIOD}&dbmonth=${PERIOD}"
  php all_reports.php "${PERIOD}" 
else 
  echo "EUR $EUR_VALUE / BTC $BTC_VALUE"
  # curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=${PERIOD}&dbmonth=${PERIOD}&eurValue=${EUR_VALUE}&btcValue=${BTC_VALUE}"
  php all_reports.php "${PERIOD}" "${EUR_VALUE}" "${BTC_VALUE}"
  cd /home/changesets/dumps
  DB_NAME=changeset_${PERIOD//-/_}
  DUMPNAME=${DB_NAME}_$(date +%s)
  pg_dump -U $DB_USER -v -F t -f $DUMPNAME.tar $DB_NAME
  gzip $DUMPNAME.tar 
fi