#!/bin/bash -xe
if [ -z "$PERIOD" ]; then 
  PERIOD=$(date -d "$(date +%Y-%m-01) -1 day" "+%Y-%m")
fi
DB_NAME=changeset_${PERIOD//-/_}
cd /var/www-download/reports/background
DUMPNAME=${DB_NAME}
if [ -z "$EUR_VALUE" ]; then 
  # curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=${PERIOD}&dbmonth=${PERIOD}"
  php all_reports.php "${PERIOD}" 
else 
  echo "EUR $EUR_VALUE / BTC $BTC_VALUE"
  # curl -X POST "http://builder.osmand.net/reports/all_reports.php?month=${PERIOD}&dbmonth=${PERIOD}&eurValue=${EUR_VALUE}&btcValue=${BTC_VALUE}"
#  php all_reports.php "${PERIOD}" "${EUR_VALUE}" "${BTC_VALUE}"
fi
cd /home/changesets/dumps
PSQL="/usr/lib/postgresql/10.4/bin/"
${PSQL}pg_dump -p 5433 -U $DB_USER -v -F t -f $DUMPNAME.tar $DB_NAME
rm $DUMPNAME.tar.gz || true
gzip $DUMPNAME.tar 
