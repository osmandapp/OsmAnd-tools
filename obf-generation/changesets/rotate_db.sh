#!/bin/bash
set -e
cd /home/changesets/dumps
if [ -z "$PERIOD" ]; then 
  PERIOD=$(date -d "$(date +%Y-%m-01) -1 day" "+%Y-%m")
fi
DB_NAME=changeset_${PERIOD//-/_}
psql -d changeset -U $DB_USER -c "DROP DATABASE $DB_NAME" || true;
psql -d changeset -U $DB_USER -c "CREATE DATABASE $DB_NAME OWNER $DB_USER TABLESPACE changeset";
pg_dump -U $DB_USER -v -F t -f $DB_NAME.tar changeset
pg_restore -n public -U $DB_USER -v $DB_NAME.tar -d $DB_NAME
psql -d $DB_NAME -U $DB_USER -c "DELETE FROM changesets where substr(closed_at_day, 0, 8) <> '$PERIOD'";
psql -d $DB_NAME -U $DB_USER -c "DELETE FROM changeset_country c where not exists (select 1 from changesets d where d.id = c.changesetid)";
psql -d $DB_NAME -U $DB_USER -c "VACUUM FULL";
# set everything
pg_dump -U $DB_USER -v -F t -f $DB_NAME.tar $DB_NAME
gzip $DB_NAME.tar 