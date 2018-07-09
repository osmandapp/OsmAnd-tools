#!/bin/bash
set -e
cd /home/changesets/dumps
if [ -z "$PERIOD" ]; then 
  PERIOD=$(date -d "$(date +%Y-%m-01) -1 day" "+%Y-%m")
fi
DB_NAME=changeset_${PERIOD//-/_}
PSQL="/usr/lib/postgresql/10.4/bin/"
# ${PSQL}psql -p 5433 -d changeset -U $DB_USER -c "DROP DATABASE $DB_NAME" || true;
${PSQL}psql -p 5433 -d changeset -U $DB_USER -c "CREATE DATABASE $DB_NAME OWNER $DB_USER TABLESPACE changeset";
${PSQL}pg_dump -p 5433 -U $DB_USER -v -F t -f $DB_NAME.tar changeset
${PSQL}pg_restore -p 5433 -n public -U $DB_USER -v $DB_NAME.tar -d $DB_NAME
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "DELETE FROM changesets where substr(closed_at_day, 0, 8) <> '$PERIOD'";
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "DELETE FROM changeset_country c where not exists (select 1 from changesets d where d.id = c.changesetid)";
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "TRUNCATE final_reports;";
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "REFRESH MATERIALIZED VIEW changesets_view;";
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "REFRESH MATERIALIZED VIEW changeset_country_view;";
${PSQL}psql -p 5433 -d $DB_NAME -U $DB_USER -c "VACUUM FULL";
# set everything
${PSQL}pg_dump -p 5433 -U $DB_USER -v -F t -f $DB_NAME.tar $DB_NAME
rm $DB_NAME.tar.gz || true
gzip $DB_NAME.tar 
