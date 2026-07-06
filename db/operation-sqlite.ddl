create table operation
(
    class_name   TEXT      not null primary key,
    name         TEXT      not null,
    title        TEXT,
    params_json  TEXT      not null,
    result_type  TEXT      not null,
    valid        INTEGER   not null default 1,
    updated_time TIMESTAMP not null default CURRENT_TIMESTAMP
);

create table job
(
    id           INTEGER   not null primary key autoincrement,
    class_name   TEXT      not null references operation (class_name),
    name         TEXT,
    description  TEXT,
    labels       TEXT,
    params_json  TEXT      not null,
    created_time TIMESTAMP not null default CURRENT_TIMESTAMP,
    updated_time TIMESTAMP not null default CURRENT_TIMESTAMP
);

create table run
(
    id            INTEGER   not null primary key autoincrement,
    job_id        INTEGER   not null references job (id) on delete cascade,
    status        TEXT      not null,
    params_json   TEXT      not null,
    result_json   TEXT,
    error_text    TEXT,
    elapsed_ms    INTEGER   not null default 0,
    started_time  TIMESTAMP,
    finished_time TIMESTAMP,
    created_time  TIMESTAMP not null default CURRENT_TIMESTAMP,
    updated_time  TIMESTAMP not null default CURRENT_TIMESTAMP
);

create table inactive_user_notice
(
    userid        INTEGER   not null primary key,
    email         TEXT,
    category      TEXT      not null,
    status        TEXT      not null,
    notified_time TIMESTAMP not null,
    deleted_time  TIMESTAMP,
    updated_time  TIMESTAMP not null default CURRENT_TIMESTAMP
);

create index inactive_user_notice_status_idx on inactive_user_notice (status);

create index job_class_name_idx on job (class_name);
create index job_updated_time_idx on job (updated_time);
create index run_job_id_idx on run (job_id);
create index run_status_idx on run (status);
create index run_created_time_idx on run (created_time);
