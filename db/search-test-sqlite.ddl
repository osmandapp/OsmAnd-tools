create table dataset
(
    id            INTEGER      not null primary key,
    all_cols      TEXT,
    created       timestamp    not null,
    error         TEXT,
    labels        varchar(255),
    name          varchar(255) not null unique,
    sel_cols      TEXT,
    size_limit    integer,
    source        TEXT         not null,
    source_status varchar(255) not null,
    test_row      TEXT,
    total         integer,
    type          varchar(255) not null,
    updated       timestamp    not null,
    check (source_status in ('UNKNOWN', 'OK', 'ERROR')),
    check (type in ('CSV', 'Overpass'))
);

create table domain
(
    id   INTEGER      not null primary key,
    data varchar(255),
    name varchar(255) not null unique
);

create table gen_result
(
    id         INTEGER   not null primary key,
    case_id    bigint    not null,
    count      integer   not null,
    dataset_id bigint    not null,
    duration   integer,
    error      TEXT,
    lat        float     not null,
    lon        float     not null,
    query      TEXT,
    row        clob,
    timestamp  timestamp not null
);

create table run
(
    id          INTEGER      not null primary key,
    base_search boolean,
    lat         float,
    locale      varchar(255),
    lon         float,
    north_west  varchar(255),
    south_east  varchar(255),
    case_id     bigint       not null,
    created     timestamp,
    dataset_id  bigint       not null,
    error       TEXT,
    status      varchar(255) not null,
    timestamp   timestamp,
    updated     timestamp,
    check (status in ('NEW', 'RUNNING', 'COMPLETED', 'CANCELED', 'FAILED'))
);

create table run_result
(
    id             INTEGER   not null primary key,
    case_id        bigint    not null,
    count          integer   not null,
    dataset_id     bigint    not null,
    duration       integer,
    error          TEXT,
    lat            float     not null,
    lon            float     not null,
    query          TEXT,
    row            clob,
    timestamp      timestamp not null,
    actual_place   integer,
    closest_result varchar(512),
    gen_id         bigint    not null,
    min_distance   integer,
    results_count  integer,
    run_id         bigint    not null
);

create table test_case
(
    id          INTEGER      not null primary key,
    base_search boolean,
    lat         float,
    locale      varchar(255),
    lon         float,
    north_west  varchar(255),
    south_east  varchar(255),
    all_cols    TEXT,
    created     timestamp,
    dataset_id  bigint       not null,
    error       TEXT,
    labels      varchar(255),
    last_run_id bigint,
    name        varchar(255),
    nocode_cfg  TEXT,
    prog_cfg    TEXT,
    sel_cols    TEXT,
    status      varchar(255) not null,
    test_row    TEXT,
    updated     timestamp,
    check (status in ('NEW', 'GENERATED', 'FAILED'))
);