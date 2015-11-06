/***********************************************************************************************************************
* UTILITY Metric Tables
***********************************************************************************************************************/

/***********************************************************************************************************************************
METRIC_RAW_PROCESS and METRIC_PROCESS Tables
***********************************************************************************************************************************/
create table _utility.metric_raw_process
(
    id bigint not null,
    user_name text,
    application_name text,
    client_address text,
    client_hostname text,
    timestamp_begin timestamp with time zone
);

comment on table _utility.metric_raw_process is
'Contains raw process metrics.  See {{_utility.metric_process}} for final results.';

create table _utility.metric_process
(
    id bigint not null,
    user_name text,
    application_name text,
    client_address text,
    client_hostname text,
    timestamp_begin timestamp with time zone,
    constraint metricprocess_pk primary key (id)
);

/***********************************************************************************************************************************
METRIC_RAW_TRANSACTION and METRIC_TRANSACTION Tables
***********************************************************************************************************************************/
create table _utility.metric_raw_transaction
(
    id bigint not null,
    metric_process_id bigint not null,
    timestamp_begin timestamp with time zone
);

comment on table _utility.metric_raw_transaction is
'Contains raw transaction metrics.  See {{_utility.metric_transaction}} for final results.';

create table _utility.metric_transaction
(
    id bigint not null,
    metric_process_id bigint not null,
    journal_transaction_id bigint, 
    timestamp_begin timestamp with time zone,
    constraint metrictransaction_pk primary key (id),
    constraint metrictransaction_metricprocessid_id_unq unique (metric_process_id, id)
);

/***********************************************************************************************************************************
METRIC_RAW_TRANSACTION_JOURNAL_MAP Table
***********************************************************************************************************************************/
create table _utility.metric_raw_transaction_journal_map
(
    metric_transaction_id bigint not null,
    journal_transaction_id bigint not null
);

comment on table _utility.metric_raw_transaction_journal_map is
'Contains raw maps to journal transactions.  See {{_utility.metric_transaction}} for final results.';

/***********************************************************************************************************************************
METRIC_RAW_QUERY and METRIC_QUERY Tables
***********************************************************************************************************************************/
create table _utility.metric_raw_query
(
    id bigint not null,
    metric_transaction_id bigint not null,
    timestamp_begin timestamp with time zone,
    sql text
);

comment on table _utility.metric_raw_query is
'Contains raw query function metrics.  See {{_utility.metric_query}} for final results.';

create table _utility.metric_query
(
    id bigint not null,
    metric_process_id bigint not null,
    metric_transaction_id bigint not null,
    timestamp_begin timestamp with time zone,
    sql text,
    constraint metricquery_pk primary key (id),
    constraint metricquery_metrictransactionid_metricprocessid_id_unq unique (metric_transaction_id, metric_process_id, id)
);

/***********************************************************************************************************************************
METRIC_RAW_BEGIN and METRIC_RAW_END Tables
***********************************************************************************************************************************/
create table _utility.metric_raw_begin
(
    id bigint not null,
    metric_query_id bigint not null,
    parent_id bigint,
    current_user_name text,
    schema_name text not null,
    function_name text not null,
    depth int not null,
    parameter text[][],
    timestamp timestamp with time zone not null
);

comment on table _utility.metric_raw_begin is
'Contains raw begin metrics.  See {{_utility.metric}} and {{_utility.vw_metric}} for final results.';

create table _utility.metric_raw_end
(
    id bigint not null,
    cached boolean,
    result text[][],
    timestamp timestamp with time zone not null
);

comment on table _utility.metric_raw_end is
'Contains raw end metrics.  See {{_utility.metric}} and {{_utility.vw_metric}} for final results.';

/***********************************************************************************************************************************
METRIC Table
***********************************************************************************************************************************/
create table _utility.metric
(
    id bigint not null,
    metric_process_id bigint,
    metric_transaction_id bigint,
    metric_query_id bigint,
    parent_id bigint,
    depth int not null
        constraint metric_depth_ck check (depth >= 0),
    current_user_name text,
    schema_name text not null,
    function_name text not null,
    cached boolean,
    parameter text[][],
    result text[][],
    timestamp_begin timestamp with time zone not null,
    timestamp_end timestamp with time zone not null,
    constraint metric_pk primary key (id),
    constraint metric_metricqueryid_id_unq unique (metric_query_id, id),
    constraint metric_parentid_metricqueryid_fk foreign key (parent_id, metric_query_id) references _utility.metric (id, metric_query_id),
    constraint metric_metricqueryid_metrictransactionid_metricprocessid_fk foreign key (metric_query_id, metric_transaction_id, metric_process_id) references _utility.metric_query (id, metric_transaction_id, metric_process_id)
);

create index metric_parentid_metricqueryid_idx on _utility.metric (parent_id, metric_query_id);
create index metric_metricqueryid_metrictransactionid_metricprocessid_idx on _utility.metric (metric_query_id, metric_transaction_id, metric_process_id);

comment on table _utility.metric is
'Keeps metrics on important/expensive function calls.  Should not be used on every function.';

comment on column _utility.metric.parent_id is
'References {{_utility.metric.id}}.  If not null references the parent function that called this function.';
