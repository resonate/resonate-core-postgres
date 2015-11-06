/***********************************************************************************************************************************
SCD Schema

The SCD schema tracks slowly changing dimensions.
***********************************************************************************************************************************/
create schema _scd;

create sequence _scd.object_id_seq;
do $$ begin execute 'grant select on _scd.object_id_seq to ' || _utility.role_get('reader'); end $$;

/***********************************************************************************************************************************
CONFIG Table

Stores configuration parameters for the _scd.
***********************************************************************************************************************************/
create table _scd.config
(
    object_id_min bigint
        constraint config_objectidmin_nn not null
        constraint config_objectidmin_ck check (object_id_min >= 100000000000000000),
    object_id_max bigint
        constraint config_objectidmax_nn not null
        constraint config_objectidmax_ck check (object_id_max <= 999999999999999999 and object_id_max >= object_id_min),
    comment_expression text not null default E'^ADHOC\:\ '
);

/***********************************************************************************************************************************
CONFIG_TABLE Table

Stores configuration parameters for a table participating in the _scd.
***********************************************************************************************************************************/
create table _scd.config_table
(
    id bigint not null default nextval('_scd.object_id_seq'),
    schema_name text not null
        constraint configtable_schemaname_ck check (schema_name = lower(schema_name)),
    table_name text not null
        constraint configtable_tablename_ck check (table_name = lower(table_name)),
    map boolean not null,
    journal boolean not null,
    constraint configtable_pk primary key (id),
    constraint configtable_schemaname_tablename_unq unique (schema_name, table_name)
);

/***********************************************************************************************************************************
CONFIG_TABLE_COLUMN Table
***********************************************************************************************************************************/
create table _scd.config_table_column
(
    id bigint not null default nextval('_scd.object_id_seq'),
    config_table_id bigint not null
        constraint configtablecolumn_configtableid_fk references _scd.config_table (id),
    name text not null
        constraint configtablecolumn_name_ck check (name = lower(name)),
    constraint configtablecolumn_pk primary key (id),
    constraint configtablecolumn_configtableid_name_unq unique (config_table_id, name)
);

create index configtablecolumn_configtableid_idx on _scd.config_table_column (config_table_id);

/***********************************************************************************************************************************
OBJECT Table

Stores a reference to every object that will participate in a slowly changing dimension, even if it does not do so in the current
database.
***********************************************************************************************************************************/
create table _scd.object
(
    id bigint
        constraint object_id_nn not null,
    key text not null default _utility.random_key_generate(8),
    config_table_id bigint
        constraint object_configtableid_nn not null
        constraint object_configtableid_fk references _scd.config_table (id),
    timestamp_insert timestamp with time zone not null default clock_timestamp(),
    timestamp_update timestamp with time zone,
    timestamp_delete timestamp with time zone,
    constraint object_pk primary key (id),
    constraint object_key_unq unique (key)
);

create index object_configtableid_idx on _scd.object (config_table_id);

/***********************************************************************************************************************************
ACCOUNT Table
***********************************************************************************************************************************/
create table _scd.account
(
    id bigint not null default nextval('_scd.object_id_seq'),
    key text not null,
    deny boolean not null default false,
    comment boolean not null default true,
    constraint account_pk primary key (id),
    constraint account_key_unq unique (key)
);

insert into _scd.account (key, deny) values ('postgres', true);
insert into _scd.account (key, deny) values ((select * from _utility.role_get()), true);
insert into _scd.account (key, deny) values ((select * from _utility.role_get('admin')), true);
insert into _scd.account (key, deny, comment) values ((select * from _utility.role_get('user')), true, false);
insert into _scd.account (key, deny) values ((select * from _utility.role_get('reader')), true);

/***********************************************************************************************************************************
APPLICATION Table
***********************************************************************************************************************************/
create table _scd.application
(
    id bigint not null default nextval('_scd.object_id_seq'),
    key text not null,
    deny boolean not null default false,
    comment boolean not null default true,
    constraint application_pk primary key (id),
    constraint application_key_unq unique (key)
);

/***********************************************************************************************************************************
TRANSACTION Table
***********************************************************************************************************************************/
create table _scd.transaction
(
    id bigint not null,
    build boolean not null,
    account_id bigint not null
        constraint transaction_accountid_fk references _scd.account (id),
    application_id bigint not null
        constraint transaction_applicationid_fk references _scd.application (id),
    comment text,
    constraint transaction_pk primary key (id)
);

create index transaction_accountid_idx on _scd.transaction (account_id);
create index transaction_applicationid_idx on _scd.transaction (application_id);

/***********************************************************************************************************************************
JOURNAL Table
***********************************************************************************************************************************/
create table _scd.journal
(
    id bigint not null default nextval('_scd.object_id_seq'),
    object_id bigint
        constraint journal_objectid_fk references _scd.object (id),
    transaction_id bigint
        constraint journal_transactionid_fk references _scd.transaction (id),
    timestamp timestamp with time zone default clock_timestamp(),
    type text not null
        constraint journal_type_ck check (type in ('i', 'u', 'd')),
    constraint journal_pk primary key (id)
);

create index journal_objectid_type_idx on _scd.journal (object_id, type);
create index journal_transactionid_idx on _scd.journal (transaction_id);

/***********************************************************************************************************************************
JOURNAL_DETAIL Table
***********************************************************************************************************************************/
create table _scd.journal_detail
(
    journal_id bigint
        constraint journaldetail_journalid_fk references _scd.journal (id),
    config_table_column_id bigint not null
        constraint journaldetail_configtablecolumnid_fk references _scd.config_table_column (id),
    value text,
    constraint journaldetail_pk primary key (journal_id, config_table_column_id)
);

create index journaldetail_configtablecolumnid_idx on _scd.journal_detail (config_table_column_id);
