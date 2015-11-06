/***********************************************************************************************************************************
UTILITY Partition Tables
***********************************************************************************************************************************/

/***********************************************************************************************************************************
PARTITION_TABLE Table
***********************************************************************************************************************************/
create table _utility.partition_table
(
    id bigint not null,
    schema_name text not null,
    name text not null,
    abbreviation text,
    partition_trigger text,
    constraint partitiontable_pk primary key (id),
    constraint partitiontable_schemaname_name_unq unique (schema_name, name)
);

do $$ begin perform _scd.add_table('_utility', 'partition_table', false, true); end $$;

/***********************************************************************************************************************************
PARTITION_TYPE Table
***********************************************************************************************************************************/
create table _utility.partition_type
(
    id bigint not null,
    key text not null,
    key_exists boolean not null,
    parent_id bigint
        constraint partitiontype_parentid_fk references _utility.partition_type (id) on delete cascade,
    partition_table_id bigint not null
        constraint partitiontype_partitiontableid_fk references _utility.partition_table (id) on delete cascade,
    type text not null
        constraint partitiontype_type_ck check (type in ('number', 'boolean', 'text', 'date')),
    name text,
    constraint partitiontype_pk primary key (id),
    constraint partitiontype_key_partitiontableid_unq unique (key, partition_table_id),
    constraint partitiontype_name_partitiontableid_unq unique (name, partition_table_id),
    constraint partitiontype_partitiontableid_id_unq unique (partition_table_id, id)
);

create index partitiontype_parentid_idx on _utility.partition_type (parent_id);

do $$ begin perform _scd.add_table('_utility', 'partition_type', true, true); end $$;

/***********************************************************************************************************************************
PARTITION Table
***********************************************************************************************************************************/
create table _utility.partition
(
    id bigint not null,
    key text[] not null,
    parent_id bigint
        constraint partition_parentid_fk references _utility.partition (id) on delete cascade,
    partition_table_id bigint not null,
    partition_type_id bigint not null,
    name text not null,
    tablespace_name_table text,
    tablespace_name_index text,
    constraint partition_pk primary key (id),
    constraint partition_name_parentid_unq unique (name, parent_id),
    constraint partition_partitiontableid_partitiontypeid_fk foreign key (partition_table_id, partition_type_id) references _utility.partition_type (partition_table_id, id) on delete cascade
);

create index partition_name_idx on _utility.partition (name);
create index partition_parentid_idx on _utility.partition (parent_id);
create index partition_partitiontableid_partitiontypeid_idx on _utility.partition (partition_table_id, partition_type_id);

do $$ begin perform _scd.add_table('_utility', 'partition', true, true); end $$;
