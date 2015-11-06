--rollback; begin; set role rn_ad; update _scd.account set comment = false; savepoint unit_test;
rollback to unit_test;

/***********************************************************************************************************************************
* Create partition schema and table
***********************************************************************************************************************************/
do $$
declare
    lPartitionTableId bigint;
    lUnitId bigint;
    lSchemaName text;
    lTableName text;
begin
    --create a temp schema
    create schema temp_common;
    
    lSchemaName = 'temp_common';
    lTableName = 'unit';
    
    --create a test table to partition
    create table temp_common.unit
    (
    unit_id bigint,
    key text,
    name text,
    constraint unit_pk primary key (unit_id)
    );

    create index unit_userkey_key_idx on temp_common.unit (key);

    perform _scd.transaction_create('ADHOC: seed unit table with a dummy record');
    insert into temp_common.unit(unit_id, key, name)
    select _scd.nextval(), '1301', 'unit_1301' returning unit_id into lUnitId;

    -- Create partition definitions
    perform _utility.partition_table_create('temp_common', 'unit');
    perform _utility.partition_type_create('temp_common', 'unit', null, 'unit_id', 'number', null);
    
    -- call the partition function to create partition tables
    perform _utility.partition_create(lSchemaName, lTableName, null, 'unit_id', lUnitId::text, '1301');
      
        -- create temp table to store index and tablespace information
        drop table if exists index_tablespace_details;
        create temp table index_tablespace_details
        (
            index_name text,
            tablespace_name text
        );

        insert into index_tablespace_details
        select pg_index.relname, pg_tablespace.spcname
        from pg_class pg_table
        inner join pg_namespace
            on pg_namespace.oid = pg_table.relnamespace
            and pg_namespace.nspname in ('temp_common_partition')
        inner join pg_index pg_index_map
            on pg_index_map.indrelid = pg_table.oid
        inner join pg_class pg_index
            on pg_index.oid = pg_index_map.indexrelid
        inner join pg_tablespace 
            on pg_tablespace.oid = pg_index.reltablespace
        where pg_table.relname in ('unit_1301');
    
    perform _test.unit_begin('Index Tablespace');      
    
    perform _test.test_begin('Test if the partition schema got created'); 
    
    if
    (
        select count(*)
        from pg_namespace
        where nspname = lSchemaName || '_partition'
    ) = 0 then
        perform _test.test_fail('Partition temp_common_partition did not get created');
    else
        perform _test.test_pass();
    end if;

    perform _test.test_begin('Test if the partition table got created'); 
    
    if
    (
        select count(*)
        from _utility.partition_table
        inner join _utility.partition
            on partition.partition_table_id = partition_table.id
        where partition_table.schema_name = lSchemaName
            and partition_table.name = lTableName
    ) = 0 then
        perform _test.test_fail('Partition table unit_1301 did not get created');
    else
        perform _test.test_pass();
    end if;
    
    perform _test.test_begin('Test if the indexes moved to data tablespace'); 
    
    if 
    (
        select count(index_name)
        from index_tablespace_details
        where tablespace_name not like '%data%'
    ) = 0 then
         perform _test.test_pass();
      else
         perform _test.test_fail('The indexes are not getting moved to data tablespaces');
    end if; 


    perform _test.unit_end();


    perform _test.unit_begin('Table persistence testing');      

    perform _test.test_begin('Test that persistence type is pushed down to partitions'); 


    --create a test table to partition
    create unlogged table temp_common.unlogged_unit_test
    (
    unit_id bigint,
    key text,
    name text,
    constraint unlogged_unit_test_pk primary key (unit_id)
    );

    -- call the partition function to create partition tables
    perform _utility.partition_table_create('temp_common', 'unlogged_unit_test');
    perform _utility.partition_type_create('temp_common', 'unlogged_unit_test', null, 'unit_id', 'number', null);

    perform _utility.partition_create('temp_common', 'unlogged_unit_test', null, 'unit_id', lUnitId::text, '1301');

    if
    (
        select relpersistence
	  from pg_namespace
	       inner join pg_class on (pg_namespace.oid = pg_class.relnamespace)
	 where nspname = 'temp_common_partition'
	   and relname = 'unlogged_unit_test_1301'
    ) = 'u' then
         perform _test.test_pass();
      else
         perform _test.test_fail('Unlogged persistence type was not pushed down to partitions');
    end if; 


    
    perform _test.unit_end();
end $$;
