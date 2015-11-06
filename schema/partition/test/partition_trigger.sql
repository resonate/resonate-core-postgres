--rollback; begin; set role rn_ad; update _scd.account set comment = false; savepoint unit_test;
rollback to unit_test;

/***********************************************************************************************************************************
* Tests for the trigger on _utility.partition table
***********************************************************************************************************************************/
do $$
declare
    lPartitionTableId bigint;
    lUnitId bigint;
    lSchemaName text;
    lTableName text;
    stryKey text[];
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
    
    
    perform _test.unit_begin('Partition Trigger');      


    
    perform _test.test_begin('Try creating a two partitions with overlapping keys, and it should fail'); 
    begin
        --this throws some other exception...
        --perform _utility.partition_create(lSchemaName, lTableName, null, 'unit_id', lUnitId::text, '1301');

        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (ARRAY['10', '20'], null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1302');
        
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (ARRAY['20', '30'], null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1303');
        
        perform _test.test_fail('Should have thrown an exception');
    exception
        when sqlstate 'UT001' then
            perform _test.test_pass();
    end;
    
    
   
    
    perform _test.test_begin('Try creating a two partitions with non-overlapping keys, and it should work'); 

    begin
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (ARRAY['50', '60'], null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1304');
        
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (ARRAY['70', '80'], null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1305');
        
        perform _test.test_pass();
    exception
        when sqlstate 'UT001' then
            perform _test.test_fail('Should not have thrown an exception');
    end;
    
   
    perform _test.test_begin('Update one of the above partitions to overlap with another, and it should fail'); 

    begin
        
        update _utility.partition
        set key = ARRAY['50', '70']
        where key = ARRAY['50', '60'];
        
        perform _test.test_fail('Should have thrown an exception');
    exception
        when sqlstate 'UT001' then
            perform _test.test_pass();
    end;


    perform _test.test_begin('Insert another partition with the same key, but with a different parent_id, and it should work'); 

    begin
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (ARRAY['70', '90'],
                (select id from _utility.partition where key = ARRAY['50', '60']),
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1305');
        
        perform _test.test_pass();
    exception
        when sqlstate 'UT001' then
            perform _test.test_fail('Should not have thrown an exception');
    end;


    perform _test.test_begin('Try creating a two partitions with non-overlapping huge keys, and it should work'); 

    begin

        stryKey := ARRAY[]::text[];

        for i in 1000..1999 loop
            stryKey = array_append(stryKey, i::text);
        end loop;

        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (stryKey, null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1304');

        stryKey := ARRAY[]::text[];

        for i in 2000..2999 loop
            stryKey = array_append(stryKey, i::text);
        end loop;
        
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (stryKey, null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1305');
        
        perform _test.test_pass();
    exception
        when sqlstate 'UT001' then
            perform _test.test_fail('Should not have thrown an exception');
    end;
    
    
    
    perform _test.test_begin('Try creating a two partitions with overlapping huge keys, and it should fail'); 

    begin

        stryKey := ARRAY[]::text[];

        for i in 1000..2000 loop  --the two arrays will both have the value "2000"
            stryKey = array_append(stryKey, i::text);
        end loop;

        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (stryKey, null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1304');

        stryKey := ARRAY[]::text[];

        for i in 2000..2999 loop
            stryKey = array_append(stryKey, i::text);
        end loop;
        
        insert into _utility.partition(key, parent_id, partition_table_id, partition_type_id, name) 
        values (stryKey, null, 
                (select id from _utility.partition_table where name = 'unit'), 
                (select id from _utility.partition_type where key = 'unit_id'),
                '1305');
        
        perform _test.test_fail('Should have thrown an exception');
    exception
        when sqlstate 'UT001' then
            perform _test.test_pass();
    end;
    
    perform _test.unit_end();
end $$;

