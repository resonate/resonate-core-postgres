/***********************************************************************************************************************************
UTILITY Partition Functions
***********************************************************************************************************************************/

/***********************************************************************************************************************************
PARTITION_TRIGGER_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_trigger_create(lPartitionTableId bigint) returns text as $$
declare
    rPartition record;
    rColumn record;
    strSchemaName text;
    strTableName text;
    strPartitionTrigger text;
    strBody text = '';
begin
    -- Get info about the table
    select schema_name,
           name,
           partition_trigger
      into strSchemaName,
           strTableName,
           strPartitionTrigger
      from _utility.partition_table
     where id = lPartitionTableId;
     
    -- If there are any unrepresented keys then this trigger cannot be written
     if
    (
         select count(*) > 0
           from _utility.partition_type
          where partition_table_id = lPartitionTableId
            and key_exists = false
    )
    then
        strBody = '    raise exception ''Partition keys in ' || strSchemaName || '.' || strTableName || 
                  ' do not all have fields.  You must insert into the partitions directly.'';';
    else
        if strPartitionTrigger is not null then
            strBody = strBody || strPartitionTrigger;
        end if;
    
        -- Find all the leaf level partitions
        for rPartition in 
            select rank() over (order by id desc) as rank,
                   partition.id
              from _utility.partition
             where partition.partition_table_id = lPartitionTableId
               and id not in
            (
                select partition_map.map_id
                  from _utility.partition
                       inner join _utility.partition_map
                            on partition_map.id = partition.id
                           and partition_map.level < 0
                 where partition.partition_table_id = lPartitionTableId
            )
        loop
            -- Add a space
            if rPartition.rank <> 1 or strPartitionTrigger is not null then
                strBody = strBody || E'\n\n';
            end if;

            strBody = strBody ||
                '    if ';

            -- Loop through all the partition key columns
            for rColumn in
                select rank() over (order by partition_type.id),
                       partition_type.key as partition_type_key,
                       partition_type.type as partition_type,
                       partition.key as partition_key
                  from _utility.partition_map
                       inner join _utility.partition
                            on partition.id = partition_map.map_id
                       inner join _utility.partition_type
                            on partition_type.id = partition.partition_type_id
                 where partition_map.id = rPartition.id
                   and partition_map.level <= 0
                 order by partition_map.depth
            loop
                -- Add a carriage return
                if rColumn.rank <> 1 then
                    strBody = strBody || E' and\n' ||
                        E'       ';
                end if;

                -- Add the partition key
                strBody = strBody ||
                    E'new.' || rColumn.partition_type_key || case when array_upper(rColumn.partition_key, 1) > 1 then ' in (' else ' = ' end || 
                        case rColumn.partition_type
                            when 'text' then (select array_to_string(array_agg(key), ', ') from (select quote_literal(unnest(rColumn.partition_key)) as key) partition_key)
                            when 'date' then (select array_to_string(array_agg(key), ', ') from (select quote_literal(to_char(unnest(rColumn.partition_key)::date, 'YYYY-MM-DD')) || '::date' as key) partition_key)
                            else (select array_to_string(array_agg(key), ', ') from (select unnest(rColumn.partition_key) as key) partition_key) end || 
                    case when array_upper(rColumn.partition_key, 1) > 1 then ')' else '' end;
            end loop;

            strBody = strBody || E'\n' ||
                E'    then\n' ||
                E'        insert into ' || strSchemaName || '_partition.' || strTableName || '_' || array_to_string(_utility.partition_tree_get(rPartition.id), '_') || E' values (new.*);\n' ||
                E'        return null;\n' ||
                E'    end if;';
        end loop;

        strBody = strBody || E'\n\n    raise exception ''Invalid partition key specified for ' || strSchemaName || '.' || strTableName || ''';';
    end if;
    
    return strBody;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_TABLE_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_table_create(strSchemaName text, strTableName text, strTableAbbreviation text default null, strPartitionTrigger text default null) returns bigint as $$
declare
    lPartitionTableId bigint = (select id from _utility.partition_table where schema_name = strSchemaName and name = strTableName);
begin
    -- Get a lock to serialize partition statements
    lock table _utility.partition_table in exclusive mode;

    -- Return an error if this table is already partitioned
    if lPartitionTableId is not null then
        raise exception '%.% is already partitioned', strSchemaName, strTableName;
    end if;
    
    -- Insert the partition table record
    insert into _utility.partition_table (schema_name, name, abbreviation, partition_trigger)
                                  values (strSchemaName, strTableName, strTableAbbreviation, strPartitionTrigger);

    lPartitionTableId = (select currval('_scd.object_id_seq'));

    -- Create the trigger
    execute _utility.trigger_create('partition', strSchemaName, strTableName, 'insert', 'before', 'definer', null,
                                    _utility.partition_trigger_create(lPartitionTableId));
                                    
    perform _utility.tablespace_table_move(strSchemaName, strTableName);

    return lPartitionTableId;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_TABLE_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_type_create(strSchemaName text, strTableName text, strParentKey text, strKey text, strType text, strName text, bKeyExists boolean default true) returns bigint as $$
declare
    lPartitionTableId bigint = (select id from _utility.partition_table where schema_name = strSchemaName and name = strTableName);
    lPartitionTypeParentId bigint = (select id from _utility.partition_type where partition_table_id = lPartitionTableId and key = strParentKey);
begin
    -- Get a lock to serialize partition statements
    lock table _utility.partition_table in exclusive mode;

    -- Return an error if this table is not partitioned
    if lPartitionTableId is null then
        raise exception '%.% is not partitioned', strSchemaName, strTableName;
    end if;

    -- Return an error if the parent key does not exist
    if strParentKey is not null and lPartitionTypeParentId is null then
        raise exception 'Partition type parent key % does not exist on %.%', strParentKey, strSchemaName, strTableName;
    end if;

    -- Insert the partition table record
    insert into _utility.partition_type (key, key_exists, parent_id, partition_table_id, type, name)
                                 values (strKey, bKeyExists, lPartitionTypeParentId, lPartitionTableId, strType, strName);

    return (select currval('_scd.object_id_seq'));
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_TREE_GET Function
***********************************************************************************************************************************/
create or replace function _utility.partition_tree_get(lPartitionId bigint) returns text[] as $$
begin
    return
    (
        select array_agg(name) as partition_tree
          from
        (  
            select coalesce(partition_type.name, '') || partition.name as name
              from _utility.partition_map
                   inner join _utility.partition
                        on partition.id = partition_map.map_id
                   inner join _utility.partition_type
                        on partition_type.id = partition.partition_type_id
             where partition_map.id = lPartitionId
               and partition_map.level <= 0
             order by partition_map.depth
        ) partition_tree
    );
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _utility.partition_tree_get(bigint) to ' || _utility.role_get('admin'); end $$;

/***********************************************************************************************************************************
PARTITION_CHECK_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_check_create(lPartitionId bigint, bReplace boolean) returns void as $$
declare
    strSchemaName text;
    strTableName text;
    strTableColumn text;
    strPartitionType text;
    strTableAbbreviation text;
    strCheckConstraintName text;
    strPrimaryKeyPrefix text;
    strPartitionTable text;
    stryKey text[];
begin
    select partition_table.schema_name,
           partition_table.name as table_name,
           partition_type.key as table_column_name,
           partition_type.type as partition_type,
           partition.key,
           partition_table.abbreviation
      into strSchemaName,
           strTableName,
           strTableColumn,
           strPartitionType,
           stryKey,
           strTableAbbreviation
      from _utility.partition
           inner join _utility.partition_type
                on partition_type.id = partition.partition_type_id
           inner join _utility.partition_table
                on partition_table.id = partition.partition_table_id
     where partition.id = lPartitionId;

    strPartitionTable = strSchemaName || '_partition.' || strTableName || '_' || array_to_string(_utility.partition_tree_get(lPartitionId), '_');

    -- Create the check constraint name and check the length
    select split_part(_utility.catalog_constraint_list_get(strSchemaName, strTableName, '{p}'), '_', 1) into strPrimaryKeyPrefix;

    strCheckConstraintName = _utility.string_table_shorten(coalesce(strTableAbbreviation, strPrimaryKeyPrefix, strTableName) || '_' || 
                             array_to_string(_utility.partition_tree_get(lPartitionId), '_')) || '_partition_' || 
                             _utility.string_table_shorten(strTableColumn) || '_ck';

    if length(strCheckConstraintName) > 63 then
        raise exception 'Constraint % on table % is greater than 63 characters', strCheckConstraintName, strPartitionTable;
    end if;

    if bReplace then
        -- Remove the old check constraint
        execute 'alter table ' || strPartitionTable || ' drop constraint ' || strCheckConstraintName;
    end if;

    -- Create the check constraint
    execute 'alter table ' || strPartitionTable || ' add constraint ' || 
            strCheckConstraintName || ' check (' || strTableColumn || case when array_upper(stryKey, 1) > 1 then ' in (' else ' = ' end || 
            case when strPartitionType in ('text', 'date') then (select array_to_string(array_agg(key), ', ') from (select quote_literal(unnest(stryKey)) as key) partition_key) else (select array_to_string(array_agg(key), ', ') from (select unnest(stryKey) as key) partition_key) end || 
            case when array_upper(stryKey, 1) > 1 then ')' else '' end || ')';
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_SCHEMA_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_schema_create
(
    strSchemaName text
)
    returns boolean as $$
begin
    if
    (
        select count(*) = 0
          from pg_namespace
         where nspname = strSchemaName || '_partition'
    ) then
        execute 'create schema ' || strSchemaName || '_partition';
        return true;
    end if;
    
    return false;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_CREATE Function
***********************************************************************************************************************************/
create or replace function _utility.partition_create
(
    strSchemaName text,
    strTableName text,
    lPartitionParentId bigint,
    strPartitionTypeKey text,
    strKey text,
    strName text default null,
    strTablespaceNameTable text default null,
    strTablespaceNameIndex text default null
)
    returns bigint as $$
declare 
    lPartitionTableId bigint = (select id from _utility.partition_table where schema_name = strSchemaName and name = strTableName);
    lPartitionTypeId bigint = (select id from _utility.partition_type where partition_table_id = lPartitionTableId and key = strPartitionTypeKey);
    lPartitionParentTypeId bigint = (select parent_id from _utility.partition_type where id = lPartitionTypeId);
    strPartitionType text = (select type from _utility.partition_type where partition_table_id = lPartitionTableId and key = strPartitionTypeKey);
    bKeyExists boolean = (select key_exists from _utility.partition_type where id = lPartitionTypeId);
    lPartitionId bigint;
    strTableColumn text = (select key from _utility.partition_type where id = lPartitionTypeId);
    strPartitionPath text;
    strPartitionParentTable text;
    strPartitionTable text;
    strPersistence text;
    bCreate boolean = false;
    bMigrate boolean = false;
    stryKey text[];
    iDmlTotal int;
begin
    -- Get a lock to serialize partition statements
    lock table _utility.partition_table in exclusive mode;
    
    -- Return an error if this table is not partitioned
    if lPartitionTableId is null then
        raise exception '%.% is not partitioned', strSchemaName, strTableName;
    end if;

    -- Return an error if the type key does not exist
    if lPartitionTypeId is null then
        raise exception 'Partition type key % does not exist on %.%', strPartitionTypeKey, strSchemaName, strTableName;
    end if;

    -- Create the partition table name
    if strName is null then
        strName = strKey;
    end if;

    -- Get the partition id
    select id
      from _utility.partition
      into lPartitionId
     where parent_id is not distinct from lPartitionParentId
       and partition_table_id = lPartitionTableId
       and name = strName;
    
    -- Create the partition if it does not exist
    if lPartitionId is null then
        -- Insert into the partition table
        insert into _utility.partition (key, parent_id, partition_table_id, partition_type_id, name,
                                        tablespace_name_table, tablespace_name_index)
                                values (('{' || strKey || '}')::text[], lPartitionParentId, lPartitionTableId, lPartitionTypeId,
                                        lower(strName), lower(strTablespaceNameTable), lower(strTablespaceNameIndex))
                             returning id into lPartitionId;

        -- Create the partition schema if it does not exist
        perform _utility.partition_schema_create(strSchemaName); 

        bCreate = true;
        
        -- If no table tablespace was specified, find the most recent parent with a tablespace
        if strTablespaceNameTable is null then
            select tablespace_name_table 
              into strTablespaceNameTable
              from _utility.partition_map
                   inner join _utility.partition
                        on partition.id = partition_map.map_id 
                       and partition.tablespace_name_table is not null
             where partition_map.id = lPartitionId
               and partition_map.level < 0
             order by depth desc
             limit 1;
        end if;
        
        -- If no index tablespace was specified, find the most recent parent with a tablespace
        if strTablespaceNameTable is null then
            select tablespace_name_index
              into strTablespaceNameIndex
              from _utility.partition_map
                   inner join _utility.partition
                        on partition.id = partition_map.map_id 
                       and partition.tablespace_name_index is not null
             where partition_map.id = lPartitionId
               and partition_map.level < 0
             order by depth desc
             limit 1;
        end if;

        -- Check parent persistence and copy it down to the child
        select case when relpersistence = 'u' then 'unlogged'
                    when relpersistence = 'p' then ''
                    when relpersistence = 't' then 'temporary'
                    else 'error'
               end as persistence
          into strPersistence
          from pg_namespace
               inner join pg_class
                    on pg_class.relnamespace = pg_namespace.oid
                   and pg_class.relname = strTableName
         where pg_namespace.nspname = strSchemaName;
    end if;

    -- Create the table names for the partition and partition parent
    if lPartitionParentId is null then
        strPartitionParentTable = strSchemaName || '.' || strTableName;
    else
        strPartitionParentTable = strSchemaName || '_partition.' || strTableName || '_' ||
                                  array_to_string(_utility.partition_tree_get(lPartitionParentId), '_');
    end if;

    strPartitionPath = array_to_string(_utility.partition_tree_get(lPartitionId), '_'); 
    strPartitionTable = strSchemaName || '_partition.' || strTableName || '_' || strPartitionPath;

    if bCreate then
        strTablespaceNameTable = coalesce(strTablespaceNameTable, _utility.tablespace_assign('table'));

        -- Create the partition table
        begin
            execute 'create ' || strPersistence || ' table ' || strPartitionTable ||
                    ' (like ' || strPartitionParentTable || ' including defaults including constraints)' ||
                    ' tablespace ' || strTablespaceNameTable;
        exception
            when duplicate_table then
                null;
        end;

        -- Create the default for the partition key (if it exists)
        if bKeyExists then
            execute 'alter table ' || strPartitionTable || ' alter ' || strTableColumn || ' set default ' ||
                    case when strPartitionType in ('text', 'date')
                        then quote_literal(strKey)
                        else strKey
                    end;
        end if;

        -- Create the key list
        stryKey = ('{' || strKey || '}')::text[];
    else
        -- Get the current key list
        select key
          into stryKey
          from _utility.partition
         where id = lPartitionId;

        -- Make sure the key exists
        if
        (
            select count(*) > 0
              from 
                (
                    select unnest(stryKey) as key
                ) partition_key
             where partition_key.key = strKey
        ) then
            raise exception 'Partition key % (%) already exists on %.% (%)', strKey, strName, strSchemaName, strTableName,
                            strPartitionTypeKey using errcode = 'PT001';
        end if;

        stryKey[array_upper(stryKey, 1) + 1] = strKey;

        update _utility.partition
           set key = stryKey
         where id = lPartitionId;

        -- Remove the default now that multiple keys exist
        execute 'alter table ' || strPartitionTable || ' alter ' || strTableColumn || ' drop default';
    end if;

    -- Create the partition insert trigger
    execute _utility.trigger_function_create('partition', strSchemaName, strTableName, 'insert', 'before', 'definer', null,
                                             _utility.partition_trigger_create(lPartitionTableId));

    -- Create/recreate the check constraint
    if bKeyExists then
        perform _utility.partition_check_create(lPartitionId, not bCreate);
                                             
        -- Insert partition data (if any) from the master table (if any)
        execute 'insert into ' || strPartitionTable || ' select * from only ' || strPartitionParentTable || ' where ' ||
                strTableColumn || ' = ' || 
                case when strPartitionType in ('text', 'date') then quote_literal(strKey) else strKey end;

        get diagnostics iDmlTotal = row_count;
                
        if iDmlTotal > 0 then
            -- Delete partition data (if any) from the master table
            execute 'delete from only ' || strPartitionParentTable || ' where ' || strTableColumn || ' = ' ||
                    case when strPartitionType in ('text', 'date') then quote_literal(strKey) else strKey end;

            -- If rows were moved from the master table set the migrate flag
            bMigrate = true;
        end if;
    end if;

    -- If this partition was just created then add it to the parent
    if bCreate then
        -- Make the partition table a child of the master table
        execute 'alter table ' || strPartitionTable || ' inherit ' || strPartitionParentTable;

        -- Perform an refresh to update indexes and constraints
        perform _utility.partition_table_refresh(strSchemaName, strTableName, lPartitionId);
    end if;

    -- If rows were migrated then analyze the parent
    if bMigrate then
        -- Analyze the parent partition
        perform _utility.analyze_table(strSchemaName, strTableName);
    end if;

    -- Analyze the new partition
    perform _utility.analyze_table(strSchemaName || '_partition',
                                   strTableName || '_' || strPartitionPath);

    return lPartitionId;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_DROP Function
***********************************************************************************************************************************/
create or replace function _utility.partition_drop(strSchemaName text, strTableName text, strPartitionTypeKey text, strName text, 
                                                   strKey text default null, bAnalyze boolean default true) returns void as $$
declare 
    lPartitionTableId bigint;
    lPartitionTypeId bigint;
    lPartitionId bigint;
    strPartitionTable text;
    bLastKey boolean = false;
begin
    -- Get a lock to serialize partition statements
    lock table _utility.partition_table in exclusive mode;

    -- Get table info
    select partition_table.id,
           partition_type.id,
           partition.id,
           strSchemaName || '_partition.' || strTableName || '_' || array_to_string(_utility.partition_tree_get(partition.id), '_')
      into lPartitionTableId,
           lPartitionTypeId,
           lPartitionId,
           strPartitionTable
      from _utility.partition_table
           left outer join _utility.partition_type
                on partition_type.partition_table_id = partition_table.id
               and partition_type.key = strPartitionTypeKey
           left outer join _utility.partition
                on partition.partition_type_id = partition_type.id
               and partition.name = strName
     where partition_table.schema_name = strSchemaName
       and partition_table.name = strTableName;

    -- Return an error if this table is not partitioned
    if lPartitionTableId is null then
        raise exception '%.% is not partitioned', strSchemaName, strTableName;
    end if;

    -- Return an error if the type does not exist
    if lPartitionTypeId is null then
        raise exception 'Partition type % does not exist on %.%', strPartitionTypeKey, strSchemaName, strTableName;
    end if;

    if strKey is not null then
        -- Make sure the specified key exists
        if
        (
            select count(*) = 0 from
            (
                select unnest(key) as key
                  from _utility.partition
                 where id = lPartitionId
            ) partition_key
             where key = strKey
        ) then
             raise exception '%.% partition % does not contain key %', strSchemaName, strTableName, strName, strKey;
        end if;

        -- If it is the last key then set the flag for partition deletion below
        if 
        (
            select count(*) = 1 from
            (
                select unnest(key) as key
                  from _utility.partition
                 where id = lPartitionId
            ) partition_key
        ) then
            bLastKey = true;

        -- If it is not the last key then remove the key and rewrite the constraint
        else
            -- Remove the key
            update _utility.partition
               set key = 
            (
                select array_agg(key)
                  from
                    (
                        select key 
                          from
                            (
                                select unnest(key) as key
                                  from _utility.partition
                                 where id = lPartitionId
                            ) partition_key
                         where key <> strKey
                         order by key
                    ) partition_key
            )
             where id = lPartitionId;

            -- Create/recreate the check constraint
            perform _utility.partition_check_create(lPartitionId, true);
        end if;
    end if;
    
    -- If no key was specified or the specified key is the last in the partition, then drop the entire partition
    if strKey is null or bLastKey is true then
        -- Remove the partition record
        delete from _utility.partition
         where id = lPartitionId;

        -- Error if the record was not found
        if not found then
             raise exception '%.% does not contain partition %', strSchemaName, strTableName, strName using errcode='UT002';
        end if;

        -- Drop the partition
        execute 'drop table ' || strPartitionTable || ' cascade';
    end if;
    
    -- Fix the trigger
    execute _utility.trigger_function_create('partition', strSchemaName, strTableName, 'insert', 'before', 'definer', null,
                                             _utility.partition_trigger_create(lPartitionTableId));
                                             
    -- Remove the schema if no more partitions exist
    if
    (
        select count(*) = 0
          from _utility.partition_table
               inner join _utility.partition
                    on partition.partition_table_id = partition_table.id
         where partition_table.schema_name = strSchemaName
    ) then
         execute 'drop schema ' || strSchemaName || '_partition cascade';
    end if;         

    -- Re-analyze the table
    if bAnalyze then
        perform _utility.analyze_table(strSchemaName, strTableName);
    end if;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_TABLE_EXISTS Function
***********************************************************************************************************************************/
create or replace function _utility.partition_table_exists(strSchemaName text, strTableName text) returns boolean as $$
begin
    return 
    (
        select count(*) = 1
          from _utility.partition_table 
         where schema_name = strSchemaName 
           and name = strTableName
    );
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _utility.partition_table_exists(text, text) to ' || _utility.role_get('admin'); end $$;

/***********************************************************************************************************************************
PARTITION_TABLE_DROP Function
***********************************************************************************************************************************/
create or replace function _utility.partition_table_drop(strSchemaName text, strTableName text, bAnalyze boolean default true) returns void as $$
declare 
    lPartitionTableId bigint = (select id from _utility.partition_table where schema_name = strSchemaName and name = strTableName);
    rPartition record;
begin
    -- Get a lock to serialize partition statements
    lock table _utility.partition_table in exclusive mode;

    -- Return an error if this table is not partitioned
    if lPartitionTableId is null then
        raise exception '%.% is not partitioned', strSchemaName, strTableName;
    end if;

    -- Drop all partitions
    for rPartition in
        select partition.name,
               partition_type.key as type_key
          from _utility.partition
               inner join _utility.partition_map
                    on partition_map.id = partition.id
                   and partition_map.level = 0
                   and partition_map.depth = 0
               inner join _utility.partition_type
                    on partition_type.id = partition.partition_type_id
         where partition.partition_table_id = lPartitionTableId
         order by partition.name
    loop
         perform _utility.partition_drop(strSchemaName, strTableName, rPartition.type_key, rPartition.name, null, false);
    end loop;

    -- Remove the partition table record
    delete from _utility.partition_table
     where id = lPartitionTableId;

    -- Remove the trigger
    perform _utility.trigger_drop('partition', strSchemaName, strTableName, 'insert', 'before');
    perform _utility.trigger_function_drop('partition', strSchemaName, strTableName, 'insert', 'before');
                                             
    -- Re-analyze the table
    if bAnalyze then
        perform _utility.analyze_table(strSchemaName, strTableName);
    end if;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_TABLE_REFRESH Function
***********************************************************************************************************************************/
create or replace function _utility.partition_table_refresh
(
    strSchemaName text,
    strName text,
    lPartitionId bigint default null,
    bDebug boolean default false
)
    returns void as $$
declare
    rObject record;
    rACL record;
    rTableACL _utility.catalog_table_acl;
    rFieldACL _utility.catalog_field_acl;
    rIndexData _utility.catalog_index;
    rFKPartitionTable record;
    rFKPartition record;
    strIndexTablespace text;
    rConstraintData _utility.catalog_constraint;
begin
    -- Get a lock to serialize refreshes
    lock table _utility.partition_table in exclusive mode;
    
    -- Make sure the schema is valid
    if strSchemaName is not null and
    (
        select count(*) = 0
          from _utility.partition_table
         where schema_name = strSchemaName
         limit 1
    ) then
        raise exception 'No tables in % schema are partitioned', strSchemaName;
    end if;

    -- Make sure the table is valid
    if strName is not null and strSchemaName is null then
        raise exception 'Schema name must be provided when table name is specified';
    end if;
        
    if strName is not null and
    (
        select count(*) = 0
          from _utility.partition_table
         where schema_name = strSchemaName
           and name = strName
    ) then
        raise exception '%.% is not partitioned', strSchemaName, strName;
    end if;
    
    -- Create a master list of all the objects that can be created or dropped
    create temp table temp_partitiontablerefresh_object_list as
    with partition_table as 
    (
    select id,
           schema_name,
           name
      from _utility.partition_table
     where (strSchemaName is null or schema_name = strSchemaName)
       and (strName is null or name = strName)
    ),
    pg_class_master as
    (
        select pg_class.oid
          from partition_table
               inner join pg_namespace
                    on pg_namespace.nspname = partition_table.schema_name
               inner join pg_class
                    on pg_class.relnamespace = pg_namespace.oid
                   and pg_class.relname = partition_table.name
    ),
    pg_class_partition_list as
    (
        with recursive pg_class_partition_list(parent) as
        (
            select pg_inherits.inhrelid as parent
              from pg_class_master
                   inner join pg_inherits
                        on pg_inherits.inhparent = pg_class_master.oid
                union
            select pg_inherits.inhrelid as parent
              from pg_class_partition_list
                   inner join pg_inherits
                        on pg_inherits.inhparent = pg_class_partition_list.parent
        )
        select *
          from pg_class_partition_list
    ),
    pg_class_list as
    (
        select 'master' as level,
               pg_class_master.oid as class_oid
          from pg_class_master
            union
        select 'partition' as level,
               pg_class_partition_list.parent as class_oid
          from pg_class_partition_list
    ),
    object_list as
    (
        select pg_class_list.level,
               pg_class_list.class_oid,
               'table persistence' as type,
               null::text as column_name,
               null::text as name,
               case when pg_class.relpersistence = 'p' then 'persisted'::text
                    when pg_class.relpersistence = 'u' then 'unlogged'::text
                    when pg_class.relpersistence = 't' then 'temporary'::text
                    else 'error'
               end as meta
          from pg_class_list
               inner join pg_class
                   on pg_class_list.class_oid = pg_class.oid
         union
        select pg_class_list.level,
               pg_class_list.class_oid,
               'index' as type,
               null::text as column_name, 
               pg_index_class.relname::text as name,
               ''::text as meta
          from pg_class_list
               inner join pg_index
                    on pg_index.indrelid = pg_class_list.class_oid
               inner join pg_class pg_index_class
                    on pg_index_class.oid = pg_index.indexrelid
                   and pg_index_class.relkind = 'i'
               inner join pg_class
                    on pg_class.oid = pg_class_list.class_oid
               left outer join pg_constraint
                    on pg_constraint.conrelid = pg_class.oid
                   and pg_constraint.conname = pg_index_class.relname
         where pg_constraint.conrelid is null
            union
        select pg_class_list.level,
               pg_class_list.class_oid,
               case pg_constraint.contype 
                   when 'p' then 'primary_key' 
                   when 'u' then 'unique' 
                   when 'f' then 'foreign_key' 
                   else 'error'
               end as type,
               null::text as column_name,
               pg_constraint.conname::text as name,
               case when pg_constraint.contype  = 'f' then
                   case when confupdtype <> 'a' then
                       ' on update ' ||
                       case confupdtype
                           when 'r' then 'restrict'
                           when 'c' then 'cascade'
                           when 'n' then 'set null'
                           when 'd' then 'set default' 
                           else 'error'
                       end
                   else
                       ''
                   end ||
                   case when confdeltype <> 'a' then
                       ' on delete ' ||
                       case confdeltype
                           when 'r' then 'restrict'
                           when 'c' then 'cascade'
                           when 'n' then 'set null'
                           when 'd' then 'set default' 
                           else 'error'
                       end
                   else
                       ''
                   end
               else
                   ''::text 
               end as meta
          from pg_class_list
               inner join pg_class
                    on pg_class.oid = pg_class_list.class_oid
               inner join pg_constraint
                    on pg_constraint.conrelid = pg_class.oid
                   and pg_constraint.contype in ('p', 'u', 'f')
            union
        select pg_class_list.level,
               pg_class_list.class_oid,
               'table_acl' as type,
               null::text as column_name,
               'acl' as name,
               pg_class.relacl::text as meta
          from pg_class_list
               inner join pg_class
                    on pg_class.oid = pg_class_list.class_oid
            union
        select pg_class_list.level,
               pg_class_list.class_oid,
               'table_column_acl' as type,
               pg_attribute.attname as column_name, 
               'acl' as name,
               pg_attribute.attacl::text as meta
          from pg_class_list
               inner join pg_attribute
                    on pg_attribute.attrelid = pg_class_list.class_oid
                   and pg_attribute.attnum >= 1
                   and attisdropped = false
    )
    select object_list.level,
           pg_namespace.nspname as schema_name,
           pg_class.relname as table_name,
           object_list.column_name,
           object_list.type,
           object_list.name,
           trim(both from object_list.meta) as meta
      from object_list
           inner join pg_class
                on pg_class.oid = object_list.class_oid
           inner join pg_namespace
                on pg_namespace.oid = pg_class.relnamespace;

    -- Expand the master list of objects against the partition table
    create temp table temp_partitiontablerefresh_master_object_list as
    with partition_path as
    (
        select id, array_agg(name) as path
          from
        (  
            select partition_map.id,
                   coalesce(partition_type.name, '') || partition.name as name
              from _utility.partition_map
                   inner join _utility.partition
                        on partition.id = partition_map.map_id
                   inner join _utility.partition_type
                        on partition_type.id = partition.partition_type_id
             where partition_map.level <= 0
               and (lPartitionId is null or partition_map.id = lPartitionId)
             order by partition_map.id, partition_map.depth
        ) partition_tree
         group by id
    )
    select partition.id as partition_id,
           partition_table.schema_name as schema_name_master,
           partition_table.schema_name || '_partition' as schema_name,
           partition_table.name as table_name_master,
           partition_table.name || '_' || array_to_string(partition_path.path, '_') as table_name,
           object_list.column_name,
           object_list.type,
           object_list.name as name_master,
           case when type in ('table_acl', 'table_column_acl') then
               object_list.name
           else
               substr(object_list.name, 1, strpos(object_list.name, '_') - 1) ||
               array_to_string(partition_path.path, '') || '_' ||
               substr(object_list.name, strpos(object_list.name, '_') + 1)
           end as name,
           object_list.meta as meta
      from temp_partitiontablerefresh_object_list as object_list
           inner join _utility.partition_table
                on partition_table.schema_name = object_list.schema_name
               and partition_table.name = object_list.table_name
           inner join _utility.partition
                on partition.partition_table_id = partition_table.id
               and (lPartitionId is null or partition.id = lPartitionId)
           inner join partition_path
                on partition_path.id = partition.id
     where object_list.level = 'master';

    -- Filter the partition list of objects
    create temp table temp_partitiontablerefresh_partition_object_list as
    select schema_name,
           table_name,
           column_name,
           type,
           name,
           meta
      from temp_partitiontablerefresh_object_list
     where level = 'partition'
       and table_name in 
    (
        select table_name
          from temp_partitiontablerefresh_master_object_list
    );

    -- Remove the temp table containing unfiltered objects.
    drop table temp_partitiontablerefresh_object_list;

    -- Find any differences in persistence
    for rObject in
        select schema_name,
               table_name,
               type,
               name,
               meta
          from temp_partitiontablerefresh_partition_object_list 
         where type in ('table persistence')
           except
        select schema_name,
               table_name,
               type,
               name,
               meta
          from temp_partitiontablerefresh_master_object_list
         where type in ('table persistence')
    loop
        raise exception 'Partition (%) persistence (%) does not match master!', rObject.schema_name || '.' || rObject.table_name,
                        rObject.meta;
    end loop;

    -- Remove any objects that are no longer on the master
    for rObject in
        select schema_name,
               table_name,
               type,
               name,
               meta
          from temp_partitiontablerefresh_partition_object_list
         where type in ('index', 'primary_key', 'unique', 'foreign_key')
            except
        select schema_name,
               table_name,
               type,
               name,
               meta
          from temp_partitiontablerefresh_master_object_list
         where type in ('index', 'primary_key', 'unique', 'foreign_key')
         order by schema_name,
                  table_name,
                  type,
                  name
    loop
        -- Drop indexes
        if rObject.type = 'index' then
            if bDebug then
                raise warning '%: Dropping index %', clock_timestamp(), rObject.schema_name || '.' ||
                                                     rObject.table_name || '.' || rObject.name;
            end if;
            
            execute _utility.catalog_index_drop_get(_utility.catalog_index_get(rObject.schema_name, rObject.name));

        -- Drop constraints
        else
            if bDebug then
                raise warning '%: Dropping constraint %', clock_timestamp(), rObject.schema_name || '.' || 
                                                          rObject.table_name || '.' || rObject.name;
            end if;
                
            execute _utility.catalog_constraint_drop_get(_utility.catalog_constraint_get(rObject.schema_name, rObject.table_name,
                                                                                         rObject.name));
        end if;
    end loop;

    -- Create any objects that are on the master but not the partitions
    for rObject in
        select master_object_list.partition_id,
               master_object_list.schema_name_master,
               master_object_list.schema_name,
               master_object_list.table_name,
               master_object_list.table_name_master,
               master_object_list.type,
               master_object_list.name,
               master_object_list.name_master
          from temp_partitiontablerefresh_master_object_list as master_object_list
               left outer join temp_partitiontablerefresh_partition_object_list as partition_object_list
                    on partition_object_list.schema_name = master_object_list.schema_name
                   and partition_object_list.table_name = master_object_list.table_name
                   and partition_object_list.type = master_object_list.type
                   and partition_object_list.name = master_object_list.name
                   and partition_object_list.meta is not distinct from master_object_list.meta
         where master_object_list.type in ('index', 'primary_key', 'unique', 'foreign_key')
           and partition_object_list.schema_name is null
         order by master_object_list.schema_name,
                  master_object_list.table_name,
                  master_object_list.type,
                  master_object_list.name
    loop
        if rObject.type <> 'foreign_key' then
            select tablespace_name_index
              into strIndexTablespace
              from _utility.partition_map
                   inner join _utility.partition
                        on partition.id = partition_map.map_id 
                       and partition.tablespace_name_index is not null
             where partition_map.id = rObject.partition_id
               and partition_map.level <= 0
             order by depth desc
             limit 1;
             
            if strIndexTablespace is null then
                strIndexTablespace = _utility.tablespace_assign('index', rObject.schema_name, rObject.table_name, rObject.name, true);

                if strIndexTablespace is null then
                    raise exception '%: Unable to get tablespace for index/constraint "%"', clock_timestamp(),
                                    rObject.schema_name || '.' || rObject.table_name || '.' || rObject.name;
                end if;
            end if;
        end if;

        -- Create indexes
        if rObject.type = 'index' then
            if bDebug then
                raise warning '%: Creating index %', clock_timestamp(), rObject.schema_name || '.' ||
                                                     rObject.table_name || '.' || rObject.name;
            end if;
            
            rIndexData = _utility.catalog_index_get(rObject.schema_name_master, rObject.name_master);

            rIndexData.schema_name = rObject.schema_name;
            rIndexData.table_name = rObject.table_name;
            rIndexData.index_name = rObject.name;
            
            rIndexData.tablespace_name = strIndexTablespace;
            execute _utility.catalog_index_create_get(rIndexData);

        -- Create constraints
        else
            if bDebug then
                raise warning '%: Creating constraint %', clock_timestamp(), rObject.schema_name || '.' ||
                                                          rObject.table_name || '.' || rObject.name;
            end if;
            
            rConstraintData = _utility.catalog_constraint_get(rObject.schema_name_master, 
                                                              rObject.table_name_master, rObject.name_master);

            rConstraintData.schema_name = rObject.schema_name;
            rConstraintData.table_name = rObject.table_name;
            rConstraintData.constraint_name = rObject.name;

            -- Check if the master table is partitioned, this is mainly to allow us to do
            -- error-checking and complain if things don't make sense.
            select
              partition_table.id,
              partition_table.schema_name,
              partition_table.name as table_name
              into rFKPartitionTable
              from _utility.partition_table
             where partition_table.schema_name = rConstraintData.schema_name_fk
               and partition_table.name = rConstraintData.table_name_fk;

            if FOUND then
                -- Referred-to table is partitioned
                -- See if we can create a FK to it from our current table.
                -- We have to refer to the leaf-level of the master and we must have some
                -- level which matches the master's leaf-level above or equal to us.

                -- Try to find a partition of the master which matches what we need.
                -- Pull all the types of partition from our level and above first
                with recursive partinfo as (
                     select partition.id,partition.partition_type_id,partition.parent_id,partition.name from _utility.partition where id = rObject.partition_id
                     union all
                     select partition.id,partition.partition_type_id,partition.parent_id,partition.name from _utility.partition join partinfo on (partition.id = partinfo.parent_id)
                )
                select
                  array_to_string(_utility.partition_tree_get(partition.id), '_') as name
                  into rFKPartition
                  from _utility.partition
                       inner join _utility.partition_table
                            on (partition.partition_table_id = partition_table.id)
                       inner join _utility.partition_type
                            on (partition.partition_type_id = partition_type.id)
                       inner join _utility.partition_type partition_typeRef
                            on (partition_type.key = partition_typeRef.key 
                            and partition_type.type = partition_typeRef.type 
                            and partition_type.name = partition_typeRef.name) 
                       inner join partinfo
                            on (partinfo.partition_type_id = partition_typeRef.id
                            and partinfo.name = partition.name)
                  where partition_table.schema_name = rConstraintData.schema_name_fk
                    and partition_table.name = rConstraintData.table_name_fk
                    and not exists (select * from _utility.partition partition_inner where partition_inner.parent_id = partition.id);

                -- If we found a match, use the partition instead of the top-level table
                if FOUND then
                    rConstraintData.schema_name_fk = rConstraintData.schema_name_fk || '_partition';
                    rConstraintData.table_name_fk = rConstraintData.table_name_fk || '_' || rFKPartition.name;
                else
                    -- If we can't find a match, complain.
                    raise exception 'Attempting to create foreign key (%.%) to a partitioned table (%.%) whose leaf level does not match any level of the referring table',
                                    rConstraintData.schema_name,
                                    rConstraintData.constraint_name, 
                                    rFKPartitionTable.schema_name,
                                    rFKPartitionTable.table_name;
                end if;
            end if;

            rConstraintData.tablespace_name = strIndexTablespace;
            execute _utility.catalog_constraint_create_get(rConstraintData);
            set constraints all immediate;
        end if;
    end loop;

    -- Make ACL changes at the table level
    for rACL in
        select master_object_list.schema_name_master,
               master_object_list.schema_name,
               master_object_list.table_name,
               master_object_list.table_name_master,
               master_object_list.meta as meta_master,
               partition_object_list.meta as meta_partition
          from temp_partitiontablerefresh_master_object_list as master_object_list
               inner join temp_partitiontablerefresh_partition_object_list as partition_object_list
                    on partition_object_list.schema_name = master_object_list.schema_name
                   and partition_object_list.table_name = master_object_list.table_name
                   and partition_object_list.type = master_object_list.type
                   and partition_object_list.name = master_object_list.name
                   and partition_object_list.meta is distinct from master_object_list.meta
         where master_object_list.type = 'table_acl'
         order by master_object_list.schema_name,
                  master_object_list.table_name,
                  master_object_list.type,
                  master_object_list.meta
    loop
        if bDebug then
            raise warning '%: ACL changes detected on % (% -> %)', clock_timestamp(), rACL.schema_name || '.' || rACL.table_name,
                          rACL.meta_partition, rACL.meta_master;
        end if; 
    
        -- Drop table-level permissions that do not exist in the master table
        for rTableACL in
            select part_table_acl.schema_name,
                   part_table_acl.table_name,
                   part_table_acl.grantor,
                   part_table_acl.grantee,
                   part_table_acl.privilege_type,
                   part_table_acl.is_grantable
              from _utility.catalog_table_acl_list_get(rACL.schema_name, rACL.table_name) as part_table_acl
                except
            select rACL.schema_name as schema_name,
                   rACL.table_name as table_name,
                   master_table_acl.grantor,
                   master_table_acl.grantee,
                   master_table_acl.privilege_type,
                   master_table_acl.is_grantable
              from _utility.catalog_table_acl_list_get(rACL.schema_name_master, rACL.table_name_master) as master_table_acl
        loop
            if bDebug then
                raise warning '%: Revoking ACL privilege % on % for %', clock_timestamp(), rTableACL.privilege_type,
                              rACL.schema_name || '.' || rACL.table_name, rTableACL.grantee;
            end if; 
            
            execute _utility.catalog_table_acl_get_revoke(rTableACL);
        end loop;
    
        -- Add table-level permissions that do not exist in the partitions
        for rTableACL in
            select rACL.schema_name as schema_name,
                   rACL.table_name as table_name,
                   master_table_acl.grantor,
                   master_table_acl.grantee,
                   master_table_acl.privilege_type,
                   master_table_acl.is_grantable
              from _utility.catalog_table_acl_list_get(rACL.schema_name_master, rACL.table_name_master) as master_table_acl
                except
            select part_table_acl.schema_name,
                   part_table_acl.table_name,
                   part_table_acl.grantor,
                   part_table_acl.grantee,
                   part_table_acl.privilege_type,
                   part_table_acl.is_grantable
              from _utility.catalog_table_acl_list_get(rACL.schema_name, rACL.table_name) as part_table_acl
        loop
            if bDebug then
                raise warning '%: Granting table ACL privilege % on % for %', clock_timestamp(), rTableACL.privilege_type,
                              rACL.schema_name || '.' || rACL.table_name, rTableACL.grantee;
            end if;
            
            execute _utility.catalog_table_acl_get_grant(rTableACL);
        end loop;
    end loop;

    -- Make ACL changes at the table level
    for rACL in
        select master_object_list.schema_name_master,
               master_object_list.schema_name,
               master_object_list.table_name,
               master_object_list.table_name_master
          from temp_partitiontablerefresh_master_object_list as master_object_list
               inner join temp_partitiontablerefresh_partition_object_list as partition_object_list
                    on partition_object_list.schema_name = master_object_list.schema_name
                   and partition_object_list.table_name = master_object_list.table_name
                   and partition_object_list.column_name = master_object_list.column_name
                   and partition_object_list.type = master_object_list.type
                   and partition_object_list.name = master_object_list.name
                   and partition_object_list.meta is distinct from master_object_list.meta
         where master_object_list.type = 'table_column_acl'
         group by master_object_list.schema_name_master,
                  master_object_list.schema_name,
                  master_object_list.table_name,
                  master_object_list.table_name_master
         order by master_object_list.schema_name,
                  master_object_list.table_name
    loop
        -- Drop column-level permissions that do not exist in the master table
        for rFieldACL in
            select part_field_acl.schema_name,
                   part_field_acl.table_name,
                   part_field_acl.column_name,
                   part_field_acl.grantor,
                   part_field_acl.grantee,
                   part_field_acl.privilege_type,
                   part_field_acl.is_grantable
              from _utility.catalog_all_field_acl_get(rACL.schema_name, rACL.table_name) as part_field_acl
                except
            select rACL.schema_name as schema_name,
                   rACL.table_name as table_name,
                   master_field_acl.column_name,
                   master_field_acl.grantor,
                   master_field_acl.grantee,
                   master_field_acl.privilege_type,
                   master_field_acl.is_grantable
              from _utility.catalog_all_field_acl_get(rACL.schema_name_master, rACL.table_name_master) as master_field_acl
        loop
            if bDebug then
                raise warning '%: Revoking ACL privilege % on % (%) for %', clock_timestamp(), rFieldACL.privilege_type,
                              rACL.schema_name || '.' || rACL.table_name, rFieldACL.column_name, rFieldACL.grantee;
            end if; 
            
            execute _utility.catalog_field_acl_get_revoke(rFieldACL);
        end loop;

        -- Add column-level permissions that do not exist in the partitions
        for rFieldACL in
            select rACL.schema_name as schema_name,
                   rACL.table_name as table_name,
                   master_field_acl.column_name,
                   master_field_acl.grantor,
                   master_field_acl.grantee,
                   master_field_acl.privilege_type,
                   master_field_acl.is_grantable
              from _utility.catalog_all_field_acl_get(rACL.schema_name_master, rACL.table_name_master) as master_field_acl
                except
            select part_field_acl.schema_name,
                   part_field_acl.table_name,
                   part_field_acl.column_name,
                   part_field_acl.grantor,
                   part_field_acl.grantee,
                   part_field_acl.privilege_type,
                   part_field_acl.is_grantable
              from _utility.catalog_all_field_acl_get(rACL.schema_name, rACL.table_name) as part_field_acl
        loop
            if bDebug then
                raise warning '%: Granting column ACL privilege % on % (%) for %', clock_timestamp(), rFieldACL.privilege_type,
                              rACL.schema_name || '.' || rACL.table_name, rFieldACL.column_name, rFieldACL.grantee;
            end if; 
            
            execute _utility.catalog_field_acl_get_grant(rFieldACL);
        end loop;
    end loop;

    -- Drop temp tables
    drop table temp_partitiontablerefresh_partition_object_list;
    drop table temp_partitiontablerefresh_master_object_list;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
PARTITION_GET Function
***********************************************************************************************************************************/
create or replace function _utility.partition_get(strSchemaName text, strTableName text, lPartitionParentId bigint, strPartitionTypeKey text, strPartitionKey text) returns bigint as $$
begin
    return
    (
        select partition.id
          from _utility.partition_table
               inner join _utility.partition_type
                    on partition_type.partition_table_id = partition_table.id
                   and partition_type.key = strPartitionTypeKey
                inner join _utility.partition
                    on partition.partition_type_id = partition_type.id
                   and strPartitionKey in (select * from unnest(partition.key))
                   and partition.parent_id is not distinct from lPartitionParentId
         where partition_table.schema_name = strSchemaName
           and partition_table.name = strTableName
    );
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _utility.partition_get(text, text, bigint, text, text) to ' || _utility.role_get('admin'); end $$;

/***********************************************************************************************************************************
PARTITION_ALL_REFRESH Function
***********************************************************************************************************************************/
create or replace function _utility.partition_all_refresh() returns void as $$
begin
    perform _utility.partition_table_refresh(null, null);
end;
$$ language plpgsql security definer;
