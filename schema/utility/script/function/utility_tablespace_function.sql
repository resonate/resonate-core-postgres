/***********************************************************************************************************************
* UTILITY Tablespace Functions
***********************************************************************************************************************/
create or replace function _utility.tablespace_table_move
(
    strSchemaName text,
    strTableName text,
    bForce boolean default false
)
    returns void as $$
declare
    strTablespaceName text;
    rIndex record;
    rPartition record;
    rPartitionIndex record;
begin
    -- Move the table if needed
    strTablespaceName = _utility.tablespace_assign('table', strSchemaName, strTableName, null, bForce);

    if strTablespaceName is not null then
        --raise notice 'Moving table %.% to %', strSchemaName, strTableName, strTablespaceName;
        execute 'alter table ' || strSchemaName || '.' || strTableName || ' set tablespace ' || strTablespaceName;
    end if;

    -- Find the table's indexes
    for rIndex in
        select pg_index.relname as name
          from pg_class pg_table
               inner join pg_namespace
                    on pg_namespace.oid = pg_table.relnamespace
                   and pg_namespace.nspname = strSchemaName
               inner join pg_index pg_index_map
                    on pg_index_map.indrelid = pg_table.oid
               inner join pg_class pg_index
                    on pg_index.oid = pg_index_map.indexrelid
         where pg_table.relname = strTableName
           and pg_table.relkind = 'r'
         order by pg_index.relname
    loop
        -- Move the index if needed
        strTablespaceName = _utility.tablespace_assign('index', strSchemaName, strTableName, rIndex.name, bForce);

        if strTablespaceName is not null then
            --raise notice '    Moving index %.%.% to %', strSchemaName, strTableName, rIndex.name, strTablespaceName;
            execute 'alter index ' || strSchemaName || '.' || rIndex.name || ' set tablespace ' || strTablespaceName;
        end if;
    end loop;

    -- Make sure the partition tables exist before querying them
    if 
    (
        select count(*) = 2
          from pg_namespace
               inner join pg_class pg_table
                    on pg_table.relname in ('partition', 'partition_table')
         where pg_namespace.nspname = '_utility'
    ) then
        -- Find the table's partitions
        for rPartition in
            select partition_table.schema_name || '_partition' as schema_name,
                   partition_table.name || '_' || array_to_string(_utility.partition_tree_get(partition.id), '_') as table_name
              from _utility.partition_table
                   inner join _utility.partition
                        on partition.partition_table_id = partition_table.id
             where partition_table.schema_name = strSchemaName
               and partition_table.name = strTableName
             order by partition.name
        loop
            -- Move the partition if needed
            strTablespaceName = _utility.tablespace_assign('table', rPartition.schema_name, rPartition.table_name, null, bForce);

            if strTablespaceName is not null then
                if not bForce then
                    raise warning '    Partition %.% should already be assigned to a data tablespace',
                                  rPartition.schema_name, rPartition.table_name;
                end if;
                
                execute 'alter table ' || rPartition.schema_name || '.' || rPartition.table_name ||
                        ' set tablespace ' || strTablespaceName;
            end if;

            -- Find the partition's indexes
            for rPartitionIndex in
                select pg_index.relname as name
                  from pg_class pg_table
                       inner join pg_namespace
                            on pg_namespace.oid = pg_table.relnamespace
                           and pg_namespace.nspname = rPartition.schema_name
                       inner join pg_index pg_index_map
                            on pg_index_map.indrelid = pg_table.oid
                       inner join pg_class pg_index
                            on pg_index.oid = pg_index_map.indexrelid
                 where pg_table.relname = rPartition.table_name
                   and pg_table.relkind = 'r'
                 order by pg_index.relname
            loop
                -- Move the partition index if needed
                strTablespaceName = _utility.tablespace_assign('index', rPartition.schema_name, rPartition.table_name,
                                                               rPartitionIndex.name, bForce);

                if strTablespaceName is not null then
                    if not bForce then
                        raise warning '        Partition index %.%.% should already be assigned to a data tablespace',
                                      rPartition.schema_name, rPartition.table_name, rPartitionIndex.name;
                    end if;
                    
                    execute 'alter index ' || rPartition.schema_name || '.' || rPartitionIndex.name ||
                            ' set tablespace ' || strTablespaceName;
                end if;
            end loop;
        end loop;
    end if;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.tablespace_table_move(text, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.tablespace_pattern(strType text) returns text as $$
begin
    -- Type must be data (more may be added later)
    if strType not in ('data') then
        raise exception 'Invalid tablespace type %', strType;
    end if;

    return '[a-z]{2}_' || strType || '_n[0-9]{2}';
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.tablespace_pattern(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.tablespace_get(strObjectType text, strSchemaName text, strObjectName text, bValidate boolean default false) returns text as $$
declare
    oidObject oid;
    strTablespace text;
begin
    -- Make sure schema and object are set 
    if strSchemaName is null or strObjectName is null then
        raise exception 'strSchemaName and strObjectName must be set';
    end if;

    -- Object type must be table or index
    if strObjectType not in ('index', 'table') then
        raise exception 'Invalid object type %: strSchemaName: %, strObjectName: %', strObjectType, strSchemaName, strObjectName;
    end if;

    -- Get the tablespace
    select pg_class.oid,
           pg_tablespace.spcname
      into oidObject,
           strTablespace
      from pg_class
           inner join pg_namespace
               on pg_namespace.oid = pg_class.relnamespace
              and pg_namespace.nspname = strSchemaName
           left outer join pg_tablespace
               on pg_tablespace.oid = pg_class.reltablespace
     where pg_class.relname = strObjectName
       and pg_class.relkind = case strObjectType when 'table' then 'r' when 'index' then 'i' else 'error' end;

    -- If the tablespace name is null get the default tablespace
    if strTablespace is null then
        select pg_tablespace.spcname
          into strTablespace
          from pg_database
               inner join pg_tablespace
                    on pg_tablespace.oid = pg_database.dattablespace
         where pg_database.datname = current_database();
    end if;

    -- If validation is on check the tablespace against the standard pattern
    if bValidate and strTablespace not similar to _utility.tablespace_pattern('data') then
        strTablespace = null;
    end if;
    
    return strTablespace;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.tablespace_get(text, text, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.tablespace_assign
(
    strObjectType text,
    strSchemaName text default null,
    strTableName text default null,
    strIndexName text default null,
    bForce boolean default false
)
    returns text as $$
declare
    strTablespace text;
    strTablespaceReturn text;
begin
    --raise warning 'type %, schema %, table %, index %', strObjectType, strSchemaName, strTableName, strIndexName;

    if not bForce and (strSchemaName is not null and strTableName is not null) then
        -- Get the tablespace for this object
        strTablespace = _utility.tablespace_get(strObjectType,
                                                strSchemaName,
                                                case when strObjectType = 'index' then strIndexName else strTableName end,
                                                true);

        -- If the object has a valid tablespace return null (no assignment needed)
        if strTablespace is not null then
            return null;
        end if;
    end if;

    -- If this is an index then get the tablespace for the table
    if strObjectType = 'index' then
        if strSchemaName is null or strTableName is null then
            raise exception 'schema and table must be provided when assigned index tablespaces';
        end if; 
    
        strTablespace = _utility.tablespace_get('table', strSchemaName, strTableName, true);

        -- If the table has no tablespace then exit with an error
        if strTablespace is null then
            raise exception 'Table %.% must be assigned to a tablespace before an index tablespace can be assigned',
                            strSchemaName, strTableName;
        end if;
    end if;

    -- Find a tablespace with the most empty space (always put indexes on a different tablespace than the table)
    select pg_tablespace.spcname as name
      into strTablespaceReturn
      from pg_tablespace 
           left outer join pg_class
                on pg_class.reltablespace = pg_tablespace.oid
     where spcname similar to _utility.tablespace_pattern('data')
       and spcname is distinct from strTablespace
     group by pg_tablespace.spcname
     order by sum(coalesce(pg_class.relpages, 0))
     limit 1;

    -- Return the tablespace if one was found
    if strTablespaceReturn is not null then
        return strTablespaceReturn;
    end if;

    -- If an index there might be only one tablespace so return the table's tablespace
    if strTablespace is not null then
        return strTablespace;
    end if;

    -- It appears there are no data tablespaces
    raise exception 'Unable to find a data tablespace';
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.tablespace_assign(text, text, text, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.tablespace_move(bForce boolean default false) returns void as $$
declare
    rTable record;
begin
    -- Iterate through all tables and run _utility.tablespace_table_move()
    for rTable in
        with owner_namespace as
        (
            select pg_namespace.oid,
                   pg_namespace.nspname as name
              from pg_roles
                   inner join pg_namespace
                        on pg_namespace.nspowner = pg_roles.oid
             where pg_roles.rolname = _utility.role_get()
        ),
        owner_table as
        (
            select pg_table.oid,
                   owner_namespace.oid as namespace_oid,
                   pg_table.relname as name,
                   pg_table.reltablespace as tablespace_oid
              from owner_namespace
                   inner join pg_class pg_table
                        on pg_table.relnamespace = owner_namespace.oid
                       and pg_table.relkind = 'r'
        ),
        unassigned_table as
        (
            select owner_table.oid
              from owner_table
                   left outer join pg_tablespace pg_table_tablespace
                        on pg_table_tablespace.oid = owner_table.tablespace_oid
                   left outer join pg_index pg_index_rel
                        on pg_index_rel.indrelid = owner_table.oid
                   left outer join pg_class pg_index
                        on pg_index.oid = pg_index_rel.indexrelid
                   left outer join pg_tablespace pg_index_tablespace
                        on pg_index_tablespace.oid = pg_index.reltablespace
             where bForce
                or pg_table_tablespace.spcname is null
                or pg_table_tablespace.spcname not like 'rn_data_%'
                or (pg_index_rel.indexrelid is not null and
                       (pg_index_tablespace.spcname is null or
                        pg_index_tablespace.spcname not like 'rn_data_%'))
             group by owner_table.oid
        )
        select owner_namespace.name as schema_name,
               owner_table.name
          from owner_namespace
               inner join owner_table
                    on owner_table.namespace_oid = owner_namespace.oid
               inner join unassigned_table
                    on unassigned_table.oid = owner_table.oid
         order by owner_namespace.name,
                  owner_table.name
    loop
        perform _utility.tablespace_table_move(rTable.schema_name, rTable.name, bForce);
    end loop;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.tablespace_move(boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;