/***********************************************************************************************************************************
* UTILITY Schema Catalog Functions
*
* Helper functions for querying the system catalog.
***********************************************************************************************************************************/

/***********************************************************************************************************************************
* CATALOG_FIELD_LIST_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_field_list_get(oRelationId oid, iyColumn int[]) returns text[] as $$
declare
    rAttribute record;
    stryList text[];
    strField text;
begin
    for rAttribute in
        select * 
          from (select unnest(iyColumn) as column_id) columns
    loop
        strField = null;

        if rAttribute.column_id <> 0 then
            strField = 
            (
                select attname 
                  from pg_attribute 
                 where attrelid = oRelationId
                   and attnum = rAttribute.column_id
            );

            if strField is null then
                raise exception 'strList is null. iyColumn = %, rAttribute.column_id = %', iyColumn, rAttribute.column_id;
            end if;
        end if;

        stryList[coalesce(array_upper(stryList, 1), 0) + 1] = strField;
    end loop;

    return stryList;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_field_list_get(oid, int[]) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_INDEX_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_index_get(strSchemaName text, strIndexName text) returns _utility.catalog_index as $$
declare
    oidId oid;
    rIndex _utility.catalog_index;
    iIndex int;
begin
    select pg_class.oid,
           pg_namespace.nspname,
           pg_class_table.relname,
           pg_class.relname,
           pg_index.indisunique,
           _utility.catalog_field_list_get(pg_index.indrelid, pg_index.indkey)
      into oidId,
           rIndex.schema_name,
           rIndex.table_name,
           rIndex.index_name,
           rIndex.is_unique,
           rIndex.field_list
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strIndexName
           inner join pg_index
                on pg_index.indexrelid = pg_class.oid
           inner join pg_class pg_class_table
                on pg_class_table.oid = pg_index.indrelid
     where pg_namespace.nspname = strSchemaName;

    for iIndex in 1..array_upper(rIndex.field_list, 1) loop
        if rIndex.field_list[iIndex] is null then
            rIndex.field_list[iIndex] = pg_catalog.pg_get_indexdef(oidId, iIndex, false);
        end if;
    end loop;

    if oidId is null then
        return null;
    end if;

    return rIndex;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_index_get(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_INDEX_LIST_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_index_list_get(strSchemaName text, strTableName text, bIncludeConstraint boolean default false) returns setof text as $$
declare
    rIndex record;
begin
    for rIndex in
        select pg_class_index.relname as name
          from pg_namespace
               inner join pg_class
                    on pg_class.relnamespace = pg_namespace.oid
                   and pg_class.relname = strTableName
               inner join pg_index
                    on pg_index.indrelid = pg_class.oid
               inner join pg_class pg_class_index
                    on pg_class_index.oid = pg_index.indexrelid
         where pg_namespace.nspname = strSchemaName
           and (bIncludeConstraint = true or 
               pg_class_index.relname not in
        (
            select conname
              from pg_namespace
                   inner join pg_class
                        on pg_class.relnamespace = pg_namespace.oid
                       and pg_class.relname = strTableName
                   inner join pg_constraint
                        on pg_constraint.conrelid = pg_class.oid
             where pg_namespace.nspname = strSchemaName
        ))
         order by pg_class_index.relname
    loop
        return next rIndex.name;
    end loop;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_index_list_get(text, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_INDEX_CREATE_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_index_create_get(rIndex _utility.catalog_index) returns text as $$
begin
    if length(rIndex.index_name) > 63 then
        raise exception 'Name is too long for index % on table %.%', rIndex.index_name, rIndex.schema_name, rIndex.table_name;
    end if;

    return 'create ' || case rIndex.is_unique when true then 'unique ' else '' end || 'index ' || rIndex.index_name || ' on ' ||
           rIndex.schema_name || '.' || rIndex.table_name || ' (' || array_to_string(rIndex.field_list, ', ') || ')' ||
           case when rIndex.tablespace_name is not null then ' tablespace ' || rIndex.tablespace_name else '' end;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_index_create_get(_utility.catalog_index) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_INDEX_DROP_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_index_drop_get(rIndex _utility.catalog_index) returns text as $$
begin
    return 'drop index ' || rIndex.schema_name || '.' || rIndex.index_name;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_index_drop_get(_utility.catalog_index) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_CONSTRAINT_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_constraint_get(strSchemaName text, strTableName text, strConstraintName text) returns _utility.catalog_constraint as $$
declare
    oidId oid;
    rConstraint _utility.catalog_constraint;
begin
    select pg_constraint.oid,
           pg_namespace.nspname,
           pg_class.relname,
           pg_constraint.conname,
           pg_constraint.contype,
           _utility.catalog_field_list_get(pg_constraint.conrelid, pg_constraint.conkey),
           _utility.catalog_field_list_get(pg_constraint.confrelid, pg_constraint.confkey),
           pg_namespace_fk.nspname,
           pg_class_fk.relname,
           pg_constraint.consrc,
           case confupdtype when 'r' then 'restrict' when 'c' then 'cascade' when 'n' then 'set null' when 'd' then 'set default' else null end,
           case confdeltype when 'r' then 'restrict' when 'c' then 'cascade' when 'n' then 'set null' when 'd' then 'set default' else null end,
           case pg_constraint.condeferrable or pg_constraint.condeferred
               when false then null
               else trim(case pg_constraint.condeferrable when true then ' deferrable' else '' end || case pg_constraint.condeferred when true then ' initially deferred' else '' end)
           end
      into oidId,
           rConstraint.schema_name,
           rConstraint.table_name,
           rConstraint.constraint_name,
           rConstraint.type,
           rConstraint.field_list,
           rConstraint.field_list_fk,
           rConstraint.schema_name_fk,
           rConstraint.table_name_fk,
           rConstraint.source,
           rConstraint.on_update,
           rConstraint.on_delete,
           rConstraint.defer
      from pg_namespace
           inner join pg_constraint
                on pg_constraint.connamespace = pg_namespace.oid
               and pg_constraint.conname = strConstraintName
           inner join pg_class
                on pg_class.oid = pg_constraint.conrelid
               and pg_class.relname = strTableName
           left outer join pg_class pg_class_fk
                on pg_class_fk.oid = pg_constraint.confrelid
           left outer join pg_namespace pg_namespace_fk
                on pg_namespace_fk.oid = pg_class_fk.relnamespace
     where pg_namespace.nspname = strSchemaName;

    return rConstraint;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_constraint_get(text, text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_CONTRAINT_LIST_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_constraint_list_get(strSchemaName text, strTableName text, stryType text[] default '{p,u,f,c}') returns setof text as $$
declare
    rConstraint record;
begin
    for rConstraint in
        select pg_constraint.conname as name
          from pg_namespace
               inner join pg_class
                    on pg_class.relnamespace = pg_namespace.oid
                   and pg_class.relname = strTableName
               inner join pg_constraint
                    on pg_constraint.conrelid = pg_class.oid
                   and pg_constraint.contype in (select * from unnest(stryType))
         where pg_namespace.nspname = strSchemaName
         order by pg_constraint.conname
    loop
        return next rConstraint.name;
    end loop;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_constraint_list_get(text, text, text[]) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_CONSTRAINT_CREATE_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_constraint_create_get(rConstraint _utility.catalog_constraint) returns text as $$
declare
    strCreate text = 'alter table ' || rConstraint.schema_name || '.' || rConstraint.table_name || ' add constraint ' || rConstraint.constraint_name;
begin
    if length(rConstraint.constraint_name) > 63 then
        raise exception 'Name is too long for index % on table %.%', rConstraint.constraint_name, rConstraint.schema_name, rConstraint.table_name;
    end if;

    strCreate = strCreate || ' ' ||
                case rConstraint.type
                    when 'u' then 'unique'
                    when 'p' then 'primary key'
                    when 'f' then 'foreign key'
                    when 'c' then 'check'
                end;

    strCreate = strCreate || ' ' ||
                case rConstraint.type
                    when 'c' then rConstraint.source
                    else '(' || array_to_string(rConstraint.field_list, ', ') || ')'
                end;

    if rConstraint.type = 'f' then
        strCreate = strCreate || ' references ' || rConstraint.schema_name_fk || '.' || rConstraint.table_name_fk || ' (' || array_to_string(rConstraint.field_list_fk, ', ') || ')';
    end if;

    if rConstraint.tablespace_name is not null and rConstraint.type in ('u', 'p') then
        strCreate = strCreate || ' using index tablespace ' || rConstraint.tablespace_name;
    end if;

    if rConstraint.type = 'f' then
        if rConstraint.on_delete is not null then
            strCreate = strCreate || ' on delete ' || rConstraint.on_delete;
        end if;

        if rConstraint.on_update is not null then
            strCreate = strCreate || ' on update ' || rConstraint.on_update;
        end if;
    end if;
    
    if rConstraint.defer is not null then
        strCreate = strCreate || ' ' || rConstraint.defer;
    end if;

    return strCreate;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_constraint_create_get(_utility.catalog_constraint) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_CONTRAINT_DROP_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_constraint_drop_get(rConstraint _utility.catalog_constraint) returns text as $$
begin
    return 'alter table ' || rConstraint.schema_name || '.' || rConstraint.table_name || ' drop constraint ' || rConstraint.constraint_name;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_constraint_drop_get(_utility.catalog_constraint) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_FUNCTION_EXISTS Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_function_exists(strSchemaName text, strFunctionName text) returns boolean as $$
begin
    if 
    (
        select count(*) > 0
          from pg_namespace
               inner join pg_proc
                    on pg_proc.pronamespace = pg_namespace.oid
                   and pg_proc.proname = strFunctionName
         where nspname = strSchemaName
    ) then
        return true;
    end if;
        
    return false;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_function_exists(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_SCHEMA_EXISTS Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_schema_exists(strSchemaName text) returns boolean as $$
begin
    return
    (
        select count(*) > 0
          from pg_namespace
         where nspname = strSchemaName
    );
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_schema_exists(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_COLUMN_MOVE Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_column_move
(
    strSchemaName text, 
    strTableName text, 
    strColumnName text, 
    strColumnNameBefore text
)
    returns void as $$
declare
    rIndex record;
    rConstraint record;
    rColumn record;
    strSchemaTable text = strSchemaName || '.' || strTableName;
    strDdl text;
    strClusterIndex text;
begin
    -- Raise notice that a reorder is in progress
    raise notice 'Reorder columns in table %.% (% before %)', strSchemaName, strTableName, strColumnName, strColumnNameBefore;

    -- Get the cluster index
    select pg_index.relname
      into strClusterIndex
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strTableName
           inner join pg_index pg_index_map
                on pg_index_map.indrelid = pg_class.oid
               and pg_index_map.indisclustered = true
           inner join pg_class pg_index
                on pg_index.oid = pg_index_map.indexrelid
     where pg_namespace.nspname = strSchemaName;

    if strClusterIndex is null then
        raise exception 'Table %.% must have a cluster index before reordering', strSchemaName, strTableName;
    end if;

    -- Disable all user triggers
    strDdl = 'alter table ' || strSchemaTable || ' disable trigger user';
    raise notice '        Disable triggers [%]', strDdl;
    execute strDdl;

    -- Create temp table to hold ddl
    create temp table temp_catalogtablecolumnreorder
    (
        type text not null,
        name text not null,
        ddl text not null
    );

    -- Save index ddl in a temp table
    raise notice '    Save indexes';

    for rIndex in
        with index as
        (
            select _utility.catalog_index_list_get(strSchemaName, strTableName) as name
        ),
        index_ddl as
        (
            select index.name,
                   _utility.catalog_index_create_get(_utility.catalog_index_get(strSchemaName, index.name)) as ddl
              from index
        )
        select index.name,
               index_ddl.ddl
          from index
               left outer join index_ddl
                    on index_ddl.name = index.name
                   and index_ddl.ddl not like '%[function]%'
    loop
        raise notice '        Save %', rIndex.name;
        insert into temp_catalogtablecolumnreorder values ('index', rIndex.name, rIndex.ddl);
    end loop;

    -- Save constraint ddl in a temp table
    raise notice '    Save constraints';

    for rConstraint in
        with constraint_list as
        (
            select _utility.catalog_constraint_list_get(strSchemaName, strTableName, '{p,u,f,c}') as name
        ),
        constraint_ddl as
        (
            select constraint_list.name,
                   _utility.catalog_constraint_create_get(_utility.catalog_constraint_get(strSchemaName, strTableName, 
                                                                                          constraint_list.name)) as ddl
              from constraint_list
        )
        select constraint_list.name,
               constraint_ddl.ddl
          from constraint_list
               left outer join constraint_ddl
                    on constraint_ddl.name = constraint_list.name
    loop
        raise notice '        Save %', rConstraint.name;
        insert into temp_catalogtablecolumnreorder values ('constraint', rConstraint.name, rConstraint.ddl);
    end loop;

    -- Move column
    for rColumn in
        with table_column as
        (
            select pg_attribute.attname as name,
                   rank() over (order by pg_attribute.attnum) as rank,
                   pg_type.typname as type,
                   case when pg_attribute.atttypmod = -1 then null else ((atttypmod - 4) >> 16) & 65535 end as precision,
                   case when pg_attribute.atttypmod = -1 then null else (atttypmod - 4) & 65535 end as scale,
                   not pg_attribute.attnotnull as nullable,
                   pg_attrdef.adsrc as default,
                   pg_attribute.*
              from pg_namespace
                   inner join pg_class
                        on pg_class.relnamespace = pg_namespace.oid
                       and pg_class.relname = strTableName
                   inner join pg_attribute
                        on pg_attribute.attrelid = pg_class.oid
                       and pg_attribute.attnum >= 1
                       and pg_attribute.attisdropped = false
                   inner join pg_type
                        on pg_type.oid = pg_attribute.atttypid
                   left outer join pg_attrdef
                        on pg_attrdef.adrelid = pg_class.oid
                       and pg_attrdef.adnum = pg_attribute.attnum
             where pg_namespace.nspname = strSchemaName
             order by pg_attribute.attnum
        )
        select table_column.*
          from table_column table_column_before
               inner join table_column
                    on table_column.rank >= table_column_before.rank
                   and table_column.name <> strColumnName
         where table_column_before.name = strColumnNameBefore
    loop
        raise notice '    Move column %', rColumn.name;

        strDdl = 'alter table ' || strSchemaTable || ' rename column "' || rColumn.name || '" to "@' || rColumn.name || '@"';
        raise notice '        Rename [%]', strDdl;
        execute strDdl;
        
        strDdl = 'alter table ' || strSchemaTable || ' add "' || rColumn.name || '" ' || rColumn.type ||
                 case when rColumn.precision is not null then '(' || rColumn.precision || ', ' || rColumn.scale || ')' else '' end;
        raise notice '        Create [%]', strDdl;
        execute strDdl;
        
        strDdl = 'update ' || strSchemaTable || ' set "' || rColumn.name || '" = "@' || rColumn.name || '@"';
        raise notice '        Copy [%]', strDdl;
        execute strDdl;

        strDdl = 'alter table ' || strSchemaTable || ' drop column "@' || rColumn.name || '@"';
        raise notice '        Drop [%]', strDdl;
        execute strDdl;

        if rColumn."default" is not null then
            strDdl = 'alter table ' || strSchemaTable || ' alter column "' || rColumn.name || '" set default ' || rColumn.default;
            raise notice '        Default [%]', strDdl;
            execute strDdl;
        end if;

        if rColumn.nullable = false then
            strDdl = 'alter table ' || strSchemaTable || ' alter column "' || rColumn.name || '" set not null';
            raise notice '        Not Null [%]', strDdl;
            execute strDdl;
        end if;
    end loop;

    -- Rebuild indexes
    raise notice '    Rebuild indexes';

    for rIndex in
        select name,
               ddl
          from temp_catalogtablecolumnreorder
         where type = 'index'
    loop
        begin
            execute rIndex.ddl;
            raise notice '        Rebuild % [%]', rIndex.name, rIndex.ddl;
        exception
            when duplicate_table then
                raise notice '        Skip % [%]', rIndex.name, rIndex.ddl;
        end;
    end loop;

    -- Rebuild constraints
    raise notice '    Rebuild constraints';
    
    for rConstraint in
        select name,
               ddl
          from temp_catalogtablecolumnreorder
         where type = 'constraint'
    loop
        begin
            execute rConstraint.ddl;
            raise notice '        Rebuild % [%]', rConstraint.name, rConstraint.ddl;
        exception
            when duplicate_object or duplicate_table or invalid_table_definition then
                raise notice '        Skip % [%]', rConstraint.name, rConstraint.ddl;
        end;
    end loop;

    -- Recluster table
    strDdl = 'cluster ' || strSchemaTable || ' using ' || strClusterIndex;
    raise notice '    Recluster [%]', strDdl;
    execute strDdl;

    -- Enable all user triggers
    strDdl = 'alter table ' || strSchemaTable || ' enable trigger user';
    raise notice '    Enable triggers [%]', strDdl;
    execute strDdl;

    -- Drop temp tables
    drop table temp_catalogtablecolumnreorder;
end
$$ language plpgsql security invoker;

comment on function _utility.catalog_table_column_move(text, text, text, text) is
'Moves a column before another column in a table.  For example:

{{perform _utility.catalog_table_column_move(''attribute'', ''attribute'', ''target'', ''active'');}}

will position the "target" column right before the "active" column.  It''s not currently possible to directly move a column to the
right but this can be achieved by multiple moves of columns to the left.

There are a few caveats:
* The table must have a cluster index.  Moving columns is messy on the storage and the table needs to be re-clustered afterwards.
* Column referencing triggers will not automatically be dropped or rebuilt.
* Column specific permissions are not restored after the move.
* A column cannot be moved before the primary key if there are foreign key references from other tables.';

do $$
declare
    strSchemaName text = '_utility';
    strFunctionName text = 'catalog_table_column_move';
begin
    if _utility.catalog_function_exists('_build', 'build_info_function_parameter') then
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strSchemaName',
                                                     'Schema name.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strTableName', 
                                                     'Table name.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strColumnName', 
                                                     'Column to move.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strColumnBeforeName', 
                                                     'Column to be moved will be positioned before this column.');
    end if;
    
    execute 'grant execute on function _utility.catalog_table_column_move(text, text, text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_EXISTS Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_exists(strSchemaName text, strTableName text) returns boolean as $$
declare
    oidNamespace oid;
begin
    if strSchemaName is null then
        oidNamespace = pg_my_temp_schema();
    else
        select oid
          into oidNamespace
          from pg_namespace
         where nspname = strSchemaName;
    end if;

    return
    (
        select count(*) <> 0
          from pg_class
         where relnamespace = oidNamespace
           and relname = strTableName
           and relkind = 'r'
    );
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_table_exists(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_ACL_LIST_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_acl_list_get(strSchemaName text, strTableName text) returns setof _utility.catalog_table_acl as $$
declare
    rAclExplode record;
    rACL _utility.catalog_table_acl;
    aACL aclitem[];
begin
    select relacl
      into aACL
      from pg_class
           inner join pg_namespace
                on (pg_class.relnamespace = pg_namespace.oid)
     where pg_namespace.nspname = strSchemaName
       and pg_class.relname = strTableName;

    rACL.schema_name = strSchemaName;
    rACL.table_name = strTableName;

    for rAclExplode in
        select *
          from aclexplode(aACL)
    loop
        select rolname into rACL.grantor from pg_roles where oid = rAclExplode.grantor;
        select rolname into rACL.grantee from pg_roles where oid = rAclExplode.grantee;
        rACL.privilege_type = rAclExplode.privilege_type;
        rACL.is_grantable = rAclExplode.is_grantable;
        return next rACL;
    end loop;

    return;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_table_acl_list_get(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_FIELD_ACL_LIST_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_field_acl_list_get(strSchemaName text, strTableName text, strColumnName text) returns setof _utility.catalog_field_acl as $$
declare
    rAclExplode record;
    rACL _utility.catalog_field_acl;
    aACL aclitem[];
begin
    select attacl
      into aACL
      from pg_attribute
           inner join pg_class
                on (pg_attribute.attrelid = pg_class.oid)
           inner join pg_namespace
                on (pg_class.relnamespace = pg_namespace.oid)
     where pg_namespace.nspname = strSchemaName
       and pg_class.relname = strTableName
       and pg_attribute.attname = strColumnName;

    rACL.schema_name = strSchemaName;
    rACL.table_name = strTableName;
    rACL.column_name = strColumnName;

    for rAclExplode in
        select *
          from aclexplode(aACL)
    loop
        select rolname into rACL.grantor from pg_roles where oid = rAclExplode.grantor;
        select rolname into rACL.grantee from pg_roles where oid = rAclExplode.grantee;
        rACL.privilege_type = rAclExplode.privilege_type;
        rACL.is_grantable = rAclExplode.is_grantable;
        return next rACL;
    end loop;

    return;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_field_acl_list_get(text, text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_ALL_FIELD_ACL_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_all_field_acl_get(strSchemaName text, strTableName text) returns setof _utility.catalog_field_acl as $$
declare
    rFieldACL _utility.catalog_field_acl;
    rAttName record;
begin
    for rAttName in
        select attname
          from pg_namespace
               inner join pg_class
                   on (pg_namespace.oid = pg_class.relnamespace)
               inner join pg_attribute
                   on (pg_class.oid = pg_attribute.attrelid)
         where pg_namespace.nspname = strSchemaName
           and pg_class.relname = strTableName
    loop
        for rFieldACL in
            select * from _utility.catalog_field_acl_list_get(strSchemaName,strTableName,rAttName.attname)
        loop
            return next rFieldACL;
        end loop;
    end loop;

    return;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_all_field_acl_get(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_ACL_GET_GRANT Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_acl_get_grant(rTableACL _utility.catalog_table_acl) returns text as $$
declare
    tResult text;
begin
    tResult := 'GRANT '
            || rTableACL.privilege_type
            || ' ON '
            || quote_ident(rTableACL.schema_name) || '.' || quote_ident(rTableACL.table_name)
            || ' TO ' || rTableACL.grantee
            || CASE WHEN rTableACL.is_grantable THEN ' WITH GRANT OPTION '::text ELSE ''::text END
            || ';';

    return tResult;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.catalog_table_acl_get_grant(_utility.catalog_table_acl) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_ACL_GET_REVOKE Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_acl_get_revoke(rTableACL _utility.catalog_table_acl) returns text as $$
declare
    tResult text;
begin
    tResult := 'REVOKE '
            || rTableACL.privilege_type
            || ' ON '
            || quote_ident(rTableACL.schema_name) || '.' || quote_ident(rTableACL.table_name)
            || ' FROM ' || rTableACL.grantee
            || ';';

    return tResult;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.catalog_table_acl_get_revoke(_utility.catalog_table_acl) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_FIELD_ACL_GET_GRANT Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_field_acl_get_grant(rFieldACL _utility.catalog_field_acl) returns text as $$
declare
    tResult text;
begin
    tResult := 'GRANT '
            || rFieldACL.privilege_type
            || ' (' || quote_ident(rFieldACL.column_name) || ')'
            || ' ON '
            || quote_ident(rFieldACL.schema_name) || '.' || quote_ident(rFieldACL.table_name)
            || ' TO ' || rFieldACL.grantee
            || CASE WHEN rFieldACL.is_grantable THEN ' WITH GRANT OPTION '::text ELSE ''::text END
            || ';';

    return tResult;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_field_acl_get_grant(_utility.catalog_field_acl) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_FIELD_ACL_GET_REVOKE Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_field_acl_get_revoke(rFieldACL _utility.catalog_field_acl) returns text as $$
declare
    tResult text;
begin
    tResult := 'REVOKE '
            || rFieldACL.privilege_type
            || ' (' || quote_ident(rFieldACL.column_name) || ')'
            || ' ON '
            || quote_ident(rFieldACL.schema_name) || '.' || quote_ident(rFieldACL.table_name)
            || ' FROM ' || rFieldACL.grantee
            || ';';

    return tResult;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.catalog_field_acl_get_revoke(_utility.catalog_field_acl) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* CATALOG_TABLE_UNREFERENCED_KEY_GET Function
***********************************************************************************************************************************/
create or replace function _utility.catalog_table_unreferenced_key_get
(
    strSchemaName text,
    strTableName text,
    strColumnName text,
    bExcludeCascade boolean default false
) 
    returns setof text as $$   
declare
    strSql text;
    rKey record;
begin
    strSql =
'select ' || strColumnName || '::text 
  from ' || strSchemaName || '.' || strTableName || ' as referenced';

    for rKey in
        with constraint_data as
        (
            select pg_constraint.oid as constraint_oid,
                   table_referenced.oid as table_referenced_oid,
                   pg_constraint.confkey as table_referenced_key,
                   table_referencing.oid as table_referencing_oid,
                   pg_constraint.conkey as table_referencing_key
              from pg_namespace
                   inner join pg_class table_referenced
                        on table_referenced.relnamespace = pg_namespace.oid
                       and table_referenced.relname = strTableName
                   inner join pg_constraint
                        on pg_constraint.confrelid = table_referenced.oid
                       and not (coalesce(pg_constraint.confdeltype = 'c', false) and bExcludeCascade)
                   inner join pg_class table_referencing
                        on table_referencing.oid = pg_constraint.conrelid
             where pg_namespace.nspname = strSchemaName
        ),
        referenced_key as
        (
            select constraint_data.constraint_oid,
                   key_subscript.subscript,
                   constraint_data.table_referenced_key[subscript] as value
              from constraint_data
                   inner join
                (
                    select constraint_oid,
                           generate_subscripts(constraint_data.table_referenced_key, 1) as subscript
                      from constraint_data
                ) key_subscript
                        on key_subscript.constraint_oid = constraint_data.constraint_oid
        )
        select rank() over (order by pg_namespace.nspname, table_referencing.relname, column_referencing.attname) as rank,
               pg_namespace.nspname as schema_name,
               table_referencing.relname as table_name,
               column_referencing.attname as column_name
          from constraint_data
               inner join referenced_key
                    on referenced_key.constraint_oid = constraint_data.constraint_oid
               inner join pg_attribute column_referenced
                    on column_referenced.attrelid = constraint_data.table_referenced_oid
                   and column_referenced.attnum = referenced_key.value
                   and column_referenced.attname = strColumnName
               inner join pg_attribute column_referencing
                    on column_referencing.attrelid = constraint_data.table_referencing_oid
                   and column_referencing.attnum = constraint_data.table_referencing_key[referenced_key.subscript]
               inner join pg_class as table_referencing
                    on table_referencing.oid = constraint_data.table_referencing_oid
               inner join pg_namespace
                    on pg_namespace.oid = table_referencing.relnamespace
         group by pg_namespace.nspname,
                  table_referencing.relname,
                  column_referencing.attname
         order by pg_namespace.nspname,
                  table_referencing.relname,
                  column_referencing.attname
    loop
        if rKey.rank = 1 then
            strSql = strSql || '
 where ';
        else
            strSql = strSql || '
   and ';
        end if;

        strSql = strSql || 'not exists
(
    select 1
      from ' || rKey.schema_name || '.' || rKey.table_name || ' as referencing
     where referencing.' || rKey.column_name || ' = referenced.' || strColumnName || '
)';
    end loop;

    return query execute strSql;
end;
$$ language plpgsql security invoker;

comment on function _utility.catalog_table_unreferenced_key_get(text, text, text, boolean) is
'Finds unreferenced single-column keys by searching all tables that reference the key.  Primarily intended for use on synthetic primary keys
though it should work fine on any single-column key.';

do $$
declare
    strSchemaName text = '_utility';
    strFunctionName text = 'catalog_table_unreferenced_key_get';
begin
    if _utility.catalog_function_exists('_build', 'build_info_function_parameter') then
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strSchemaName', 'Schema of the table to search.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strTableName', 'Name of the column to search.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'strColumnName', 'Name of the column that contrains the single-column key.');
        perform _build.build_info_function_parameter(strSchemaName, strFunctionName, 'bExcludeCascade', 'Exclude any tables where delete are set to cascade.');
    end if;
end $$;

do $$
begin
    execute 'grant execute on function _utility.catalog_table_unreferenced_key_get(text, text, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;
