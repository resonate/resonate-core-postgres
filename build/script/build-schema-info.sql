/***********************************************************************************************************************************
build-schema-info.sql

Add the info tables and functions to the build schema.

Known limitations:
- Does not check column default values (seems to be blowing up on some sequences)
- Does not check functions in function based indexes (currently inserts "function()" as a placholder)
- Does not check trigger when conditions
***********************************************************************************************************************************/
create sequence _build.buildinfo_id_seq;

/***********************************************************************************************************************************
* Tables to hold db structure for update builds
***********************************************************************************************************************************/
create table _build.build_info
(
    id int not null,
    oid oid,
    parent_id int
        constraint buildinfo_parentid_fk references _build.build_info (id)
        constraint buildinfo_parentid_ck check ((type = 'db' and parent_id is null) or parent_id is not null),
    type text not null
        constraint buildinfo_type_ck check (type in  ('db', 'schema', 'table', 'view', 'column', 'constraint', 'index', 'function', 'trigger')),
    name text not null,
    owner text,
    acl text,
    meta text,
    meta_hash text,
    comment_build_id int
        constraint buildinfo_commentbuildid_fk references _build.build_info (id),
    comment text,
    constraint buildinfo_pk primary key (id)
);

create index buildinfo_parentid_name_idx on _build.build_info (parent_id, name);
create index buildinfo_commentbuildid_idx on _build.build_info (comment_build_id);

create table _build.build_info_function_parameter
(
    schema_name text not null,
    name text not null,
    parameter_name text not null,
    comment text not null,
    constraint buildinfofunctionparameter_pk primary key (schema_name, name, parameter_name)
);

/***********************************************************************************************************************************
* BUILD_INFO_FUNCTION_PARAMETER Function
***********************************************************************************************************************************/
create or replace function _build.build_info_function_parameter
(
    strSchemaName text,
    strName text, 
    strParameterName text, 
    strComment text
)
    returns void as $$
begin
    begin
        insert into _build.build_info_function_parameter values (strSchemaName, strName, strParameterName, strComment);
    exception
        when unique_violation then
            update _build.build_info_function_parameter
               set comment = strComment
             where schema_name = strSchemaName
               and name = strName
               and parameter_name = strParameterName;
    end;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_function_return
(
    strSchemaName text,
    strName text,
    strComment text
)
    returns void as $$
begin
    insert into _build.build_info_function_parameter values (strSchemaName, strName, '@return', strComment);
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* BUILD_INFO_HAS_PRIVILEGE Function
***********************************************************************************************************************************/
create or replace function _build.build_info_has_privilege
(
    stryRole text[], 
    strType text, 
    strName text, 
    strOperation text, 
    bCompare boolean
)
    returns boolean as $$
declare
    strSql text;
    bReturn boolean;
begin
    strSql = 'select sum(case when (has_' || strType || '_privilege(name, ' || quote_literal(strName) || ', ' || quote_literal(strOperation) || ') = ' || bCompare::text || E') then 1 else 0 end) > 0\n' ||
             '  from unnest(' || quote_literal(stryRole::text) || '::text[]) name';

    execute strSql into bReturn;

    return bReturn;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* Fill the build tables
***********************************************************************************************************************************/
create or replace function _build.contrib_hash(text, text) returns bytea
as '$libdir/pgcrypto', 'pg_digest'
language c immutable strict;

create or replace function _build.contrib_hash_md5(strData text) returns text as $$
begin
    return encode(_build.contrib_hash(strData, 'md5'), 'hex');
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_acl(acl aclitem[]) returns text as $$
begin
    if acl is null then
        return null;
    end if;
    
    return
    (
        select array_to_string(array_agg(acl_item), ',')
          from
            (  
                select unnest(acl)::text as acl_item
                 order by acl_item
            ) acl
    );
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_owner(oidOwner oid) returns text as $$
begin
    if oidOwner is null then
        return null;
    end if;
    
    return (select rolname from pg_roles where oid = oidOwner);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_field_list(oRelationId oid, iyColumn int[]) returns text as $$
declare
    rAttribute record;
    strList text = '';
begin
    for rAttribute in
        select * 
          from (select unnest(iyColumn) as column_id) columns
    loop
        if strList <> '' then
            strList = strList || ', ';
        end if;
        
        if rAttribute.column_id = 0 then
            strList = strList || '[function]';
        else
            strList = strList ||
            (
                select attname 
                  from pg_attribute 
                 where attrelid = oRelationId
                   and attnum = rAttribute.column_id
            );
        end if;
        
        if strList is null then
            raise exception 'strList is null. iyColumn = %, rAttribute.column_id = %', iyColumn, rAttribute.column_id;
        end if;
    end loop;

    return strList;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_set(iParentId int, oidId oid, strType text, strName text, strOwner text, strAcl text, strMeta text, strMetaHash text, iCommentBuildId int, strComment text) returns int as $$
declare
    iId int;
    iRank int;
begin
    if iParentId is null then
        iId = 
        (
            select id 
              from _build.build_info 
             where parent_id is null
               and type = strType
               and name = strName
        );
    else
        iId = 
        (
            select id 
              from _build.build_info 
             where parent_id = iParentId
               and type = strType
               and name = strName
        );
    end if;

    if iId is null then
        iId = (select nextval('_build.buildinfo_id_seq'));
        
        insert into _build.build_info ( id, parent_id,    type,    name,    owner,    acl,    meta,   meta_hash, comment_build_id,    comment,   oid)
                               values (iId, iParentId, strType, strName, strOwner, strAcl, strMeta, strMetaHash,  iCommentBuildId, strComment, oidId);
    else
        update _build.build_info
           set owner = strOwner,
               acl = strAcl,
               meta = strMeta,
               meta_hash = strMetaHash,
               comment = coalesce(strComment, comment)
         where id = iId;
    end if;

    return iId;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_db_get_max() returns int as $$
begin
    return (select max(id) from _build.build_info where type = 'db');
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_db_set(strName text, strComment text default null) returns int as $$
declare
    oidId oid;
    strOwner text;
    strAcl text;
begin
    select oid,
           _build.build_info_owner(pg_database.datdba),
           _build.build_info_acl(pg_database.datacl)
      into oidId,
           strOwner,
           strAcl
      from pg_database
     where datname = current_database();

    if oidId is null then
        raise exception 'Invalid database: %', strName;
    end if;

    return _build.build_info_set(null, oidId, 'db', strName, strOwner, strAcl, null, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_schema_get(strName text) returns int as $$
begin
    return 
    (
        select id
          from _build.build_info
         where type = 'schema' 
           and parent_id = _build.build_info_db_get_max() 
           and name = strName
    );
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_schema_set(strName text) returns text as $$
declare
    iParentId int = _build.build_info_db_get_max();
    oidId oid;
    strOwner text;
    strAcl text;
    strComment text;
begin
    select pg_namespace.oid,
           _build.build_info_owner(pg_namespace.nspowner),
           _build.build_info_acl(pg_namespace.nspacl),
           pg_description.description
      into oidId,
           strOwner,
           strAcl,
           strComment
      from pg_namespace
           left outer join pg_description
               on pg_description.objoid = pg_namespace.oid
     where pg_namespace.nspname = strName;
     
    if oidId is null then
        raise exception 'Invalid schema: %', strName;
    end if;
    
    return _build.build_info_set(iParentId, oidId, 'schema', strName, strOwner, strAcl, null, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_table_get(strSchemaName text, strName text, strType text default 'table') returns int as $$
begin
    return 
    (
        select id
          from _build.build_info
         where type = strType
           and parent_id = _build.build_info_schema_get(strSchemaName)
           and name = strName
    );
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_table_set(strSchemaName text, strName text) returns text as $$
declare
    iParentId int = _build.build_info_schema_get(strSchemaName);
    oidId oid;
    strOwner text;
    strMeta text = null;
    strAcl text;
    strComment text;
    bScdMap boolean;
    bScdJournal boolean;
begin
    select pg_class.oid,
           _build.build_info_owner(pg_class.relowner),
           _build.build_info_acl(pg_class.relacl),
           pg_description.description
      into oidId,
           strOwner,
           strAcl,
           strComment
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relkind = 'r'
               and pg_class.relname = strName
           left outer join pg_description
               on pg_description.objoid = pg_class.oid
     where pg_namespace.nspname = strSchemaName;
     
    -- Error if the table does not exist
    if oidId is null then
        raise exception 'Invalid table: %.%', strSchemaName, strName;
    end if;
     
    -- Add meta data if the table is registered in _scd
    select map,
           journal
      into bScdMap,
           bScdJournal
      from _scd.config_table
     where schema_name = strSchemaName
       and table_name = strName;

    if bScdMap is not null then
        strMeta = '[{scd';

        if bScdMap or bScdJournal then 
            strMeta = strMeta || ':';
        end if;
        
        if bScdMap then 
            strMeta = strMeta || 'map';
        end if;

        if bScdJournal then 
            if bScdMap then 
                strMeta = strMeta || ',';
            end if;

            strMeta = strMeta || 'journal';
        end if;

        strMeta = strMeta || '}]';
    end if;
    
    return _build.build_info_set(iParentId, oidId, 'table', strName, strOwner, strAcl, strMeta, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_column_set(strSchemaName text, strTableName text, strName text) returns text as $$
declare
    iParentId int = coalesce(_build.build_info_table_get(strSchemaName, strTableName), _build.build_info_table_get(strSchemaName, strTableName, 'view'));
    oidId oid;
    strType text;
    iLength int;
    bNullable boolean;
    strDefault text;
    strAcl text;
    strMeta text;
    strComment text;
begin
    select pg_class.oid,
           pg_type.typname as type,
           pg_attribute.atttypmod as length,
           not pg_attribute.attnotnull as nullable,
           pg_attrdef.adsrc as default,
           _build.build_info_acl(pg_attribute.attacl) as acl,
           pg_description.description
      into oidId,
           strType,
           iLength,
           bNullable,
           strDefault,
           strAcl,
           strComment
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strTableName
           inner join pg_attribute
                on pg_attribute.attrelid = pg_class.oid
               and pg_attribute.attname = strName
           inner join pg_type
                on pg_type.oid = pg_attribute.atttypid
           left outer join pg_attrdef
                on pg_attrdef.adrelid = pg_attribute.attrelid
               and pg_attrdef.adnum = pg_attribute.attnum
           left outer join pg_description
               on pg_description.objoid = pg_class.oid
              and pg_description.objsubid = pg_attribute.attnum
     where pg_namespace.nspname = strSchemaName;

    if oidId is null then
        raise exception 'Invalid column: %.%.%', strSchemaName, strTableName, strName;
    end if;

    strMeta = strType;

    if iLength <> -1 then
        strMeta = strMeta || '(' || iLength::text || ')';
    end if;

    if not bNullable then
        strMeta = strMeta || ' not null';
    end if;

    -- !!! Figure out why this fails for sequences !!!
    if strDefault is not null then
        if strpos(strDefault, 'nextval') = 0 then
            strMeta = strMeta || ' default ' || strDefault;
        else
            strMeta = strMeta || ' default [sequence]';
        end if;
    end if;
    
    return _build.build_info_set(iParentId, oidId, 'column', strName, null, strAcl, strMeta, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_constraint_set(strSchemaName text, strTableName text, strName text, strComment text default null) returns text as $$
declare
    iParentId int = _build.build_info_table_get(strSchemaName, strTableName);
    oidId oid;
    strType text;
    strMeta text;
begin
    select pg_constraint.oid,
           pg_constraint.contype
      into oidId,
           strType
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strTableName
           inner join pg_constraint
                on pg_constraint.conrelid = pg_class.oid
               and conname = strName
     where pg_namespace.nspname = strSchemaName;

    if strType in ('p', 'u') then
        select case contype when 'p' then 'primary key' when 'u' then 'unique' else '???' end || 
               ' (' || _build.build_info_field_list(conrelid, conkey) || ')'
          into strMeta
          from pg_constraint
         where pg_constraint.oid = oidId;
    end if;

    if strType = 'c' then
        select 'check ' || consrc
          into strMeta
          from pg_constraint
         where pg_constraint.oid = oidId;
    end if;

    if strType = 'f' then
        select 'foreign key (' || _build.build_info_field_list(conrelid, conkey) || 
               ') references ' || pg_namespace.nspname || '.' || pg_class.relname || ' ('|| _build.build_info_field_list(confrelid, confkey) || ')' ||
               case when confupdtype <> 'a' then ' on update ' || case confupdtype when 'r' then 'restrict' when 'c' then 'cascade' when 'n' then 'set null' when 'd' then 'set default' else '[unknown]' end else '' end ||
               case when confdeltype <> 'a' then ' on delete ' || case confdeltype when 'r' then 'restrict' when 'c' then 'cascade' when 'n' then 'set null' when 'd' then 'set default' else '[unknown]' end else '' end
          into strMeta
          from pg_constraint
               inner join pg_class
                    on pg_class.oid = pg_constraint.confrelid
               inner join pg_namespace
                    on pg_namespace.oid = pg_class.relnamespace
         where pg_constraint.oid = oidId;
    end if;

    if strMeta is null then
        raise exception 'Constraint ''%'' of type ''%'' not supported', strName, strType;
    end if;

    select strMeta || 
           case condeferrable when true then ' deferrable' else '' end ||
           case condeferred when true then ' initially deferred' else '' end
      into strMeta
      from pg_constraint
     where pg_constraint.oid = oidId;

    return _build.build_info_set(iParentId, oidId, 'constraint', strName, null, null, strMeta, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_index_set(strSchemaName text, strTableName text, strName text, strComment text default null) returns text as $$
declare
    iParentId int = _build.build_info_table_get(strSchemaName, strTableName);
    oidId oid;
    strMeta text;
begin
    select pg_class_index.oid,
--           'index ' || pg_class_index.relname || ' on ' || strSchemaName || '.' || strTableName || ' (' || _build.build_info_field_list(pg_class_index.oid, pg_index.indkey) || ')'
           case pg_index.indisunique
               when true then 'unique '
               else ''
           end ||
           '(' || _build.build_info_field_list(pg_index.indrelid, pg_index.indkey) || ')'
      into oidId,
           strMeta
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strTableName
           inner join pg_index
                on pg_index.indrelid = pg_class.oid
           inner join pg_class pg_class_index
                on pg_class_index.oid = pg_index.indexrelid
               and pg_class_index.relname = strName
     where pg_namespace.nspname = strSchemaName
       and pg_class_index.relname not in 
     (
        select pg_constraint.conname
          from pg_namespace
               inner join pg_class
                    on pg_class.relnamespace = pg_namespace.oid
                   and pg_class.relname = strTableName
               inner join pg_constraint
                    on pg_constraint.conrelid = pg_class.oid
                   and conname = strName
         where pg_namespace.nspname = strSchemaName
     );

    if oidId is null then
        raise exception 'Invalid index: %.%.%', strSchemaName, strTableName, strName;
    end if;

    return _build.build_info_set(iParentId, oidId, 'index', strName, null, null, strMeta, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_function_name(oidId oid, bIncludeName boolean default false) returns text as $$
declare
    strName text;
    iArgCount int;
    iArgIdx int;
    oidyArgType oidvector;
    stryArgName text[];
    strTypeCategory text;
    strTypeName text;
    strSchema text;
begin
    select proname,
           pronargs,
           proargtypes,
           proargnames
      into strName,
           iArgCount,
           oidyArgType,
           stryArgName
      from pg_proc
     where oid = oidId;

    strName = strName || '(';

    if iArgCount > 0 then
        for iArgIdx in 0..iArgCount - 1 loop
            if iArgIdx <> 0 then
                strName = strName || ', ';
            end if;

            select pg_namespace.nspname,
                   pg_type.typcategory,
                   pg_type.typname
              into strSchema,
                   strTypeCategory,
                   strTypeName
              from pg_type
                   inner join pg_namespace
                        on pg_namespace.oid = typnamespace
             where pg_type.oid = oidyArgType[iArgIdx];

            strTypeName = 
                case strTypeName
                    when 'bpchar' then 'char'
                    when 'int8' then 'bigint'
                    when '_int8' then '_bigint'
                    when 'int4' then 'int'
                    when '_int4' then '_int'
                    when 'bool' then 'boolean'
                    when '_bool' then '_boolean'
                    else strTypeName
                end;
            
            strName = strName || case when bIncludeName then coalesce(stryArgName[iArgIdx + 1], 'unknown') || ' ' else '' end ||
                case strSchema when 'pg_catalog' then '' else strSchema || '.' end ||
                case strTypeCategory when 'A' then substr(strTypeName, 2) || '[]' else strTypeName end;
        end loop;
    end if;

    strName = strName || ')';

    if strName is null then
        raise exception 'Invalid function name: oid %, proname %, pronargs %, prodargtypes %, proargnames %, record %', oidId, strName, iArgCount, oidyArgType, stryArgName, (select pg_proc from pg_proc where oid = oidId);
    end if;

    return strName;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_function_set(strSchemaName text, strName text, strComment text default null) returns text as $$
declare
    iParentId int = _build.build_info_schema_get(strSchemaName);
    oidId oid;
    strOwner text;
    strAcl text;
    strMeta text;
    strMetaHash text;
    strComment text;
    iArgCount int;
    iArgDefaultCount int;
    iArgIdx int;
    strArg text = '';
    oidyArgType oidvector;
    stryArgName text[];
begin
    select pg_proc.oid,
           _build.build_info_owner(pg_proc.proowner),
           _build.build_info_acl(pg_proc.proacl),
           '(%%args%%) returns ' || case pg_proc.proretset when true then 'set of ' else '' end || pg_type.typname || ' ' ||
           'language ' || pg_language.lanname || ' ' ||
           case pg_proc.provolatile when 'i' then 'immutable' when 'v' then 'volatile' when 's' then 'stable' else 'unknown' end || ' ' ||
           'security ' || case pg_proc.prosecdef when true then 'definer' else 'invoker' end,
           _build.contrib_hash_md5(replace(pg_proc.prosrc, E'\r\n', E'\n')),
           pg_proc.pronargs,
           pg_proc.pronargdefaults,
           pg_proc.proargtypes,
           pg_proc.proargnames,
           pg_description.description
      into oidId,
           strOwner,
           strAcl,
           strMeta,
           strMetaHash,
           iArgCount,
           iArgDefaultCount,
           oidyArgType,
           stryArgName,
           strComment
      from pg_namespace
           inner join pg_proc
                on pg_proc.pronamespace = pg_namespace.oid
               and _build.build_info_function_name(pg_proc.oid) = strName
           inner join pg_type
                on pg_type.oid = pg_proc.prorettype
           inner join pg_language
                on pg_language.oid = pg_proc.prolang
           left outer join pg_description
               on pg_description.objoid = pg_proc.oid
     where pg_namespace.nspname = strSchemaName;

    if iArgCount > 0 then
        for iArgIdx in 0..iArgCount - 1 loop
            if iArgIdx <> 0 then
                strArg = strArg || ', ';
            end if;
            
            strArg = strArg || stryArgName[iArgIdx + 1] || ' ' || (select typname from pg_type where oid = oidyArgType[iArgIdx]);

            if iArgDefaultCount > 0 and iArgIdx + 1 > iArgDefaultCount then
                strArg = strArg || ' default';
            end if;
        end loop;

        strMeta = replace(strMeta, '%%args%%', strArg);
    else
        strMeta = replace(strMeta, '%%args%%', '');
    end if;

    if oidId is null then
        raise exception 'Invalid function: %.%', strSchemaName, strName;
    end if;

    if strName like '%_workflow_trigger_%' or strName like '%_scd_trigger_%' or strName like '%_partition_trigger_%' then
        strMetaHash = null;
    end if;

    return _build.build_info_set(iParentId, oidId, 'function', strName, strOwner, strAcl, strMeta, strMetaHash, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_trigger_set(strSchemaName text, strTableName text, strName text, strComment text default null) returns text as $$
declare
    iParentId int = _build.build_info_table_get(strSchemaName, strTableName);
    oidId oid;
    strMeta text;
begin
    select pg_trigger.oid,
           case pg_trigger.tgtype & cast(2 as int2)
               when 0 then 'after'
               else 'before'
           end ||
           case pg_trigger.tgtype & cast(4 as int2)
               when 0 then ''
               else ' insert'
           end ||
           case pg_trigger.tgtype & cast(16 as int2)
               when 0 then ''
               else
                   case pg_trigger.tgtype & cast(4 as int2)
                       when 0 then ''
                       else ' or'
                   end || ' update' || 
                   case when _build.build_info_field_list(pg_class.oid, pg_trigger.tgattr) is null
                       then ''
                       else ' of ' || _build.build_info_field_list(pg_class.oid, pg_trigger.tgattr) 
                   end
           end ||
           case pg_trigger.tgtype & cast(8 as int2)
               when 0 then ''
               else
                   case pg_trigger.tgtype & cast(20 as int2)
                       when 0 then ''
                       else ' or'
                   end || ' delete'
           end ||
           case pg_trigger.tgtype & cast(1 as int2)
               when 0 then ''
               else ' for each row'
           end ||
           ' execute ' || pg_namespace.nspname || '.' || pg_proc.proname || '()'
      into oidId,
           strMeta
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relname = strTableName
           inner join pg_trigger
                on pg_trigger.tgrelid = pg_class.oid
               and pg_trigger.tgname = strName
               and pg_trigger.tgisinternal = false
           inner join pg_proc
                on pg_proc.oid = pg_trigger.tgfoid
     where pg_namespace.nspname = strSchemaName;

    if oidId is null then
        raise exception 'Invalid trigger: %.%.%', strSchemaName, strTableName, strName;
    end if;

    return _build.build_info_set(iParentId, oidId, 'trigger', strName, null, null, strMeta, null, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_view_set(strSchemaName text, strName text, strComment text default null) returns text as $$
declare
    iParentId int = _build.build_info_schema_get(strSchemaName);
    oidId oid;
    strOwner text;
    strAcl text;
    strMetaHash text;
begin
    select pg_class.oid,
           _build.build_info_owner(pg_class.relowner),
           _build.build_info_acl(pg_class.relacl),
           _build.contrib_hash_md5(pg_views.definition),
           pg_description.description
      into oidId,
           strOwner,
           strAcl,
           strMetaHash,
           strComment
      from pg_namespace
           inner join pg_class
                on pg_class.relnamespace = pg_namespace.oid
               and pg_class.relkind = 'v'
               and pg_class.relname = strName
           inner join pg_views
               on pg_views.schemaname = strSchemaName
              and pg_views.viewname = strName
           left outer join pg_description
               on pg_description.objoid = pg_class.oid
     where pg_namespace.nspname = strSchemaName;

    if oidId is null then
        raise exception 'Invalid view: %.%', strSchemaName, strName;
    end if;
    
    return _build.build_info_set(iParentId, oidId, 'view', strName, strOwner, strAcl, null, strMetaHash, null, strComment);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info(strDbName text) returns void as $$
declare
    rSchema record;
    rTable record;
    rView record;
    rFunction record;
begin
    perform _build.build_info_db_set(strDbName);

    for rSchema in
        select pg_namespace.oid, 
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and nspname not in ('_test', '_build', '_dev', @db.validation.schema.exclusion@)
           and nspname not like '%_partition'
         order by pg_namespace.oid
    loop
        perform _build.build_info_schema_set(rSchema.name);

        for rTable in
            select pg_class.oid,
                   pg_class.relname as name
              from pg_class, pg_roles
             where pg_class.relkind = 'r'
               and pg_class.relnamespace = rSchema.oid
               and pg_class.relowner = pg_roles.oid
             order by pg_class.oid
        loop
            if not (rSchema.name = 'stage' and rTable.name not like 'stage%' and rTable.name not like 'deploy%') then
                perform _build.build_info_table_set(rSchema.name, rTable.name);

                perform _build.build_info_column_set(rSchema.name, rTable.name, pg_attribute.attname)
                   from pg_attribute
                  where pg_attribute.attrelid = rTable.oid
                    and pg_attribute.attnum >= 1
                    and pg_attribute.attisdropped = false
                  order by pg_attribute.attnum;

                perform _build.build_info_constraint_set(rSchema.name, rTable.name, pg_constraint.conname)
                   from pg_constraint 
                  where pg_constraint.conrelid = rTable.oid
                    and pg_constraint.contype <> 't'
                  order by pg_constraint.conname;

                perform _build.build_info_trigger_set(rSchema.name, rTable.name, pg_trigger.tgname)
                   from pg_trigger
                  where pg_trigger.tgrelid = rTable.oid
                    and pg_trigger.tgisinternal = false
                  order by pg_trigger.tgname;

                perform _build.build_info_index_set(rSchema.name, rTable.name, pg_class.relname)
                   from pg_index
                        inner join pg_class
                            on pg_class.oid = pg_index.indexrelid
                  where pg_index.indrelid = rTable.oid
                    and pg_class.relname not in
                (
                   select conname
                     from pg_constraint 
                    where pg_constraint.conrelid = rTable.oid
                )
                 order by pg_class.relname;
            end if;
        end loop;

        for rView in
            select pg_class.oid,
                   pg_class.relname as name
              from pg_class, pg_roles
             where pg_class.relkind = 'v'
               and pg_class.relnamespace = rSchema.oid
               and pg_class.relowner = pg_roles.oid
             order by relname
        loop
            perform _build.build_info_view_set(rSchema.name, rView.name);

            perform _build.build_info_column_set(rSchema.name, rView.name, pg_attribute.attname)
               from pg_attribute
              where pg_attribute.attrelid = rView.oid
                and pg_attribute.attnum >= 1
                and pg_attribute.attisdropped = false
              order by pg_attribute.attnum;
        end loop;

        for rFunction in
            select _build.build_info_function_name(pg_proc.oid) as name
              from pg_proc
             where pg_proc.pronamespace = rSchema.oid
               and pg_proc.proname is not null
             order by name
        loop
            if not (rSchema.name = 'stage' and rFunction.name not like 'stage%' and rFunction.name not like 'deploy%') then
                perform _build.build_info_function_set(rSchema.name, rFunction.name);
            end if;
        end loop;
    end loop;         
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_name_get(iId int) returns text as $$
declare
    strName text = null;
    strNamePart text;
    strType text;
begin
    select parent_id,
           type,
           name
      into iId,
           strType,
           strName
      from _build.build_info           
     where id = iId;

    loop
        select parent_id,
               type,
               name
          into iId,
               strType,
               strNamePart
          from _build.build_info           
         where id = iId;

        exit when strType = 'db' or iId is null;

        strName = strNamePart || '.' || strName;
    end loop;

    return strName;
end
$$ language plpgsql security definer;

create or replace function _build.build_info_validate() returns void as $$
declare
    strError text;
begin
    strError = _build.build_info_validate((select id from _build.build_info where type = 'db' and parent_id is null and name = 'full'),
                                          (select id from _build.build_info where type = 'db' and parent_id is null and name = 'update'));
                                          
    if strError is null then
        raise exception 'strError is null - this should not happen!';
    end if;

    if length(strError) > 0 then
        raise exception 'Validation errors occurred:%', strError;
    end if;
end
$$ language plpgsql security definer;

create or replace function _build.build_info_validate(iFullId int, iUpdateId int) returns text as $$
declare
    rMissing record;
    rExtra record;
    rMatching record;
    rFull record;
    rUpdate record;
    strError text = '';
begin
    for rMissing in
        select name
          from _build.build_info
         where parent_id = iFullId
            except
        select name
          from _build.build_info
         where parent_id = iUpdateId
    loop
        select initcap(type) as type,
               _build.build_info_name_get(id) as name
          into rFull
          from _build.build_info
         where parent_id = iFullId
           and name = rMissing.name;

        strError = strError || E'\n' || rFull.type || ' ' || rFull.name || ' is in the full build but not in the update scripts.';
    end loop;

    for rExtra in
        select name
          from _build.build_info
         where parent_id = iUpdateId
            except
        select name
          from _build.build_info
         where parent_id = iFullId
    loop
        select initcap(type) as type,
               _build.build_info_name_get(id) as name
          into rUpdate
          from _build.build_info
         where parent_id = iUpdateId
           and name = rExtra.name;

        strError = strError || E'\n' || rUpdate.type || ' ' || rUpdate.name || ' is in the update scripts but not in the full build.';
    end loop;

    for rMatching in
        select name
          from _build.build_info
         where parent_id = iUpdateId
            intersect
        select name
          from _build.build_info
         where parent_id = iFullId
    loop
        select id,
               initcap(type) as type,
               _build.build_info_name_get(id) as name,
               acl,
               owner,
               meta,
               meta_hash,
               comment
          into rFull
          from _build.build_info
         where parent_id = iFullId
           and name = rMatching.name;

        select id,
               acl,
               owner,
               meta,
               meta_hash
          into rUpdate
          from _build.build_info
         where parent_id = iUpdateId
           and name = rMatching.name;
        
        if lower(rFull.type) in ('schema', 'table', 'column') then
            perform _build.build_info_comment_set(lower(rFull.type), _build.build_info_name_get(rFull.id), rFull.Comment);
        end if;

        if rFull.owner is distinct from rUpdate.owner then
            strError = strError || E'\n' || rFull.type || ' ' || rFull.name || 
                       ' owner changed from ''' || coalesce(rFull.owner, '<NULL>') || ''' in the full build to ''' || coalesce(rUpdate.owner, '<NULL>') || ''' in the update scripts.';
        end if;

        if rFull.acl is distinct from rUpdate.acl then
            strError = strError || E'\n' || rFull.type || ' ' || rFull.name || 
                       ' acl changed from ''' || coalesce(rFull.acl, '<NULL>') || ''' in the full build to ''' || coalesce(rUpdate.acl, '<NULL>') || ''' in the update scripts.';
        end if;

        if rFull.meta is distinct from rUpdate.meta then
            strError = strError || E'\n' || rFull.type || ' ' || rFull.name || 
                       ' meta data changed from ''' || coalesce(rFull.meta, '<NULL>') || ''' in the full build to ''' || coalesce(rUpdate.meta, '<NULL>') || ''' in the update scripts.';
        end if;

        if rFull.meta_hash is distinct from rUpdate.meta_hash then
            strError = strError || E'\n' || rFull.type || ' ' || rFull.name || ' meta hash does between the full build and the update scripts.';
        end if;

        strError = strError || _build.build_info_validate(rFull.id, rUpdate.id);
    end loop;

    return strError;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_comment_set(strType text, strName text, strComment text) returns void as $$
begin
    if strComment is not null then
        execute 'comment on ' || strType || ' ' || strName || ' is ' || quote_literal(replace(replace(replace(trim(E'\n\r\t ' from strComment), E'\</br\>', ''), E'\{\{', ''), E'\}\}', ''));
        
/*        if strType = 'function' then
            raise warning '%', 'comment on ' || strType || ' ' || strName || ' is ' || quote_literal(replace(replace(replace(trim(E'\n\r\t ' from strComment), E'\</br\>', ''), E'\{\{', ''), E'\}\}', ''));
        end if;*/
    end if;
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_comment_out(strComment text, strCR text default E'\n') returns text as $$
begin
    return replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(trim(E'\n\r\t ' from strComment), E'\<br/\>\n', 'CrEOlWikI'), E'\n', ' '), E'\r\n', ' '), E'\<br/\>', strCR), E'\[', E'\\\['), E'\]', E'\\\]'), '|', E'\\|'), '_', E'\\_'), '-', E'\\-'), 'CrEOlWikI', strCR);
end;
$$ language plpgsql security definer;

create or replace function _build.build_info_document() returns text as $$
declare
    rSchema record;
    rTable record;
    rView record;
    rColumn record;
    rIndex record;
    rConstraint record;
    strDocument text = E'h1. Schemas';
    strPermissionRole text = '{@build.doc.role@}';
    bPermissionInclude boolean = @build.doc.include@;
    iTableTotal int;
    iViewTotal int;
    iFunctionTotal int;
    rFunction record;
    strFunctionReturnType text;
    strFunctionReturnDescription text;
    rParameter record;
begin
    for rSchema in
        select id,
               name,
               comment
          from _build.build_info
         where type = 'schema'
           and parent_id = (select id from _build.build_info where type = 'db' and parent_id is null and name = 'full')
           and name not like E'\\_%'
         order by id
    loop
        perform _build.build_info_comment_set('schema', rSchema.name, rSchema.comment);

        select count(*)
          into iTableTotal
          from _build.build_info
         where type = 'table'
           and parent_id = rSchema.id
           and _build.build_info_has_privilege(strPermissionRole::text[], 'table', rSchema.name || '.' || name, 'select', bPermissionInclude);
           
        select count(*)
          into iViewTotal
          from _build.build_info
         where type = 'view'
           and parent_id = rSchema.id
           and _build.build_info_has_privilege(strPermissionRole::text[], 'table', rSchema.name || '.' || name, 'select', bPermissionInclude);

        select count(*)
          into iFunctionTotal
          from _build.build_info
               inner join pg_proc
                    on pg_proc.oid = build_info.oid
               inner join pg_type
                    on pg_type.oid = pg_proc.prorettype
                   and pg_type.typname <> 'trigger'
         where type = 'function'
           and parent_id = rSchema.id
           and _build.build_info_has_privilege(strPermissionRole::text[], 'function', rSchema.name || '.' || name, 'execute', bPermissionInclude);

        if iTableTotal + iFunctionTotal > 0 then
            strDocument = strDocument || E'\n\nh2. ' || rSchema.name || ' schema';

            if rSchema.comment is not null then
                strDocument = strDocument || E'\n\n' || _build.build_info_comment_out(rSchema.comment);
            end if;

            if iTableTotal > 0 then
                strDocument = strDocument || E'\n\nh3. ' || rSchema.name || ' tables';

                for rTable in
                    select id,
                           name,
                           comment
                      from _build.build_info
                     where type = 'table'
                       and parent_id = rSchema.id
                       and _build.build_info_has_privilege(strPermissionRole::text[], 'table', rSchema.name || '.' || name, 'select', bPermissionInclude)
                     order by id
                loop
                    -- Set db comment
                    perform _build.build_info_comment_set('table', rSchema.name || '.' || rTable.name, rTable.comment);

                    -- Output the table name and comment
                    strDocument = strDocument || E'\n\nh4. ' || rSchema.name || '.' || rTable.name;

                    if rTable.comment is not null then
                        strDocument = strDocument || E'\n\n' || _build.build_info_comment_out(rTable.comment);
                    end if;

                    strDocument = strDocument || E'\n\n||name||meta||description||';
                    
                    -- Output the columns
                    for rColumn in
                        select id,
                               name,
                               meta,
                               comment
                          from _build.build_info
                         where type = 'column'
                           and parent_id = rTable.id
                         order by id
                    loop
                        -- Set db comment
                        perform _build.build_info_comment_set('column', rSchema.name || '.' || rTable.name || '.' || rColumn.name, rColumn.comment);

                        -- Output to wiki
                        strDocument = strDocument || E'\n|' || rColumn.name || '|{{' || 
                                      replace(replace(replace(replace(replace(coalesce(_build.build_info_comment_out(rColumn.meta, E' \\\\'), ''), ' ', '&nbsp;'), 'int8', 'bigint'), 'int4', 'int'), 'bool', 'boolean'), 'default', E'\\\\default') || '}}|' || 
                                      coalesce(_build.build_info_comment_out(rColumn.comment, E' \\\\ '), '') || '|';
                    end loop;

                    -- Output the primary key (if it exists)
                    if
                    (
                        select count(*) > 0
                          from _build.build_info
                         where type = 'constraint' and 
                          name like E'%\\_pk'
                           and parent_id = rTable.id
                    ) then
                        strDocument = strDocument || E'\n\n*Primary Key*: ';

                        for rConstraint in
                            select id,
                                   name,
                                   meta,
                                   comment
                              from _build.build_info
                             where type = 'constraint' 
                               and name like E'%\\_pk'
                               and parent_id = rTable.id
                        loop
                            -- Set db comment
        --                    perform _build.build_info_comment_set('index', rSchema.name || '.' || rIndex.name, rIndex.comment);

                            -- Output to wiki
                            strDocument = strDocument || E'{{' || _build.build_info_comment_out(replace(replace(rConstraint.meta, 'primary key (', ''), ')', '')) || '}}';
                        end loop;
                    end if;

                    -- Output the partition key (if it exists)
                    begin
                        if
                        (
                            select count(*) > 0
                              from _utility.partition_table
                             where schema_name = rSchema.name
                               and name = rTable.name
                        ) then
                            strDocument = strDocument || E'\n\n*Partition Key*: ';
                            strDocument = strDocument || E'{{';
                            
                            strDocument = strDocument ||
                            (
                                select array_to_string(array_agg(key), ', ')
                                  from
                                (
                                    select partition_type.key
                                      from _utility.partition_table
                                           inner join _utility.partition_type
                                                on partition_type.partition_table_id = partition_table.id
                                     where partition_table.schema_name = rSchema.name
                                       and partition_table.name = rTable.name
                                     order by partition_type.id
                                ) partition_column
                            );
                            
                            strDocument = strDocument || E'}}';
                        end if;
                    exception
                        when undefined_table then
                            null;
                    end;

                    -- Output the foreign keys (if there are any)
                    if
                    (
                        select count(*) > 0
                          from _build.build_info
                         where type = 'constraint'
                           and name like E'%\\_fk'
                           and name not like E'%\\_scd\\_%'
                           and parent_id = rTable.id
                    ) then
                        strDocument = strDocument || E'\n\n*Foreign Keys*:';

                        for rConstraint in
                            select id,
                                   name,
                                   meta,
                                   comment
                              from _build.build_info
                             where type = 'constraint'
                               and name like E'%\\_fk'
                               and name not like E'%\\_scd\\_%'
                               and parent_id = rTable.id
                             order by name
                        loop
                            -- Set db comment
        --                    perform _build.build_info_comment_set('constraint', rSchema.name || '.' || rIndex.name, rIndex.comment);

                            -- Output to wiki
                            strDocument = strDocument || E'\n* {{' || rConstraint.name || ' ' || _build.build_info_comment_out(rConstraint.meta) || '}}';
                        end loop;
                    end if;

                    -- Output the indexes (if there are any)
                    if
                    (
                        select count(*) > 0
                          from _build.build_info
                         where (type = 'index' or (type = 'constraint' and (name like E'%\\_pk' or name like E'%\\_unq')))
                           and parent_id = rTable.id
                    ) then
                        strDocument = strDocument || E'\n\n*Indexes*:';

                        for rIndex in
                            select id,
                                   name,
                                   meta,
                                   comment
                              from _build.build_info
                             where (type = 'index' or (type = 'constraint' and (name like E'%\\_pk' or name like E'%\\_unq')))
                               and parent_id = rTable.id
                             order by name
                        loop
                            -- Set db comment
                            perform _build.build_info_comment_set('index', rSchema.name || '.' || rIndex.name, rIndex.comment);

                            -- Output to wiki
                            strDocument = strDocument || E'\n* {{' || rIndex.name || ' ' || _build.build_info_comment_out(rIndex.meta) || '}}';
                        end loop;
                    end if;

                    -- Output the check constraints (if there are any)
                    if
                    (
                        select count(*) > 0
                          from _build.build_info
                         where type = 'constraint'
                           and name like E'%\\_ck'
                           and parent_id = rTable.id
                    ) then
                        strDocument = strDocument || E'\n\n*Check Constraints*:';

                        for rConstraint in
                            select id,
                                   name,
                                   meta,
                                   comment
                              from _build.build_info
                             where type = 'constraint'
                               and name like E'%\\_ck'
                               and parent_id = rTable.id
                             order by name
                        loop
                            -- Set db comment
        --                    perform _build.build_info_comment_set('constraint', rSchema.name || '.' || rIndex.name, rIndex.comment);

                            -- Output to wiki
                            strDocument = strDocument || E'\n* {{' || rConstraint.name || ' ' || _build.build_info_comment_out(rConstraint.meta) || '}}';
                        end loop;
                    end if;
                end loop;
            end if;

            -- Output views
            if iViewTotal > 0 then
                strDocument = strDocument || E'\n\nh3. ' || rSchema.name || ' views';

                for rView in
                    select id,
                           name,
                           comment
                      from _build.build_info
                     where type = 'view'
                       and parent_id = rSchema.id
                       and _build.build_info_has_privilege(strPermissionRole::text[], 'table', rSchema.name || '.' || name, 'select', bPermissionInclude)
                     order by id
                loop
                    -- Set db comment
                    perform _build.build_info_comment_set('view', rSchema.name || '.' || rView.name, rView.comment);

                    -- Output the view name and comment
                    strDocument = strDocument || E'\n\nh4. ' || rSchema.name || '.' || rView.name;

                    if rView.comment is not null then
                        strDocument = strDocument || E'\n\n' || _build.build_info_comment_out(rView.comment);
                    end if;

                    strDocument = strDocument || E'\n\n||name||meta||description||';
                    
                    -- Output the columns
                    for rColumn in
                        select id,
                               name,
                               meta,
                               comment
                          from _build.build_info
                         where type = 'column'
                           and parent_id = rView.id
                         order by id
                    loop
                        -- Set db comment
                        perform _build.build_info_comment_set('column', rSchema.name || '.' || rView.name || '.' || rColumn.name, rColumn.comment);

                        -- Output to wiki
                        strDocument = strDocument || E'\n|' || rColumn.name || '|{{' || 
                                      replace(replace(replace(replace(replace(coalesce(_build.build_info_comment_out(rColumn.meta, E' \\\\'), ''), ' ', '&nbsp;'), 'int8', 'bigint'), 'int4', 'int'), 'bool', 'boolean'), 'default', E'\\\\default') || '}}|' || 
                                      coalesce(_build.build_info_comment_out(rColumn.comment, E' \\\\ '), '') || '|';
                    end loop;
                end loop;
            end if;
            
            -- Output functions
            if iFunctionTotal > 0 then
                strDocument = strDocument || E'\n\nh3. ' || rSchema.name || ' functions';
                
                for rFunction in
                    select build_info.id,
                           build_info.oid,
                           build_info.name,
                           build_info.comment
                      from _build.build_info
                           inner join pg_proc
                                on pg_proc.oid = build_info.oid
                           inner join pg_type
                                on pg_type.oid = pg_proc.prorettype
                               and pg_type.typname <> 'trigger'
                     where build_info.type = 'function'
                       and build_info.parent_id = rSchema.id
                       and _build.build_info_has_privilege(strPermissionRole::text[], 'function', rSchema.name || '.' || build_info.name, 'execute', bPermissionInclude)
                     order by id
                loop
                    -- Set db comment
                    perform _build.build_info_comment_set('function', rSchema.name || '.' || rFunction.name, rFunction.comment);

                    -- Output the table name and comment
                    strDocument = strDocument || E'\n\nh4. ' || rSchema.name || '.' || trim(both E'\t ' from split_part(rFunction.name, '(', 1));

                    if rFunction.comment is not null then
                        strDocument = strDocument || E'\n\n' || _build.build_info_comment_out(rFunction.comment);
                    end if;

                    -- Get parameters
                    create temp table temp_buildinfodocument_parameter_parameter as
                    select coalesce(build_info_function_parameter.parameter_name, parameter_map.name) as name,
                           parameter_map.type,
                           build_info_function_parameter.comment
                      from 
                        (
                            select split_part(parameter, ' ', 1) as name,
                                   split_part(parameter, ' ', 2) as type
                              from
                                (
                                    select trim
                                    (
                                        both E'\t ' from unnest
                                        (
                                            regexp_split_to_array
                                            (
                                                (
                                                    regexp_split_to_array
                                                    (
                                                        _build.build_info_function_name(rFunction.oid, true),
                                                        E'\\(|\\)'
                                                    )
                                                )[2],
                                                E'\\,'
                                            )
                                        )
                                    ) as parameter
                                ) parameter_map
                        ) parameter_map
                           left outer join _build.build_info_function_parameter
                                on lower(build_info_function_parameter.parameter_name) = parameter_map.name
                               and build_info_function_parameter.schema_name = rSchema.name
                               and build_info_function_parameter.name = trim(both E'\t ' from split_part(rFunction.name, '(', 1));

                    -- Output parameters
                    if
                    (
                        select count(*) > 0
                          from temp_buildinfodocument_parameter_parameter
                    ) then
                        strDocument = strDocument || E'\n\n||parameter||type||description||';
                        
                        for rParameter in 
                            select *
                              from temp_buildinfodocument_parameter_parameter
                        loop
                            strDocument = strDocument || E'\n|' || rParameter.name || '|{{' || 
                                          coalesce(rParameter.type, '') || '}}|' || 
                                          coalesce(_build.build_info_comment_out(rParameter.comment, E' \\\\ '), '') || '|';
                        end loop;
                    end if;

                    drop table temp_buildinfodocument_parameter_parameter;

                    -- Get the return type
                    select case when pg_type.typname = 'void' then null else case when pg_proc.proretset then 'setof ' else '' end ||
                           case when pg_namespace.nspname like 'pg%' then '' else pg_namespace.nspname || '.' end ||
                           case pg_type.typcategory when 'A' then substr(pg_type.typname, 2) || '[]' else pg_type.typname end end
                      into strFunctionReturnType
                      from pg_proc
                           left outer join pg_type
                                on pg_type.oid = pg_proc.prorettype
                           inner join pg_namespace
                                on pg_namespace.oid = pg_type.typnamespace
                     where pg_proc.oid = rFunction.oid;
                    
                    if strFunctionReturnType is not null then
                        strFunctionReturnType = replace(strFunctionReturnType, 'bpchar', 'char');
                        strFunctionReturnType = replace(strFunctionReturnType, 'int8', 'bigint');
                        strFunctionReturnType = replace(strFunctionReturnType, 'int4', 'int');
                        strFunctionReturnType = replace(strFunctionReturnType, 'bool', 'boolean');
                    
                        select comment
                          into strFunctionReturnDescription
                          from _build.build_info_function_parameter
                         where build_info_function_parameter.parameter_name = '@return'
                           and build_info_function_parameter.schema_name = rSchema.name
                           and build_info_function_parameter.name = trim(both E'\t ' from split_part(rFunction.name, '(', 1));
                    
                        strDocument = strDocument || E'\n\n||return type||description||';
                        strDocument = strDocument || E'\n|' || _build.build_info_comment_out(strFunctionReturnType) || '|' || coalesce(_build.build_info_comment_out(strFunctionReturnDescription), '') || '|';
                    end if;
                end loop;
            end if;
        end if;
    end loop;

    return strDocument;
end;
$$ language plpgsql security definer;

do $$
begin
    perform _build.build_info_db_set('full');

    if '@build.type@' = 'update' then
        alter sequence _build.buildinfo_id_seq restart with 1000000;
    end if;
end $$;
