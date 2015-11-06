/***********************************************************************************************************************************
build-schema.sql

Common functions to be used in other parts of the build.
***********************************************************************************************************************************/
create schema _build;

/***********************************************************************************************************************************
Function to indicate a quick build
***********************************************************************************************************************************/
create or replace function _build.quick(strReason text) returns boolean as $$
begin
    if strReason is null or strReason = '' then
        raise exception 'You must give a reason for invoking quick mode';
    end if;

    if @build.quick@ = true then
        raise warning 'Quick mode has been invoked to avoid an expensive data only operation: %', strReason;
    end if;

    return @build.quick@;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Function to indicate a debug build
***********************************************************************************************************************************/
create or replace function _build.debug() returns boolean as $$
begin
    return @build.debug@;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Tables and functions to support object naming exceptions
***********************************************************************************************************************************/
create table _build.object_name_exception
(
    schema_name text not null,
    object_name text not null
);

create or replace function _build.object_name_exception(strSchemaName text, strObjectName text) returns void as $$
begin
    insert into _build.object_name_exception (schema_name, object_name) values (strSchemaName, strObjectName);
end;
$$ language plpgsql security definer;

create or replace function _build.object_name(oRelationId oid, strObjectName text, strType text, iyColumn int[], bRegExp boolean default true, bException boolean default true) returns text as $$
declare
     strName text;
     strSeparator text = '_';
begin
/*
PURPOSE: For objects whose name includes columns. For determining what the name of the object should be.
INPUTS:
    oRelationId - oid of the table object
    strObjectName - name of the object
    strType - type characters appended to the obbject name, if applicable. For example "fk", "ck", "idx", etc. 
    iyColumn - array of constrained column numbers
    bRegExp -
    bException - is there a naming exception?
*/

/*
    for rRecord in
        select indrelid as relation_id, 
               pg_class.relname as name
          from pg_namespace, pg_class, pg_index
         where pg_namespace.nspname = strSchema
           and pg_class.relnamespace = pg_namespace.oid
           and pg_class.relname = strName
           and pg_class.oid = pg_index.indexrelid 
            union
        select pg_constraint.conrelid as relation_id, 
               pg_constraint.conname as name
          from pg_namespace, pg_constraint
         where pg_namespace.nspname = strSchema
           and pg_constraint.connamespace = pg_namespace.oid
           and pg_constraint.conname = strName
    loop
        insert into _build.object_name_exception (relation_id, name) values (rRecord.relation_id, rRecord.name);
    end loop;
*/

    if bException then
        select object_name
          into strName
          from pg_constraint
               inner join pg_namespace
                    on pg_namespace.oid = pg_constraint.connamespace
               inner join _build.object_name_exception
                    on object_name_exception.schema_name = pg_namespace.nspname
                   and object_name_exception.object_name = pg_constraint.conname
                   and object_name_exception.object_name = strObjectName
         where pg_constraint.conrelid = oRelationId;
         
        if strName is not null then
            return strName;
        end if;
        
        select object_name
          into strName
          from pg_index
               inner join pg_class
                    on pg_class.oid = pg_index.indexrelid
               inner join pg_namespace
                    on pg_namespace.oid = pg_class.relnamespace
               inner join _build.object_name_exception
                    on object_name_exception.schema_name = pg_namespace.nspname
                   and object_name_exception.object_name = pg_class.relname
                   and object_name_exception.object_name = strObjectName
         where pg_index.indrelid = oRelationId;
         
        if strName is not null then
            return strName;
        end if;
    end if;

    select replace(relname, '_', '')
      into strName
      from pg_class 
     where oid = oRelationId;

    if bRegExp then
        strName = '^' || strName || E'\\_(scd\\_|workflow\\_|)';
        strSeparator = E'\\_';
    else
        strName = strName || strSeparator;
    end if;

    if strType not in ('pk', 'ck') then
        for iIndex in array_lower(iyColumn, 1) .. array_upper(iyColumn, 1) loop
            if iyColumn[iIndex] <> 0 then
                select strName || replace(attname, '_', '') || strSeparator
                  into strName
                  from pg_attribute
                 where attrelid = oRelationId
                   and attnum = iyColumn[iIndex];
            else
                strName = strName || 'function' || strSeparator;
            end if;
        end loop ;
    end if;

    if strType = 'ck' then
        strName = strName || E'.*' || strSeparator;
    end if;

    strName = strName || strType;

    if bRegExp then
        strName = strName || '$';
    end if;

    return strName;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Table and function to support foreign key exceptions
***********************************************************************************************************************************/
create table _build.foreign_key_exception
(
    schema_name text not null,
    foreign_key_name text not null,
    constraint foreignkeyexception_pk primary key (schema_name, foreign_key_name)
);

create or replace function _build.foreign_key_exception(strSchemaName text, strForeignKeyName text) returns void as $$
begin
    insert into _build.foreign_key_exception values (strSchemaName, strForeignKeyName);
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Tables and functions to support object owner exceptions
***********************************************************************************************************************************/
create table _build.object_owner_exception
(
    schema_name text not null,
    object_name text not null,
    owner text not null
);

create or replace function _build.object_owner_exception(strSchemaName text, strObjectName text, strOwner text) returns void as $$
begin
    insert into _build.object_owner_exception (schema_name, object_name, owner) values (strSchemaName, strObjectName, strOwner);
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Table and function to support trigger exceptions
***********************************************************************************************************************************/
create table _build.trigger_exception
(
    schema_name text not null,
    trigger_name text not null,
    constraint triggerexception_pk primary key (schema_name, trigger_name)
);

create or replace function _build.trigger_exception(strSchemaName text, strTriggerName text) returns void as $$
begin
    insert into _build.trigger_exception values (strSchemaName, strTriggerName);
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Table and sql block to retrieve the list of existing table and index relations
***********************************************************************************************************************************/
create table _build.schema_relation_existing
(
    schema_name text not null,
    relation_name text not null,
    constraint schemarelationexisting_pk primary key (schema_name, relation_name)
);

do $$
declare
    xSchema record;
    xRelation record;
begin
    for xSchema in
        select pg_namespace.oid,
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and nspname not in ('_build', 'research')
         order by name
    loop
        for xRelation in
            select relname as name,
                   relkind as type,
                   reltablespace as tablespace_oid
              from pg_class 
             where relkind in ('i', 'r')
               and relnamespace = xSchema.oid
             order by relkind, relname 
        loop
            insert into _build.schema_relation_existing values (xSchema.name, xRelation.name);
        end loop;
    end loop;
end $$;
