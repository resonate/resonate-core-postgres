/***********************************************************************************************************************************
pre.sql

Setup the database for creating or updating.
NOTE: Do not put a semi-colon at the end of this comment as it will end up starting a transaction before we've had a chance
to twiddle AUTOCOMMIT in the right direction.
**********************************************************************************************************************************/
-- Do not output information messages
\set QUIET on

-- Specifiy whether to commit after each statement (generally set to off, but on for database create scripts).
\set AUTOCOMMIT off

-- Reset the role to the original logon role
reset role;

-- Only show warnings and errors
set client_min_messages='warning';

-- Stop on error
\set ON_ERROR_STOP on

-- Make sure that errors are detected and not automatically rolled back
\set ON_ERROR_ROLLBACK off

-- Turn off timing
\timing off

-- Set output format
\pset format unaligned
\pset tuples_only on

-- Set verbosity according to build settings
\set VERBOSITY default

-- Create a lock so copies are exclusive.
/*do $$
declare
    rLock record;
    iBuildLock int = 123456789;
    iQueueLock int = 800000000;
    iCopyLock int = 900000000;
    strBuildType text = 'full';
    bLockAcquired boolean;
begin
    if pg_try_advisory_lock_shared(iBuildLock, iQueueLock) = false then
        raise exception 'Unable to acquire shared queue lock.  That shouldn''t happen';
    end if;

    if strBuildType = 'copy' then
        bLockAcquired = pg_try_advisory_lock(iBuildLock, iCopyLock);
    else
        bLockAcquired = pg_try_advisory_lock_shared(iBuildLock, iCopyLock);
    end if;

    if not bLockAcquired then
        create temp table temp_build_process
        (
            user_name text,
            db_name text
        );
        
        -- This exception is for 9.0-9.2 compatability.
        begin
            insert into temp_build_process
            select pg_stat_activity.usename as user_name,
                   pg_stat_activity.datname as db_name
              from pg_locks 
                   inner join pg_stat_activity 
                        on pg_stat_activity.procpid = pg_locks.pid
             where pg_locks.classid = iBuildLock
               and pg_locks.objid = iCopyLock;
        exception
            when undefined_column then
                insert into temp_build_process
                select pg_stat_activity.usename as user_name,
                       pg_stat_activity.datname as db_name
                  from pg_locks 
                       inner join pg_stat_activity 
                            on pg_stat_activity.pid = pg_locks.pid
                 where pg_locks.classid = iBuildLock
                   and pg_locks.objid = iCopyLock;
        end;
    
        for rLock in
            select user_name,
                   db_name
              from temp_build_process
        loop
            raise warning 'User % is performing a database %.', rLock.user_name, 
                            case when rLock.db_name = 'postgres' then 'copy' else 'build (' || rLock.db_name || ')' end;
            raise exception 'Builds are in progress.  Please try again later.';
        end loop;

        perform pg_advisory_unlock_shared(iBuildLock, iQueueLock);
    end if;
end $$;
*/
/***********************************************************************************************************************************
init.sql

Make sure the db cannot accept new connections while update is going on.
**********************************************************************************************************************************/;
-- Allow connections to the db
update pg_database set datallowconn = true where datname = 'sample_dev';
commit;

-- Connection to the db
\connect sample_dev;

-- Only show warnings and errors
set client_min_messages='warning';

-- Make sure that no connections are made while script is running
update pg_database set datallowconn = false where datname = 'sample_dev';
commit;

update pg_language set lanpltrusted = true where lanname = 'c';
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
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

    if false = true then
        raise warning 'Quick mode has been invoked to avoid an expensive data only operation: %', strReason;
    end if;

    return false;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
Function to indicate a debug build
***********************************************************************************************************************************/
create or replace function _build.debug() returns boolean as $$
begin
    return true;
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
/***********************************************************************************************************************************
test-schema.sql

Creates the unit test schema.
**********************************************************************************************************************************/;
create schema _test;

grant usage on schema _test to public;

create sequence _test.test_id_seq;

-- Table to track current unit test being run
create table _test.unit_test
(
    unit text,
    unit_begin timestamp without time zone,
    test text,
    test_begin timestamp without time zone
);

insert into _test.unit_test values (null, null, null, null);

-- Table to track all unit test results
create table _test.unit_test_result
(
    id int not null default nextval('_test.test_id_seq'),
    unit text not null,
    test text not null,
    test_time_ms int not null,
    result text not null
        constraint unittestresult_result_ck check (result in ('fail', 'pass')),
    description text 
        constraint unittestresult_description_ck check (description is null or result = 'fail' and description is not null),
    constraint unittestresult_pk primary key (id),
    constraint unittestresult_unit_test_unq unique (unit, test)
);

-- Function to begin the unit
create or replace function _test.unit_begin(strUnit text) returns void as $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Cannot begin unit "%" when unit "%" is already running', strUnit, strUnitCurrent;
    end if;
    
/*    set client_min_messages = 'notice';
    raise notice 'Test Unit Start: %', strUnit;
    set client_min_messages = 'warning';*/

    update _test.unit_test
       set unit = strUnit,
           unit_begin = clock_timestamp();
end;
$$ language plpgsql security definer;

-- Function to end the unit
create or replace function _test.unit_end() returns void as $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
    rResult record;
    bError boolean = false;
    strError text = E'\nErrors in ' || strUnitCurrent || ' Unit:';
begin
    if strUnitCurrent is null then
        raise exception 'Cannot end unit before it has begun';
    end if;

    update _test.unit_test
       set unit = null,
           unit_begin = null;
           
    for rResult in
        select rank() over (order by id) as rank,
               *
          from _test.unit_test_result
         where result = 'fail'
           and unit = strUnitCurrent
    loop
        strError = strError || E'\n' || rResult.rank || '. ' || rResult.test || ' - ' || coalesce(rResult.Description, '');        
        bError = true;
    end loop;
    
    if bError then
        raise exception '%', strError;
    end if;
end;
$$ language plpgsql security definer;

-- Function to begin the test
create or replace function _test.test_begin(strTest text) returns void as $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
    strTestCurrent text = (select test from _test.unit_test);
begin
    if strUnitCurrent is null then
        raise exception 'Cannot begin test "%" before a unit has begun', strTest;
    end if;

    if strTestCurrent is not null then
        raise exception 'Cannot begin unit test "%/%" when unit test "%/%" is already running', strUnitCurrent, strTest, strUnitCurrent, strTestCurrent;
    end if;

    update _test.unit_test
       set test = strTest,
           test_begin = clock_timestamp();
end;
$$ language plpgsql security definer;

-- Function to end the test
create or replace function _test.test_end(strResult text, strDescription text) returns void as $$
declare
    strTestCurrent text = (select test from _test.unit_test);
begin
    if strTestCurrent is null then
        raise exception 'Must begin a test before calling %', strResult;
    end if;

    update _test.unit_test
       set test = null,
           test_begin = null;

    insert into _test.unit_test_result (unit, test, test_time_ms, result, description)
                                values ((select unit from _test.unit_test),
                                        strTestCurrent,
                                        1, --extract(milliseconds from timestamp (clock_timestamp() - (select test_begin from _test.unit_test))),
                                        strResult,
                                        strDescription);
end;
$$ language plpgsql security definer;

-- Wrapper function to pass the test
create or replace function _test.test_pass() returns void as $$
begin
    perform _test.test_end('pass', null);
end;
$$ language plpgsql security definer;

-- Wrapper function to fail the test
create or replace function _test.test_fail(strDescription text) returns void as $$
begin
    perform _test.test_end('fail', strDescription);
end;
$$ language plpgsql security definer;

-- Wrapper function to assert that two bigints should be equal; if they aren't, fail the test
create or replace function _test.assert_equals(strDescription text, lExpected bigint, lActual bigint) returns void as $$
begin
    if (lActual = lExpected) 
    then
        perform _test.test_pass();
    elsif (lActual is null and lExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(lExpected::text, 'null') || ' got ' || coalesce(lActual::text, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(lExpected bigint, lActual bigint) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', lExpected, lActual);
end;
$$ language plpgsql security definer;

-- Like above, but for ints
create or replace function _test.assert_equals(strDescription text, iExpected int, iActual int) returns void as $$
begin
    if (iActual = iExpected) 
    then
        perform _test.test_pass();
    elsif (iActual is null and iExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(iExpected::text, 'null') || ' got ' || coalesce(iActual::text, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(iExpected int, iActual int) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', iExpected, iActual);
end;
$$ language plpgsql security definer;

-- Like above, but for booleans
create or replace function _test.assert_equals(strDescription text, bExpected boolean, bActual boolean) returns void as $$
begin
    if (bActual = bExpected) 
    then
        perform _test.test_pass();
    elsif (bActual is null and bExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(bExpected::text, 'null') || ' got ' || coalesce(bActual::text, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(bExpected boolean, bActual boolean) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', bExpected, bActual);
end;
$$ language plpgsql security definer;


-- Like above, but for text
create or replace function _test.assert_equals(strDescription text, strExpected text, strActual text) returns void as $$
begin
    if (strActual = strExpected) 
    then
        perform _test.test_pass();
    elsif (strActual is null and strExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(strExpected, 'null') || ' got ' || coalesce(strActual, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(strExpected text, strActual text) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', strExpected, strActual);
end;
$$ language plpgsql security definer;

-- Like above, but for date
create or replace function _test.assert_equals(strDescription text, dtExpected date, dtActual date) returns void as $$
begin
    if (dtActual = dtExpected) 
    then
        perform _test.test_pass();
    elsif (dtActual is null and dtExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(dtExpected::text, 'null') || ' got ' || coalesce(dtActual::text, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(dtExpected date, dtActual date) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', dtExpected, dtActual);
end;
$$ language plpgsql security definer;

-- Like above, but for numeric
create or replace function _test.assert_equals(strDescription text, nExpected numeric, nActual numeric) returns void as $$
begin
    if (nActual = nExpected) 
    then
        perform _test.test_pass();
    elsif (nActual is null and nExpected is null)
    then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected ' || coalesce(nExpected::text, 'null') || ' got ' || coalesce(nActual::text, 'null') || '');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(nExpected numeric, nActual numeric) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', nExpected, nActual);
end;
$$ language plpgsql security definer;


-- Like above, but for bigint arrays (order independent)
create or replace function _test.assert_equals(strDescription text, lyExpected bigint[], lyActual bigint[]) returns void as $$
begin
    if (lyExpected @> lyActual AND lyExpected <@ lyActual) then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected (' || array_to_string(lyExpected, ',') || ') got (' || array_to_string(lyActual, ',') || ')');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_equals(lyExpected bigint[], lyActual bigint[]) returns void as $$
begin
    perform _test.assert_equals('Assertion failed', lyExpected, lyActual);
end;
$$ language plpgsql security definer;

-- Wrapper function to assert that a bigint is not null
create or replace function _test.assert_not_null(strDescription text, lActual bigint) returns void as $$
begin
    if (lActual is not null) then
        perform _test.test_pass();
    else
        perform _test.test_fail(strDescription ||': expected a not-null value');
    end if;
end;
$$ language plpgsql security definer;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_not_null(lActual bigint) returns void as $$
begin
    perform _test.assert_not_null('Assertion failed', lActual);
end;
$$ language plpgsql security definer;

-- Function to assert that two SQL statements product the same records
create or replace function _test.assert_records_equal(strDescription text, strSQLExpected text, strSQLActual text) returns void as $$
declare
    iExtraExpectedRecords integer;
    iExtraActualRecords integer;
    r record;
    strErrorMessage text;
begin
    execute 'select count(*) from (' || strSQLExpected || ' except ' || strSQLActual || ') as differences' into iExtraExpectedRecords;
    execute 'select count(*) from (' || strSQLActual || ' except ' || strSQLExpected || ') as differences' into iExtraActualRecords;

    if (iExtraExpectedRecords > 0) then
        strErrorMessage = strDescription ||': there were ' || iExtraExpectedRecords || E' record(s) in the expected recordset that were not in the actual recordset.\n';

        strErrorMessage = strErrorMessage || E'Actual records:\n';
        for r in execute strSQLActual loop
            strErrorMessage = strErrorMessage || r || E'\n';
        end loop;
        
        perform _test.test_fail(strErrorMessage);

    elsif (iExtraActualRecords > 0) then
        strErrorMessage = strDescription ||': there were ' || iExtraActualRecords || E' record(s) in the actual recordset that were not in the expected recordset.\n';

        strErrorMessage = strErrorMessage || E'Actual records:\n';
        for r in execute strSQLActual loop
            strErrorMessage = strErrorMessage || r || E'\n';
        end loop;
        
        perform _test.test_fail(strErrorMessage);

    else
        perform _test.test_pass();
    end if;
end;
$$ language plpgsql security invoker;

-- Like the above, except without requiring strDescription
create or replace function _test.assert_records_equal(strSQLExpected text, strSQLActual text) returns void as $$
begin
    perform _test.assert_records_equal('Assertion failed', strSQLExpected, strSQLActual);
end;
$$ language plpgsql security invoker;

create or replace function _test.vendor_dictionary(strVendorkey text) returns text as $$
declare
  stryVendorDictionary text[][];
begin
   stryVendorDictionary = ARRAY[ ['doubleclick', 'Doubleclick'],['acxiom', 'Acxiom'], ['nielsen', 'Nielsen'], ['mediamath', 'Mediamath'], ['tubemogul','Tubemogul'], ['resonate','Resonate Networks'], ['comscore','Comscore'] ];

   for i in 1..array_upper(stryVendorDictionary, 1)
   loop
       if (stryVendorDictionary[i][1] = strVendorKey)
	   then
	       return stryVendorDictionary[i][2];
	   end if;
   end loop;
end $$ language plpgsql security invoker;

create or replace function _test.create_vendor(stryVendorKey text[]) returns void as $$
declare
begin
    perform _scd.transaction_create('ADHOC: Add all required vendors'); 
    for i in 1..array_upper(stryVendorKey, 1)
	loop
       insert into common.vendor(key, name) values (stryVendorKey[i], _test.vendor_dictionary(stryVendorKey[i]));
	end loop;
end $$ language plpgsql security invoker;

-- Grant permissions
grant execute on all functions in schema _test to public;
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
           and nspname not in ('_test', '_build', '_dev', 'public')
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
    strPermissionRole text = '{xx_sample}';
    bPermissionInclude boolean = true;
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

    if 'full' = 'update' then
        alter sequence _build.buildinfo_id_seq restart with 1000000;
    end if;
end $$;
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
/***********************************************************************************************************************************
UTILITY Pre
***********************************************************************************************************************************/
create schema _utility;
/***********************************************************************************************************************************
../schema/utility Build Scripts
***********************************************************************************************************************************/
\i ../schema/utility/script/function/utility_role_function.sql
\i ../schema/utility/script/function/utility_cast_function.sql
\i ../schema/utility/script/function/utility_string_function.sql
\i ../schema/utility/script/ddl/utility_trigger_type.sql
\i ../schema/utility/script/function/utility_trigger_function.sql
\i ../schema/utility/script/function/utility_analyze_function.sql
\i ../schema/utility/script/function/utility_contrib_function.sql
\i ../schema/utility/script/function/utility_denorm_function.sql
\i ../schema/utility/script/function/utility_lock_function.sql
\i ../schema/utility/script/ddl/utility_release_table.sql
\i ../schema/utility/script/function/utility_release_function.sql
\i ../schema/utility/script/ddl/utility_catalog_type.sql
\i ../schema/utility/script/function/utility_catalog_function.sql
\i ../schema/utility/script/function/utility_tablespace_function.sql
\i ../schema/utility/script/function/utility_eval_function.sql
\i ../schema/utility/script/function/utility_release_trigger_function.sql
\i ../schema/utility/script/ddl/utility_release_trigger.sql
\i ../schema/utility/script/function/utility_process_function.sql
\i ../schema/utility/script/ddl/utility_process_view.sql
\i ../schema/utility/script/ddl/utility_metric_table.sql
\i ../schema/utility/script/ddl/utility_metric_view.sql
\i ../schema/utility/script/function/utility_metric_function.sql
\i ../schema/utility/script/function/utility_array_function.sql
/***********************************************************************************************************************************
UTILITY Post
***********************************************************************************************************************************/
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('etl'); end $$;
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('reader'); end $$;
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('user'); end $$;
/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role xx_sample;

savepoint unit_test;

/***********************************************************************************************************************************
../schema/utility Unit Init Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-init-end.sql

Creates a savepoint so all init changes can be rolled back.
**********************************************************************************************************************************/;
savepoint unit_test_init;

/***********************************************************************************************************************************
../schema/utility Unit Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role xx_sample;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;

/***********************************************************************************************************************************
../schema/utility Seed Scripts
***********************************************************************************************************************************/

/***********************************************************************************************************************************
../schema/utility Exception Script
***********************************************************************************************************************************/
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
/***********************************************************************************************************************************
../schema/scd Build Scripts
***********************************************************************************************************************************/
\i ../schema/scd/script/ddl/scd_table.sql
\i ../schema/scd/script/function/scd_trigger_function.sql
\i ../schema/scd/script/ddl/scd_trigger.sql
\i ../schema/scd/script/function/scd_function.sql
/***********************************************************************************************************************************
SCD Post
***********************************************************************************************************************************/
do $$ 
begin
    if _utility.role_get() = 'rn_cookie' then
        -- Create the temp table for the shard config
        create temp table shard_config
        (
            key text not null,
            value text
        );

        reset role;
        copy shard_config from 'shard.conf' csv header;
        set role rn_cookie;

        -- Init the SCD table
        declare
            iShardKey int;
        begin
            select value::int
              into iShardKey
              from shard_config
             where key = 'shard';

            -- Init the scd before trying to insert the shard
            perform _scd.init(('4' || lpad(iShardKey::text, 2, '0') || '000000000000000')::bigint, ('4' || lpad(iShardKey::text, 2, '0') || '999999999999999')::bigint);
        end;
    else
        perform _scd.init(100000000000000000, 199999999999999999);
    end if;
end $$;
/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role xx_sample;

savepoint unit_test;

/***********************************************************************************************************************************
../schema/scd Unit Init Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-init-end.sql

Creates a savepoint so all init changes can be rolled back.
**********************************************************************************************************************************/;
savepoint unit_test_init;

/***********************************************************************************************************************************
../schema/scd Unit Test Scripts
***********************************************************************************************************************************/
\i ../schema/scd/test/metric.sql
/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role xx_sample;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;

/***********************************************************************************************************************************
../schema/scd Seed Scripts
***********************************************************************************************************************************/

/***********************************************************************************************************************************
../schema/scd Exception Script
***********************************************************************************************************************************/
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
/***********************************************************************************************************************************
../schema/partition Build Scripts
***********************************************************************************************************************************/
\i ../schema/partition/script/ddl/partition_table.sql
\i ../schema/partition/script/function/partition_view.sql
\i ../schema/partition/script/function/partition_function.sql
\i ../schema/partition/script/function/partition_trigger_function.sql
\i ../schema/partition/script/ddl/partition_trigger.sql
/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role xx_sample;

savepoint unit_test;

/***********************************************************************************************************************************
../schema/partition Unit Init Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-init-end.sql

Creates a savepoint so all init changes can be rolled back.
**********************************************************************************************************************************/;
savepoint unit_test_init;

/***********************************************************************************************************************************
../schema/partition Unit Test Scripts
***********************************************************************************************************************************/
\i ../schema/partition/test/partition.sql
\i ../schema/partition/test/partition_trigger.sql
/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role xx_sample;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;

/***********************************************************************************************************************************
../schema/partition Seed Scripts
***********************************************************************************************************************************/

/***********************************************************************************************************************************
../schema/partition Exception Script
***********************************************************************************************************************************/
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
/***********************************************************************************************************************************
../schema/workflow Build Scripts
***********************************************************************************************************************************/
\i ../schema/workflow/script/ddl/workflow_schema.sql
\i ../schema/workflow/script/ddl/workflow_type.sql
\i ../schema/workflow/script/ddl/workflow_table.sql
\i ../schema/workflow/script/function/workflow_function.sql
\i ../schema/workflow/script/function/workflow_trigger_function.sql
\i ../schema/workflow/script/ddl/workflow_trigger.sql
\i ../schema/workflow/script/function/workflow_api_function.sql
\i ../schema/workflow/script/ddl/workflow_view.sql
/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role xx_sample;

savepoint unit_test;

/***********************************************************************************************************************************
../schema/workflow Unit Init Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-init-end.sql

Creates a savepoint so all init changes can be rolled back.
**********************************************************************************************************************************/;
savepoint unit_test_init;

/***********************************************************************************************************************************
../schema/workflow Unit Test Scripts
***********************************************************************************************************************************/
\i ../schema/workflow/test/workflow.sql
/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role xx_sample;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;

/***********************************************************************************************************************************
../schema/workflow Seed Scripts
***********************************************************************************************************************************/

/***********************************************************************************************************************************
../schema/workflow Exception Script
***********************************************************************************************************************************/
/***********************************************************************************************************************************
Insert the transaction comment for a full build.
**********************************************************************************************************************************/;
do $$
begin
    perform _scd.transaction_create('release1 full build');
end $$;
/***********************************************************************************************************************************
reset.sql

Reset to the database owner before creating or updating each schema.
**********************************************************************************************************************************/;
reset role;
set role xx_sample;
/***********************************************************************************************************************************
../db/sample Build Scripts
***********************************************************************************************************************************/
\i ../db/sample/script/sample_table.sql
/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role xx_sample;

savepoint unit_test;

/***********************************************************************************************************************************
../db/sample Unit Init Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-init-end.sql

Creates a savepoint so all init changes can be rolled back.
**********************************************************************************************************************************/;
savepoint unit_test_init;

/***********************************************************************************************************************************
../db/sample Unit Test Scripts
***********************************************************************************************************************************/
/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role xx_sample;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;

/***********************************************************************************************************************************
../db/sample Seed Scripts
***********************************************************************************************************************************/
\i ../db/sample/seed/sample_seed.sql

/***********************************************************************************************************************************
../db/sample Exception Script
***********************************************************************************************************************************/
/***********************************************************************************************************************************
post.sql

Finalize the database update.
**********************************************************************************************************************************/;
-- Reset the role to the original logon role
reset role;

-- Make sure that all objects belong to the db owner
do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
    xObject record;
    iCount int = 0;
begin
    -- Temp table to hold the objects with invalid ownership
    create temp table temp_post_owner
    (
        ordering serial,
        type text,
        schema_name text,
        object_name text,
        owner text
    );
     
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspname not in ('public', '_dev')
           and pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        insert into temp_post_owner (type, schema_name, object_name, owner)
        select type,
               schema_name,
               object_name,
               owner
          from
            (
                select 'class' as type,
                       xSchema.name as schema_name,
                       relname as object_name,
                       pg_roles.rolname as owner
                  from pg_class
                       inner join pg_roles
                            on pg_roles.oid = pg_class.relowner
                           and pg_roles.rolname <> strOwnerName
                 where relnamespace = xSchema.oid
                    union
                select 'function' as types,
                       xSchema.name as schema_name,
                       proname as object_name,
                       pg_roles.rolname as owner
                  from pg_proc
                       inner join pg_roles
                            on pg_roles.oid = pg_proc.proowner
                           and pg_roles.rolname <> strOwnerName
                 where pronamespace = xSchema.oid
                    union
                select 'type' as type,
                       xSchema.name as schema_name,
                       typname as object_name,
                       pg_roles.rolname as owner
                  from pg_type
                       inner join pg_roles
                            on pg_roles.oid = pg_type.typowner
                           and pg_roles.rolname <> strOwnerName
                 where typnamespace = xSchema.oid
                   and typname not like E'\\_%'
            ) object
         where not exists
        (
            select 1
              from _build.object_owner_exception
             where object_owner_exception.schema_name = object.schema_name
               and object_owner_exception.object_name = object.object_name
               and object_owner_exception.owner = object.owner
        )
         order by schema_name,
                  object_name,
                  type;
    end loop;

    for xObject in
            select *
              from temp_post_owner
             order by ordering
        loop
            raise warning '% %.% is owned by % instead of %', Initcap(xObject.type), xSchema.name, xObject.object_name,
                                                              xObject.owner, strOwnerName;
            iCount = iCount + 1;
        end loop;

    if iCount <> 0 then
        raise exception 'Some objects do not have correct ownership';
    end if;
end $$;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
do $$
declare
    xSchema record;
    strOwnerName text = 'xx_sample';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

-- Make sure all names follow the standard
do $$
declare
    xSchema record;
    xObject record;
    iCount int = 0;
begin
    for xSchema in
        select pg_namespace.oid,
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and pg_namespace.nspname not in ('public', '_dev')
           and pg_namespace.nspname not like '%_partition'
         order by name
    loop
        for xObject in
            select 'Index' as label,
                   pg_class.relname as name,
                   indrelid as table_oid,
                   pg_table.relname as table_name,
                   case indisunique 
                       when true then 'unq'
                       else 'idx'
                   end as type,
                   indkey::int[] as columns
              from pg_class, pg_index, pg_class pg_table
             where pg_class.relnamespace = xSchema.oid
               and pg_class.oid = pg_index.indexrelid 
               and pg_index.indrelid = pg_table.oid
               and not exists
             (
                 select conrelid
                   from pg_constraint
                  where pg_constraint.connamespace = xSchema.oid
                    and pg_constraint.conname = pg_class.relname
             )
                union
            select 'Constraint' as label,
                   pg_constraint.conname as name, 
                   pg_constraint.conrelid as table_oid,
                   pg_class.relname as table_name,
                   case contype
                       when 'p' then 'pk'
                       when 'u' then 'unq'
                       when 'f' then 'fk'
                       when 'c' then 'ck'
                       else 'err'
                   end as type,
                   pg_constraint.conkey::int[] as columns
              from pg_constraint, pg_class
             where pg_constraint.connamespace = xSchema.oid
               and pg_constraint.conrelid = pg_class.oid
               and pg_constraint.contype <> 't'
            order by table_name, label, name
        loop
           if (xObject.name !~ _build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns)) or
              (_build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns) is null) then
               raise warning '% "%" on table "%.%" (oid %) should be named "%"', xObject.label, xObject.name, xSchema.name, xObject.table_name, xObject.table_oid, _build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns);
               iCount = iCount + 1;
           end if;
        end loop;                
        
        -- for each trigger in the current schema (that does not begin with _) that is not a postgres internal trigger and is not an _scd,_workflow or _partition trigger
        -- check if there is an exception on the name else error if the name is not in the correct format
        for xObject in
            select tgname as name,
                   pg_class.oid as table_oid,
                   pg_class.relname as table_name,
                   lower(replace(pg_class.relname, '_', '')) as table_abbr
              from pg_trigger
                   inner join pg_class
                        on pg_class.oid = pg_trigger.tgrelid
                       and pg_class.relnamespace = xSchema.oid
             where pg_trigger.tgisinternal = false
               and tgname !~ ('^' || lower(replace(pg_class.relname, '_', '')) || '_(scd|workflow|partition)_trigger_.*')
               and xSchema.name !~ E'^_'
               and not exists
                (
                    select trigger_name
                      from _build.trigger_exception
                     where schema_name = xSchema.name
                       and trigger_name = pg_trigger.tgname
                )
            order by table_name, name
        loop
           if xObject.name !~ (xObject.table_abbr || '_[0-9]{2}_trigger.*') then
               raise warning 'Trigger "%" on table "%.%" (oid %) should begin with "%"', xObject.name, xSchema.name, xObject.table_name, xObject.table_oid, xObject.table_abbr || '_[0-9]{2}_trigger_';
               iCount = iCount + 1;
           end if;
        end loop;                
    end loop;

    if iCount > 0 then
        raise exception 'Object naming errors were detected';
    end if;
end $$;

-- Make sure all foreign keys have supporting indexes
do $$
declare
    xSchema record;
    xTable record;
    xForeignKey record;
    xIndex record;
    iCount int = 0;
    bFound boolean;
begin
    for xSchema in
        select pg_namespace.oid,
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and pg_namespace.nspname not in ('public')
           and pg_namespace.nspname not like '%_partition'
         order by name
    loop
        for xTable in 
            select oid,
                   relname as name
              from pg_class
             where relnamespace = xSchema.oid
             order by relname
        loop
            for xForeignKey in 
                select _build.object_name(xTable.oid, pg_constraint.conname, '', pg_constraint.conkey::int[], false, false) as name
                  from pg_constraint
                 where pg_constraint.conrelid = xTable.oid
                   and pg_constraint.contype = 'f'
                   and not exists
                (
                    select foreign_key_name
                      from _build.foreign_key_exception
                     where schema_name = xSchema.name
                       and foreign_key_name = _build.object_name(xTable.oid, pg_constraint.conname, 'fk', pg_constraint.conkey::int[], false, true)
                )
                order by name
            loop
                bFound = false;

                for xIndex in
                    select _build.object_name(xTable.oid, pg_class.relname, 'idx', pg_index.indkey::int[], false, false) as name
                  from pg_index, pg_class
                 where pg_index.indrelid = xTable.oid
                   and pg_index.indexrelid = pg_class.oid
                   and pg_index.indpred is null
                loop
                    if strpos(xIndex.name, xForeignKey.name) = 1 then
                        bFound = true;
                    end if;
                end loop;

                if not bFound then
                    raise warning 'Foreign key %.%fk has no supporting index', xSchema.name, xForeignKey.name;
                    iCount = iCount + 1;
                end if;
            end loop;
        end loop;
    end loop;

    if iCount > 0 then
        raise exception '% unsupported foreign keys were found', iCount;
    end if;
end $$;
/***********************************************************************************************************************************
release-set.sql

Set the new database version.
**********************************************************************************************************************************/;
do $$
declare
    strName text = _utility.release_split('release1', 'name');
    iPatch int = _utility.release_split('release1', 'patch');
    tsTimestamp timestamp = current_timestamp;
    strBuildUser text = current_user;
begin
    if
    (
        select count(*) = 0
          from _utility.release
    ) then
        insert into _utility.release (name, patch, build_user, timestamp) 
                              values (strName, iPatch, strBuildUser, tsTimestamp);
    else
        update _utility.release
           set name = strName,
               patch = iPatch,
               build_user = strBuildUser,
               timestamp = tsTimestamp;
    end if;    
end $$;

-- Output the version to the console
select case when 'full' = 'full' and '${build.update}' = 'y' then null else 'Database ' || current_database() || ' has been successfully ' || 
       case when 'full' = 'update' then 'updated to' else 'built at' end || ' release ' || _utility.release_get() end;
/***********************************************************************************************************************************
test-drop.sql

Drop the test schema.
**********************************************************************************************************************************/;
drop schema _test cascade;
/***********************************************************************************************************************************
complete.sql

Drop the _build and _test schemas and then commit the transaction (or rollback if this is a test build)
**********************************************************************************************************************************/;
reset role;

do $$
begin
    if 'commit' <> 'rollback' then
        -- Create database documentation
        perform _build.build_info_document();

        -- Assign tablespaces
        perform _utility.tablespace_move();

        -- Refresh SCD triggers
        perform _scd.refresh();

        -- Refresh the partitions (if the partition code exists)
        begin
            perform _utility.partition_all_refresh();
        exception
            when undefined_function then
                null;
        end;
        
        -- Process metrics and truncate raw tables
        perform _utility.metric_process();
    end if;
end $$;

-- Drop the build schema
drop schema _build cascade;
commit;

-- Allow connections to the db again (unless it is a clean instance)
update pg_database set datallowconn = true where datname = 'sample_dev';
commit;

\connect postgres
