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
