/***********************************************************************************************************************
* UTILITY Schema Analyze Functions
*
* These functions perform db, schema, or table level analysis and should be run after a big data load.
***********************************************************************************************************************/
create or replace function _utility.analyze_table(strSchemaName text, strTableName text) returns void as $$
begin
    execute 'analyze ' || strSchemaName || '.' || strTableName;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.analyze_table(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.analyze_schema(strSchemaName text) returns void as $$
declare
    xTable record;
begin
    for xTable in
        select tablename
          from pg_tables
         where schemaname = strSchemaName
         order by tablename
    loop
        perform _utility.analyze_table(strSchemaName, xTable.tablename);
    end loop;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.analyze_schema(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.analyze_db() returns void as $$
begin
    execute 'analyze';
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.analyze_db() to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;