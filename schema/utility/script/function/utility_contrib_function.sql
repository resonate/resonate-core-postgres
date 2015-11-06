/***********************************************************************************************************************
* UTILITY Schema Contrib Functions
*
* These functions are from various contrib libraries that ship with postgres
***********************************************************************************************************************/

/***********************************************************************************************************************
* CONTRIB_CROSSTAB Functions
***********************************************************************************************************************/
create or replace function _utility.contrib_crosstab(strSql text) returns setof record 
as '$libdir/tablefunc', 'crosstab' 
language C stable strict;

do $$
begin 
    execute 'grant execute on function _utility.contrib_crosstab(text) to ' ||
            _utility.role_get('reader') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('user');
end $$;

create or replace function _utility.contrib_crosstab(strSql text, strColumnSql text) returns setof record 
as '$libdir/tablefunc', 'crosstab_hash' 
language C stable strict;

do $$
begin 
    execute 'grant execute on function _utility.contrib_crosstab(text, text) to ' ||
            _utility.role_get('reader') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('user');
end $$;

/***********************************************************************************************************************
* CONTRIB_CROSSTAB_GENERATE Function
***********************************************************************************************************************/
create or replace function _utility.contrib_crosstab_generate(strSql text, strColumnSql text, strColumnType text, strFixedColumns text, strRecord text) returns text as $$
declare
    strColumns text = '';
    rColumn record;
begin
    for rColumn in execute strColumnSql
    loop
        strColumns = strColumns || ', ' || rColumn.key || ' ' || strColumnType;
    end loop;

    return E'select * from _utility.contrib_crosstab\n' ||
           E'(\n' ||
           'E''' || replace(strSql, '''', '''''') || E''',\n' ||
           'E''' || replace(strColumnSql, '''', '''''') || E'''\n' ||
           E')\n' ||
           E'as ' || strRecord || ' (' || strFixedColumns || strColumns || ')';
end;
$$ language plpgsql;

do $$
begin 
    execute 'grant execute on function _utility.contrib_crosstab_generate(text, text, text, text, text) to ' ||
            _utility.role_get('reader') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('user');
end $$;

/***********************************************************************************************************************
* CONTRIB_HASH Functions
***********************************************************************************************************************/
create or replace function _utility.contrib_hash(text, text) returns bytea
as '$libdir/pgcrypto', 'pg_digest'
language c immutable strict;

do $$
begin 
    execute 'grant execute on function _utility.contrib_hash(text, text) to ' ||
            _utility.role_get('reader') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('user');
end $$;

create or replace function _utility.contrib_hash(bytea, text) returns bytea
as '$libdir/pgcrypto', 'pg_digest'
language c immutable strict;

do $$
begin 
    execute 'grant execute on function _utility.contrib_hash(bytea, text) to ' ||
            _utility.role_get('reader') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('user');
end $$;
