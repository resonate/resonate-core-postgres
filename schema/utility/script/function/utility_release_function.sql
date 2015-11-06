/***********************************************************************************************************************
* UTILITY Version Functions
***********************************************************************************************************************/
create or replace function _utility.release_get(strName text, iPatch int) returns text as $$
begin
     return (strName || coalesce('.' || iPatch, ''));
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.release_get(text, int) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.release_get() returns text as $$
declare
    strName text;
    iPatch int;
begin
    select name,
           patch
      into strName,
           iPatch
      from _utility.release;

     return (_utility.release_get(strName, iPatch));
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.release_get() to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.release_split(strRelease text, strPart text) returns text as $$
begin
    if strPart = 'name' then
        return split_part(strRelease, '.', 1)::text;
    elsif strPart = 'patch' then
        return case when split_part(strRelease, '.', 2) = '' then null else split_part(strRelease, '.', 2) end::int::text;
    end if;

    raise exception 'Invalid part ''%'' specified for release ''%''', strPart, strRelease;
exception
    when invalid_text_representation or numeric_value_out_of_range then
        raise exception 'Invalid integer in part ''%'' for release ''%''', strPart, strRelease;
    when others then
        raise exception 'Unable to find part ''%'' for release ''%''', strPart, strRelease;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.release_split(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;
