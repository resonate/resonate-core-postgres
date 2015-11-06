/***********************************************************************************************************************
* UTILITY Schema String Functions
*
* These functions implement some string processing.
***********************************************************************************************************************/
create or replace function _utility.string_schema_table_combine(strSchemaName text, strTableName text) returns text as $$
begin
    return strSchemaName || '.' || strTableName;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.string_schema_table_combine(text, text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.string_table_shorten(strTableName text) returns text as $$
begin
    return replace(strTableName, '_', '');
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.string_table_shorten(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.random_key_generate(iSize integer) returns text as $$
declare
    strLookup text;
    strTextID text;
begin
    strLookup = 'ABCDEFGHIJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';

    strTextID = substring(strLookup from ceil(random() * 49)::integer for 1);

    for i in 1..iSize - 1 loop
        strTextID = strTextID || substring(strLookup from ceil(random() * 57)::integer for 1);
    end loop;

    return(strTextID);
end;
$$ language plpgsql security definer;

create or replace function _utility.blank_is_null(strText text) returns text as $$
begin
    if strText = '' then
        return null;
    end if;

    return strText;
end;
$$ language plpgsql security definer immutable;
