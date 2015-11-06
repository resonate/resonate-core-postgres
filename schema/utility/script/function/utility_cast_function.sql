/***********************************************************************************************************************
* UTILITY Schema Cast Functions
*
* These function cast text strings to values without throwing an error.
***********************************************************************************************************************/
create or replace function _utility.cast_timestamp(strValue text, tDefault timestamp) returns timestamp as $$
begin
    return strValue::timestamp;
exception
    when invalid_datetime_format then
        return tDefault;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.cast_timestamp(text, timestamp) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.cast_int(strValue text, iDefault int) returns int as $$
begin
    return strValue::int;
exception
    when invalid_text_representation or numeric_value_out_of_range then
        return iDefault;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.cast_int(text, int) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.cast_bigint(strValue text, lDefault bigint) returns bigint as $$
begin
    return strValue::bigint;
exception
    when invalid_text_representation or numeric_value_out_of_range then
        return lDefault;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.cast_bigint(text, bigint) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.cast_numeric(strValue text, nDefault numeric) returns numeric as $$
begin
    return strValue::numeric;
exception
    when invalid_text_representation or numeric_value_out_of_range then
        return nDefault;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.cast_numeric(text, numeric) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.cast_boolean(strValue text, bDefault boolean) returns boolean as $$
begin
    return strValue::boolean;
exception
    when invalid_text_representation then
        return bDefault;
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.cast_boolean(text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;