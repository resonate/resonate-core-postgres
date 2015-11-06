/***********************************************************************************************************************
* UTILITY Role Functions
***********************************************************************************************************************/
create or replace function _utility.role_get(strName text) returns text as $$
begin
    return
    (
        select pg_roles.rolname || case strName when '' then '' else '_' || strName end
          from pg_database, pg_roles
         where pg_database.datname = current_database()
           and pg_database.datdba = pg_roles.oid
    );
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.role_get(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl') || ', ' || _utility.role_get('admin');
end $$;

create or replace function _utility.role_get() returns text as $$
begin
    return _utility.role_get('');
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.role_get() to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.role_get_all() returns setof text as $$
declare
    rRole record;
begin    
    for rRole in 
        select roles.rolname as name
          from pg_database, pg_roles role_owner, pg_roles roles
         where pg_database.datname = current_database()
           and pg_database.datdba = role_owner.oid
           and roles.rolname like role_owner.rolname || '%'
    loop
        return next rRole.name;
    end loop;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.role_get_all() to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;