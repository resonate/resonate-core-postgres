/***********************************************************************************************************************
* UTILITY Schema Lock Functions
*
* These functions aid in the creation of locks.
***********************************************************************************************************************/

/***********************************************************************************************************************
* LOCK Function
***********************************************************************************************************************/
create or replace function _utility.lock(strLockName text, bPersistent boolean default true) returns boolean as $$
declare
    strSql text;
begin
    strSql = 'create temporary table globallock_' || strLockName || ' (id int)';
    
    if not bPersistent then
        strSql = strSql || ' on commit drop';
    end if;

    execute strSql;
     
    return true;
exception
    when duplicate_table then
        return(false);
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.lock(text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.lock_exists(strLockName text) returns boolean as $$
begin
     execute 'create temporary table globallock_' || strLockName || ' (id int) on commit drop';
     execute 'drop table globallock_' || strLockName;
     
     return false;
exception
    when duplicate_table then
        return(true);
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.lock_exists(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;

create or replace function _utility.lock_release(strLockName text) returns boolean as $$
begin
     execute 'drop table globallock_' || strLockName;
     
     return true;
exception
    when undefined_table then
        return(false);
end;
$$ language plpgsql;

do $$
begin
    execute 'grant execute on function _utility.lock_release(text) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;