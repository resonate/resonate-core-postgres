/***********************************************************************************************************************
* UTILITY Process Functions
***********************************************************************************************************************/
set role postgres;

/***********************************************************************************************************************
* PROCESS_LIST Function
***********************************************************************************************************************/
create or replace function _utility.process_list()
    returns setof pg_stat_activity as $$
begin
    return query
    (
        select pg_stat_activity.*
          from pg_stat_activity
               inner join pg_roles
                    on pg_roles.rolname = session_user
         where pg_roles.rolsuper
            or pg_stat_activity.datname = current_database()
    );
end
$$ language plpgsql security definer;

do $$
begin
    if _utility.catalog_schema_exists('_build') then
        perform _build.object_owner_exception('_utility', 'process_list', 'postgres');
    end if;

    execute '
        grant execute
           on function _utility.process_list()
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;

/***********************************************************************************************************************
* PROCESS_TERMINATE Function
***********************************************************************************************************************/
create or replace function _utility.process_terminate
(
    iProcessId int
) 
    returns boolean as $$
begin
    if 
    (
        select count(*) = 0
          from pg_stat_activity
               inner join pg_roles
                    on rolname = session_user
         where pid = iProcessId
           and (pg_roles.rolsuper or
                datname = current_database())
    ) then
        raise exception 'Process % does not exist on %', iProcessId, current_database();
    end if;
    
    return pg_terminate_backend(iProcessId);
end
$$ language plpgsql security definer;

do $$
begin
    if _utility.catalog_schema_exists('_build') then
        perform _build.object_owner_exception('_utility', 'process_terminate', 'postgres');
    end if;

    execute '
        grant execute
           on function _utility.process_terminate(int)
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;

/***********************************************************************************************************************
* Reset the role to the current build role
***********************************************************************************************************************/
do $$
begin
    execute 'set role ' || _utility.role_get();
end $$; 
