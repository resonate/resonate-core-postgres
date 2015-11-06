/***********************************************************************************************************************
* UTILITY Process Views
***********************************************************************************************************************/
set role postgres;

/***********************************************************************************************************************
* VW_PROCESS_LOCK View
***********************************************************************************************************************/
create or replace view _utility.vw_process_lock as
select pg_locks.*
  from pg_locks
       left outer join pg_database
            on pg_database.oid = pg_locks.database
       inner join pg_roles
            on pg_roles.rolname = session_user
 where pg_locks.pid <> pg_backend_pid()
   and (pg_roles.rolsuper or
        pg_database.datname = current_database());

do $$
begin
    if _utility.catalog_schema_exists('_build') then
        perform _build.object_owner_exception('_utility', 'vw_process_lock', 'postgres');
    end if;

    execute '
        grant select
           on _utility.vw_process_lock
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;

/***********************************************************************************************************************
* VW_PROCESS_BLOCK View
***********************************************************************************************************************/
create or replace view _utility.vw_process_block as
select blocking.pid as blocking_pid,
       blocking.mode as blocking_mode,
       blocked.pid as blocked_pid,
       blocked.mode as blocked_mode,
       blocked.locktype,
       blocked.database,
       blocked.relation,
       blocked.page,
       blocked.tuple,
       blocked.virtualxid,
       blocked.transactionid,
       blocked.classid,
       blocked.objid,
       blocked.objsubid
  from pg_catalog.pg_locks blocking
       inner join _utility.vw_process_lock blocked
            on blocked.locktype = blocking.locktype
           and blocked.database is not distinct from blocking.database
           and blocked.relation is not distinct from blocking.relation
           and blocked.page is not distinct from blocking.page
           and blocked.tuple is not distinct from blocking.tuple
           and blocked.virtualxid is not distinct from blocking.virtualxid
           and blocked.transactionid is not distinct from blocking.transactionid
           and blocked.classid is not distinct from blocking.classid
           and blocked.objid is not distinct from blocking.objid
           and blocked.objsubid is not distinct from blocking.objsubid
           and blocked.pid <> blocking.pid
 where blocking.granted and not blocked.granted
 order by blocking.pid,
          blocked.pid;
          
do $$
begin
    if _utility.catalog_schema_exists('_build') then
        perform _build.object_owner_exception('_utility', 'vw_process_block', 'postgres');
    end if;

    execute '
        grant select
           on _utility.vw_process_block
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;

/***********************************************************************************************************************
* VW_PROCESS_FULL View
***********************************************************************************************************************/
create or replace view _utility.vw_process_full as
select pg_stat_activity.datname,
       pg_stat_activity.pid,
       pg_stat_activity.usename,
       pg_stat_activity.application_name,
       pg_stat_activity.client_addr,
       pg_stat_activity.backend_start,
       pg_stat_activity.xact_start,
       pg_stat_activity.query_start,
       pg_stat_activity.state_change,
       pg_stat_activity.waiting,
       pg_stat_activity.state,
       pg_stat_activity.query,
       vw_process_block.blocking_pid,
       pg_stat_activity_blocking.usename as blocking_usename,
       pg_stat_activity_blocking.application_name as blocking_application_name,
       pg_stat_activity_blocking.client_addr as blocking_client_addr,
       pg_stat_activity_blocking.backend_start as blocking_backend_start,
       pg_stat_activity_blocking.xact_start as blocking_xact_start,
       pg_stat_activity_blocking.query_start as blocking_query_start,
       pg_stat_activity_blocking.state_change as blocking_state_change,
       pg_stat_activity_blocking.waiting as blocking_waiting,
       pg_stat_activity_blocking.state as blocking_state,
       pg_stat_activity_blocking.query as blocking_query,
       vw_process_block.blocking_mode,
       vw_process_block.blocked_mode,
       pg_namespace.nspname as blocking_relation_schema,
       pg_class.relname as blocking_relation_name
  from _utility.process_list() as pg_stat_activity
       left outer join _utility.vw_process_block
            on vw_process_block.blocked_pid = pg_stat_activity.pid
       left outer join pg_stat_activity as pg_stat_activity_blocking
            on pg_stat_activity_blocking.pid = vw_process_block.blocking_pid
       left outer join pg_class
            on pg_class.oid = vw_process_block.relation
       left outer join pg_namespace
            on pg_namespace.oid = pg_class.relnamespace;

do $$
begin
    if _utility.catalog_schema_exists('_build') then
        perform _build.object_owner_exception('_utility', 'vw_process_full', 'postgres');
    end if;

    execute '
        grant select
           on _utility.vw_process_full
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;

/***********************************************************************************************************************
* Reset the role to the current build role
***********************************************************************************************************************/
do $$
begin
    execute 'set role ' || _utility.role_get();
end $$; 

/***********************************************************************************************************************
* VW_PROCESS View
***********************************************************************************************************************/
create or replace view _utility.vw_process as
select datname,
       pid,
       usename,
       query_start,
       waiting,
       blocking_pid,
       state,
       query
  from _utility.vw_process_full;

do $$
begin
    execute '
        grant select
           on _utility.vw_process
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get();
end $$;
