/***********************************************************************************************************************************
pre.sql

Setup the database for creating or updating.
NOTE: Do not put a semi-colon at the end of this comment as it will end up starting a transaction before we've had a chance
to twiddle AUTOCOMMIT in the right direction.
**********************************************************************************************************************************/
-- Do not output information messages
\set QUIET on

-- Specifiy whether to commit after each statement (generally set to off, but on for database create scripts).
\set AUTOCOMMIT on

-- Reset the role to the original logon role
reset role;

-- Only show warnings and errors
set client_min_messages='warning';

-- Stop on error
\set ON_ERROR_STOP on

-- Make sure that errors are detected and not automatically rolled back
\set ON_ERROR_ROLLBACK off

-- Turn off timing
\timing off

-- Set output format
\pset format unaligned
\pset tuples_only on

-- Set verbosity according to build settings
\set VERBOSITY default

-- Create a lock so copies are exclusive.
/*do $$
declare
    rLock record;
    iBuildLock int = 123456789;
    iQueueLock int = 800000000;
    iCopyLock int = 900000000;
    strBuildType text = 'drop';
    bLockAcquired boolean;
begin
    if pg_try_advisory_lock_shared(iBuildLock, iQueueLock) = false then
        raise exception 'Unable to acquire shared queue lock.  That shouldn''t happen';
    end if;

    if strBuildType = 'copy' then
        bLockAcquired = pg_try_advisory_lock(iBuildLock, iCopyLock);
    else
        bLockAcquired = pg_try_advisory_lock_shared(iBuildLock, iCopyLock);
    end if;

    if not bLockAcquired then
        create temp table temp_build_process
        (
            user_name text,
            db_name text
        );
        
        -- This exception is for 9.0-9.2 compatability.
        begin
            insert into temp_build_process
            select pg_stat_activity.usename as user_name,
                   pg_stat_activity.datname as db_name
              from pg_locks 
                   inner join pg_stat_activity 
                        on pg_stat_activity.procpid = pg_locks.pid
             where pg_locks.classid = iBuildLock
               and pg_locks.objid = iCopyLock;
        exception
            when undefined_column then
                insert into temp_build_process
                select pg_stat_activity.usename as user_name,
                       pg_stat_activity.datname as db_name
                  from pg_locks 
                       inner join pg_stat_activity 
                            on pg_stat_activity.pid = pg_locks.pid
                 where pg_locks.classid = iBuildLock
                   and pg_locks.objid = iCopyLock;
        end;
    
        for rLock in
            select user_name,
                   db_name
              from temp_build_process
        loop
            raise warning 'User % is performing a database %.', rLock.user_name, 
                            case when rLock.db_name = 'postgres' then 'copy' else 'build (' || rLock.db_name || ')' end;
            raise exception 'Builds are in progress.  Please try again later.';
        end loop;

        perform pg_advisory_unlock_shared(iBuildLock, iQueueLock);
    end if;
end $$;
*/
/***********************************************************************************************************************************
disconnect.sql

Drop all connections to the db.
**********************************************************************************************************************************/;
do $$
declare
    xProcess record;
    strDbName text = 'sample_dev';
begin
    create temp table temp_build_process
    (
        pid integer
    );
    
    -- This exception is for 9.0-9.2 compatability.
    begin
        insert into temp_build_process
        select procpid as pid
          from pg_stat_activity
         where datname = strDbName;
    exception
        when undefined_column then
            insert into temp_build_process
            select pid
              from pg_stat_activity
             where datname = strDbName;
    end;

    for xProcess in
        select pid
          from temp_build_process
    loop
        perform pg_terminate_backend(xProcess.pid);
    end loop;
    
    drop table temp_build_process;
end $$;
/***********************************************************************************************************************************
drop.sql

Drop a database and all associated roles.
**********************************************************************************************************************************/;
drop database if exists sample_dev;

do $$
declare
    xRole record;
    xTablespace record;
    strRoleName text = 'xx_sample';
begin
    if
    (
        select count(*) = 0
          from pg_roles, pg_database
         where pg_roles.rolname = strRoleName
           and pg_roles.oid = pg_database.datdba
    ) then
        for xRole in
            select rolname as name
              from pg_roles
             where rolname like strRoleName || '%'
        loop
            for xTablespace in
                select spcname as name
                  from pg_tablespace
            loop
                execute 'revoke all on tablespace ' || xTablespace.name || ' from ' || xRole.name;
            end loop;

            execute 'drop role ' || xRole.name;
        end loop;
    end if;
end $$;
