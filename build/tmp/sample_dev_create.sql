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
    strBuildType text = 'create';
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
create.sql

Create a database and all standard roles.
**********************************************************************************************************************************/;

/***********************************************************************************************************************************
Create the database roles and grant permissions on all tablespaces.
**********************************************************************************************************************************/;
do $$
declare
    xTablespace record;
    strRoleName text = 'xx_sample';
    strDbName text = 'sample_dev';
begin
    if
    (
        select count(*) = 0 
          from pg_roles 
         where rolname = strRoleName
    ) then
        execute 'create role ' || strRoleName || ' noinherit createrole';
        
        execute 'create role ' || strRoleName || '_reader';
        execute 'create role ' || strRoleName || '_user';
        execute 'create role ' || strRoleName || '_admin';
        execute 'create user ' || strRoleName || '_etl with password ''' || strRoleName || '_etl''';

        execute 'grant ' || strRoleName || '_reader to ' || strRoleName || '_user';
        execute 'grant ' || strRoleName || '_user to ' || strRoleName || '_admin';
        execute 'grant ' || strRoleName || '_user to ' || strRoleName || '_etl';

        for xTablespace in
            select spcname as name
              from pg_tablespace
        loop
            execute 'grant create on tablespace ' || xTablespace.name || ' to ' || strRoleName;
        end loop;
    end if;
end $$;

/***********************************************************************************************************************************
Create the database and connect to it.
**********************************************************************************************************************************/;
create database sample_dev with owner xx_sample encoding = 'UTF8' tablespace = pg_default;
revoke all on database sample_dev from public;
\connect sample_dev
update pg_database set datallowconn = false where datname = 'sample_dev';

/***********************************************************************************************************************************
Drop the default public schema.
**********************************************************************************************************************************/;
drop schema public;

/***********************************************************************************************************************************
Make C a trusted language so contrib functions can be added (only the db owner can create functions so this is safe).
**********************************************************************************************************************************/;
update pg_language set lanpltrusted = true where lanname = 'c'; 

/***********************************************************************************************************************************
Allow the reader role to connect
**********************************************************************************************************************************/;
do $$
declare
    strRoleName text = 'xx_sample';
    strDbName text = 'sample_dev';
begin
    execute 'grant connect on database ' || strDbName || ' to ' || strRoleName || '_reader';
end $$;
