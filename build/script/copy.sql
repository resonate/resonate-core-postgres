/***********************************************************************************************************************************
copy.sql

Copy a database from a clean template.
**********************************************************************************************************************************/;
-- Drop all connections to the source db
do $$
declare
    xProcess record;
    strDbName text = '@db.instance_name.clean@';
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

-- Copy the database
create database @db.instance_name@ with template = @db.instance_name.clean@;
alter database @db.instance_name@ owner to @db.user@;

-- Connect to the database and create a copy record in the restore table
\connect @db.instance_name@;

do $$
begin
    insert into _dev.restore_audit_log (type, source, destination) values ('copy', '@db.instance_name.clean@', '@db.instance_name@');
exception
    when invalid_schema_name or undefined_table then
        null;
end $$;
