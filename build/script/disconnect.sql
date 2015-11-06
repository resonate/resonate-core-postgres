/***********************************************************************************************************************************
disconnect.sql

Drop all connections to the db.
**********************************************************************************************************************************/;
do $$
declare
    xProcess record;
    strDbName text = '@db.instance_name@';
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
