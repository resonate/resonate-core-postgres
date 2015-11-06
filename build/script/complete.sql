/***********************************************************************************************************************************
complete.sql

Drop the _build and _test schemas and then commit the transaction (or rollback if this is a test build)
**********************************************************************************************************************************/;
reset role;

do $$
begin
    if '@build.commit@' <> 'rollback' then
        -- Create database documentation
        perform _build.build_info_document();

        -- Assign tablespaces
        perform _utility.tablespace_move();

        -- Refresh SCD triggers
        perform _scd.refresh();

        -- Refresh the partitions (if the partition code exists)
        begin
            perform _utility.partition_all_refresh();
        exception
            when undefined_function then
                null;
        end;
        
        -- Process metrics and truncate raw tables
        perform _utility.metric_process();
    end if;
end $$;

-- Drop the build schema
drop schema _build cascade;
@build.commit@;

-- Allow connections to the db again (unless it is a clean instance)
update pg_database set datallowconn = @build.allow_connect@ where datname = '@db.instance_name@';
commit;

\connect postgres
