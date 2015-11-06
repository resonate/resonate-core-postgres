/***********************************************************************************************************************************
* UTILITY Metric Functions
**********************************************************************************************************************************/;

/***********************************************************************************************************************************
* METRIC_BEGIN Function
**********************************************************************************************************************************/;
create or replace function _utility.metric_begin
(
    strSchemaName text,
    strFunctionName text,
    strParameter text[][] default null,
    lJournalTransactionId bigint default null -- Should ONLY EVER be set by _scd.transaction_create
)
    returns bigint as $$
declare
    tsTimestampBegin timestamp with time zone = clock_timestamp();
    lMetricId bigint;
    lyMetricId bigint[];
    lRootId bigint;
    lProcessId bigint;
    lTransactionId bigint;
    lJournalTransactionPriorId bigint;
    lQueryId bigint;
    tsQueryKey timestamp with time zone;
    lParentId bigint;
    iDepth int;
begin
    -- If running on a hot standby, return without recording metrics
    if current_setting('transaction_read_only') = 'on' then
        return null;
    end if;

    -- Assign a unique id to the metric record
    lMetricId = nextval('_scd.object_id_seq');

    -- Make sure the schema and function exists (skip this check on prod instances)
    if strpos(current_database(), '_') <> -1 then
        if
        (
            select count(*) = 0
              from pg_namespace
                   inner join pg_proc
                        on pg_proc.pronamespace = pg_namespace.oid
                       and pg_proc.proname = strFunctionName
             where nspname = strSchemaName
        ) then
            raise exception 'Schema "%" or function "%" does not exist', strSchemaName, strFunctionName;
        end if;
    end if;

    -- Create the process temp table if it does not already exist
    begin
        -- Create the temp table
        create temp table _utility_temp_metric_process
        (
            metric_process_id bigint
        );

        -- Get initial sequences
        lProcessId = nextval('_scd.object_id_seq');

        -- Initialize the process temp table
        insert into _utility_temp_metric_process (metric_process_id)
                                          values (lProcessId);

        -- Insert the process row
        insert into _utility.metric_raw_process (id, user_name, application_name, client_address,
                                                 client_hostname, timestamp_begin)
        select lProcessId as process_id,
               usename as user_name,
               application_name,
               client_addr as client_address,
               client_hostname,
               backend_start
          from _utility.process_list()
         where pid = pg_backend_pid();
    exception
        -- If the process temp table already exists then continue
        when duplicate_table then
            null;
    end;

    -- Create the transaction temp table if it does not already exist
    begin
        -- Create the temp table
        create temp table _utility_temp_metric_transaction
        (
            metric_process_id bigint,
            metric_transaction_id bigint,
            metric_query_id bigint,
            metric_query_key timestamp with time zone,
            metric_root_id bigint,
            metric_id bigint[]
        ) on commit drop;

        -- Get initial sequences
        lTransactionId = nextval('_scd.object_id_seq');
        lQueryId = nextval('_scd.object_id_seq');

        -- Get the process id if it is not already set
        if lProcessId is null then
            select metric_process_id
              into lProcessId
              from _utility_temp_metric_process;
        end if;

        -- Start a block to declare local variables
        declare
            strUserName text;
            strApplicationName text;
            strClientAddress text;
            strClientHostname text;
            tsTransactionBegin timestamp with time zone;
            tsQueryBegin timestamp with time zone;
        begin
            --Get information about the process, transaction and query
            select usename,
                   application_name,
                   client_addr,
                   client_hostname,
                   xact_start,
                   query_start
              into strUserName,
                   strApplicationName,
                   strClientAddress,
                   strClientHostname,
                   tsTransactionBegin,
                   tsQueryBegin
              from _utility.process_list()
             where pid = pg_backend_pid();

            -- If this is not a journal transaction, initialize the function stack
            if lJournalTransactionId is null then
                lRootId = lMetricId;
                lyMetricId[1] = lMetricId;
                iDepth = 0;
            else
                tsQueryBegin = null;
            end if;

            -- Initialize the metric temp table
            insert into _utility_temp_metric_transaction (metric_process_id, metric_transaction_id, metric_query_id,
                                                          metric_query_key, metric_root_id, metric_id)
                                                  values (lProcessId, lTransactionId, lQueryId, tsQueryBegin, lRootId,
                                                          lyMetricId);

            -- Insert the transaction row
            insert into _utility.metric_raw_transaction (id, metric_process_id, timestamp_begin)
                                                 values (lTransactionId, lProcessId, tsTransactionBegin);

            -- Determine whether the journal transaction id is set
            if lJournalTransactionId is null then
                -- Insert the query row
                insert into _utility.metric_raw_query (id, metric_transaction_id, timestamp_begin, sql)
                                               values (lQueryId, lTransactionId, tsQueryBegin, current_query());
            else
                -- Insert the transaction map row.  This associates the current journal transaction with the
                -- current metric transaction, tying these two hierarchies together.
                insert into _utility.metric_raw_transaction_journal_map (metric_transaction_id,
                                                                         journal_transaction_id)
                                                                 values (lTransactionId, lJournalTransactionId);

                -- Return here because there are no function metrics to store
                return null;
            end if;
         end;
    exception
        -- If the process temp table already exists
        when duplicate_table then
            -- Get the process_id and root_id
            select metric_process_id,
                   metric_transaction_id,
                   metric_query_id,
                   metric_query_key,
                   metric_root_id,
                   metric_id
              into lProcessId,
                   lTransactionId,
                   lQueryId,
                   tsQueryKey,
                   lRootId,
                   lyMetricId
              from _utility_temp_metric_transaction;

            -- If root is null then reset
            if lRootId is null then
                declare
                    tsQueryBegin timestamp with time zone;
                begin
                    -- If a journal transaction id was passed then store it
                    if lJournalTransactionId is not null then
                        insert into _utility.metric_raw_transaction_journal_map (metric_transaction_id,
                                                                                 journal_transaction_id)
                                                                         values (lTransactionId,
                                                                                 lJournalTransactionId);

                    -- Otherwise see if there was a new query
                    else
                        -- Get the query start time to see if we have started a new query
                        select query_start
                          into tsQueryBegin
                          from _utility.process_list()
                         where pid = pg_backend_pid();

                        -- If a new query has started then store it
                        if tsQueryKey is distinct from tsQueryBegin then
                            lQueryId = nextval('_scd.object_id_seq');

                            insert into _utility.metric_raw_query (id, metric_transaction_id, timestamp_begin, sql)
                                                           values (lQueryId, lTransactionId, tsQueryBegin, current_query());

                            tsQueryKey = tsQueryBegin;
                        end if;

                        -- Reset function stack
                        lRootId = lMetricId;
                        lyMetricId[1] = lMetricId;
                        iDepth = 0;
                    end if;

                    -- Truncate the transaction table to keep it from growing
                    truncate table _utility_temp_metric_transaction;

                    -- Recreate the transaction temp table
                    insert into _utility_temp_metric_transaction (metric_transaction_id, metric_query_id, metric_query_key,
                                                                  metric_root_id, metric_id)
                                                          values (lTransactionId, lQueryId, tsQueryKey, lRootId, lyMetricId);

                    -- If a journal transaction id was passed then exit - there are no function metrics to store
                    if lJournalTransactionId is not null then
                        return null;
                    end if;
                end;
            -- Else add the current metric to the stack
            else
                iDepth = array_upper(lyMetricId, 1);
                lParentId = lyMetricId[iDepth];
                lyMetricId[iDepth + 1] = lMetricId;

                update _utility_temp_metric_transaction
                   set metric_id = lyMetricId;
            end if;
    end;

    -- Insert the metric record
    insert into _utility.metric_raw_begin (id, parent_id, metric_query_id, depth, current_user_name,
                                           schema_name, function_name, parameter, timestamp)
                                   values (lMetricId, lParentId, lQueryId, iDepth, current_user, strSchemaName, 
                                           strFunctionName, strParameter, tsTimestampBegin);

    return lMetricId;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* METRIC_END Function
**********************************************************************************************************************************/;
create or replace function _utility.metric_end
(
    lMetricId bigint,
    stryResult text[][] default null,
    bCached boolean default false
)
    returns void as $$
declare
    lMetricExpectedId bigint;
begin
    -- If running on a hot standby, return without recording metrics
    if current_setting('transaction_read_only') = 'on' then
        return;
    end if;

    -- Get the last function off the stack - this is one that should be ending
    select metric_id[array_upper(metric_id, 1)]
      into lMetricExpectedId
      from _utility_temp_metric_transaction;

    -- Make sure that we are ending the correct function
    if  lMetricId <> lMetricExpectedId then
        declare
            strActualFunction text;
            strExpectedFunction text;
        begin
            -- Create the expected function name
            select schema_name || '.' || function_name
              into strExpectedFunction
              from _utility.metric_raw_begin
             where id = lMetricExpectedId;

            -- Create the actual function name
            select schema_name || '.' || function_name
              into strActualFunction
              from _utility.metric_raw_begin
             where id = lMetricId;

            -- Raise the exception
            raise exception 'ended function is not the bottom of the stack: expected "%", actual "%", stack %',
                            strExpectedFunction, strActualFunction,
                            (select metric_id from _utility_temp_metric_transaction);
        end;
    end if;

    -- Insert the end metric
    insert into _utility.metric_raw_end (id, cached, result, timestamp)
                                 values (lMetricId, bCached, stryResult, clock_timestamp());

    -- Reset root_id if the root function is ending
    if lMetricId = (select metric_root_id from _utility_temp_metric_transaction) then
        update _utility_temp_metric_transaction
           set metric_root_id = null;

    -- Else pop the last function off the stack
    else
        update _utility_temp_metric_transaction
           set metric_id = metric_id[1:array_upper(metric_id, 1) - 1];
    end if;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* METRIC_PROCESS Function
**********************************************************************************************************************************/;
create or replace function _utility.metric_process()
    returns void as $$
declare
    lMetricId bigint = _utility.metric_begin('_utility', 'metric_process');

    lMetricMinId bigint;
    lMetricMaxId bigint;
    iMetricTotal int;

    lProcessMaxId bigint;
    lTransactionMaxId bigint;
    lQueryMaxId bigint;
begin
    -- Find the max and min metrics
    select min(id),
           max(id) - 1
      into lMetricMinId,
           lMetricMaxId
      from _utility.metric_raw_begin;

    -- Find all the max IDs to move
    select max(metric_process_id),
           max(metric_transaction_id),
           max(metric_query_id)
      into lProcessMaxId,
           lTransactionMaxId,
           lQueryMaxId
      from _utility.vw_metric
     where id between lMetricMinId and lMetricMaxId;

    -- Move raw process rows
    insert into _utility.metric_process
    select *
      from _utility.vw_metric_process
     where id between
    (
        select min(id)
          from _utility.metric_raw_process
          
    ) and lProcessMaxId
       and not exists
    (
        select 1
          from _utility.metric_process
         where metric_process.id = vw_metric_process.id
    );

    delete from _utility.metric_raw_process
     where id <= lProcessMaxId;    

    -- Move raw transaction rows
    insert into _utility.metric_transaction
    select *
      from _utility.vw_metric_transaction
     where id between
    (
        select min(id)
          from _utility.metric_raw_transaction
          
    ) and lTransactionMaxId
       and not exists
    (
        select 1
          from _utility.metric_transaction
         where metric_transaction.id = vw_metric_transaction.id
    );

    delete from _utility.metric_raw_transaction
     where id <= lTransactionMaxId;

    delete from _utility.metric_raw_transaction_journal_map
     where metric_transaction_id <= lTransactionMaxId;

    -- Move raw query rows
    insert into _utility.metric_query
    select *
      from _utility.vw_metric_query
     where id between
    (
        select min(id)
          from _utility.metric_raw_query
          
    ) and lQueryMaxId
       and not exists
    (
        select 1
          from _utility.metric_query
         where metric_query.id = vw_metric_query.id
    );     

    delete from _utility.metric_raw_query
     where id <= lQueryMaxId;

    -- Move raw metric rows
    insert into _utility.metric
    select *
      from _utility.vw_metric
     where id between lMetricMinId and lMetricMaxId;

    delete from _utility.metric_raw_begin
     where id <= lMetricMaxId;

    delete from _utility.metric_raw_end
     where id <= lMetricMaxId;

    perform _utility.metric_end(lMetricId);
end
$$ language plpgsql security definer;
