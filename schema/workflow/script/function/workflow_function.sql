create or replace function _workflow.trigger_function_get(strSchema text, strTable text, eType _utility.trigger_type) returns text as $$
declare
    strBody text;
    strConfigWorkflowName text;
    strException text;
    strMessage text = null;
    strCondition text = null;
    strType text;
    xJobResult record;
begin
    select config_workflow.name
      into strConfigWorkflowName
      from _workflow.config_job_table, 
           _workflow.config_workflow
     where config_job_table.schema_name = strSchema
       and config_job_table.table_name = strTable
       and config_job_table.config_workflow_id = config_workflow.id;

    strException = 'raise exception ''This table is a part of workflow ''''' || strConfigWorkflowName || '''''.';

    if
    (
        select count(*)
          from _workflow.job_table, 
               _workflow.job,
               _workflow.workflow
             where job_table.schema_name = strSchema
               and job_table.table_name = strTable
               and job_table.job_id = job.id
               and job.state = 'running'
               and job.workflow_id = workflow.id
               and (workflow.key_field is null or
                    workflow.key_field not in 
            (
                select pg_attribute.attname
                  from pg_namespace
                       inner join pg_class
                           on pg_class.relnamespace = pg_namespace.oid
                          and pg_class.relname = strTable
                       inner join pg_attribute
                           on pg_attribute.attrelid = pg_class.oid
                          and pg_attribute.attname = workflow.key_field
                          and pg_attribute.attnum > 0
                 where pg_namespace.nspname = strSchema
            ))
    ) > 0 then
        strBody = '    -- Workflow is active for the entire table';
    else               
        for xJobResult in
            select workflow.key,
                   workflow.key_field
              from _workflow.job_table,
                   _workflow.job,
                   _workflow.workflow
             where job_table.schema_name = strSchema
               and job_table.table_name = strTable
               and job_table.job_id = job.id
               and job.state = 'running'
               and job.workflow_id = workflow.id
             group by workflow.key,
                      workflow.key_field
        loop
            if strCondition is not null then
                strCondition = strCondition || ' and' || E'\r\n' || '       ';
                strMessage = strMessage || ', ';
            else
                strCondition = '';
                strMessage = 'wave_id = (';
            end if;

            if eType = 'delete' then
                strType = 'old';
            else
                strType = 'new';
            end if;

            strCondition = strCondition || strType || '.' || xJobResult.key_field || ' != ' || xJobResult.key;
            strMessage = strMessage || xJobResult.key;
        end loop;

        if strCondition is not null then
            strBody = 
                     '    if ' || strCondition || ' and' || E'\r\n' ||
                     '       current_user <> ''' || _utility.role_get('') || ''' then' || E'\r\n' ||
                     '        ' || strException || '  Only ' || eType || 's where ' || strMessage || ') are possible.'';' || E'\r\n' ||
                     '    end if;';
        else
            strBody =
                     '    if current_user <> ''' || _utility.role_get('') || ''' then' || E'\r\n' ||
                     '        ' || strException || '  No workflows are active so ' || eType || 's are not possible.'';' || E'\r\n' ||
                     '    end if;';
        end if;
    end if;        

    return(strBody);
end;
$$ language plpgsql security definer;

create or replace function _workflow.table_trigger_create(bInitial boolean, strSchema text, 
                                                          strTable text) returns void as $$
begin
    execute _utility.trigger_function_create('workflow', strSchema, strTable, 'insert', 'before', 'invoker', null, 
                                            _workflow.trigger_function_get(strSchema, strTable, 'insert'));
    execute _utility.trigger_function_create('workflow', strSchema, strTable, 'update', 'before', 'invoker', null, 
                                            _workflow.trigger_function_get(strSchema, strTable, 'update'));
    execute _utility.trigger_function_create('workflow', strSchema, strTable, 'delete', 'before', 'invoker', null, 
                                            _workflow.trigger_function_get(strSchema, strTable, 'delete'));

    if bInitial then
        execute _utility.trigger_create('workflow', strSchema, strTable, 'insert', 'before');
        execute _utility.trigger_create('workflow', strSchema, strTable, 'update', 'before');
        execute _utility.trigger_create('workflow', strSchema, strTable, 'delete', 'before');
    end if;
end;
$$ language plpgsql security definer;

create or replace function _workflow.table_trigger_drop(strSchema text, strTable text) returns void as $$
begin
    execute _utility.trigger_drop('workflow', strSchema, strTable, 'insert', 'before');
    execute _utility.trigger_drop('workflow', strSchema, strTable, 'update', 'before');
    execute _utility.trigger_drop('workflow', strSchema, strTable, 'delete', 'before');
    
    execute _utility.trigger_function_drop('workflow', strSchema, strTable, 'insert', 'before');
    execute _utility.trigger_function_drop('workflow', strSchema, strTable, 'update', 'before');
    execute _utility.trigger_function_drop('workflow', strSchema, strTable, 'delete', 'before');
end;
$$ language plpgsql security definer;

create or replace function _workflow.state_update(iJobId int) returns void as $$
declare
    xJobResult record;
    xJobTableResult record;
begin
    for xJobResult in
        select *
          from _workflow.job job_top
         where id in
        (
            select job.id
              from _workflow.job_map, _workflow.job
             where job_map.parent_id = iJobId
               and job_map.id = job.id
               and job.state = 'pending'
                union
            select job.id
              from _workflow.job job_parent, 
                   _workflow.workflow workflow_parent,
                   _workflow.workflow,
                   _workflow.job
             where job_parent.id = iJobId
               and job_parent.workflow_id = workflow_parent.id
               and workflow_parent.name = workflow.name
               and workflow_parent.key_field = workflow.key_field
               and workflow_parent.key = workflow.key_serialize
               and workflow.id = job.workflow_id
               and job.serialize = true
               and job.name = job_parent.name
               and job.state = 'pending'
        )
           and not exists
        (
            select job.id
              from _workflow.job_map, _workflow.job
             where job_map.id = job_top.id
               and job_map.parent_id = job.id
               and job.state <> 'complete'
                union
            select job.id
              from _workflow.job job_parent, 
                   _workflow.workflow workflow_parent,
                   _workflow.workflow,
                   _workflow.job
             where job.id = job_top.id
               and job.serialize = true
               and job.workflow_id = workflow.id
               and workflow.name = workflow_parent.name
               and workflow.key_field = workflow_parent.key_field
               and workflow.key_serialize = workflow_parent.key
               and workflow_parent.id = job_parent.workflow_id
               and job_parent.name = job.name
               and job_parent.state <> 'complete'
        )
    loop
        update _workflow.job
           set state = 'ready'
         where id = xJobResult.id;
    end loop;
end;
$$ language plpgsql security definer;

create or replace function _workflow.job_rollback(iJobId int) returns void as $$
declare
    xJobTableResult record;
    strKeyField text;
    lKey bigint;
    strDetail text = null;
    iRowsDeleted int;
    strState text;
    strPartitionTypeKey text;
    bPartitionMulti boolean;
    strPartitionName text;
begin
    raise notice 'Rollback of job % started', iJobId;

    select workflow.key_field,
           workflow.key,
           job.state
      into strKeyField,
           lKey,
           strState
      from _workflow.job, _workflow.workflow
     where job.id = iJobId
       and job.workflow_id = workflow.id;

    for xJobTableResult in
        select schema_name,
               table_name,
               rollback_action
          from _workflow.job_table
         where job_table.job_id = iJobId
         order by id desc
    loop
        -- If the action is delete
        if xJobTableResult.rollback_action = 'delete' then

            -- If there is no workflow key then the entire table is truncated
            if lKey is null then
                raise notice 'Truncating %.%', xJobTableResult.schema_name, xJobTableResult.table_name;

                execute 'truncate table ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name;

                strDetail = strDetail || 'Truncated ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name;

            -- Else see if a delete or truncate is needed
            else
                -- See if the workflow key is partitioned
                with partition_type as
                (
                    select partition_type.id,
                           partition_type.key
                      from _utility.partition_table
                           inner join _utility.partition_type
                                on partition_type.partition_table_id = partition_table.id
                               and partition_type.key = strKeyField
                           inner join _utility.partition_type_map
                                on partition_type_map.id = partition_type.id
                               and partition_type_map.level = 0
                               and partition_type_map.depth = 0
                     where partition_table.schema_name = xJobTableResult.schema_name
                       and partition_table.name = xJobTableResult.table_name
                ),
                partition as
                (
                    select partition_type.id as partition_type_id,
                           partition.id as id,
                           unnest(partition.key) as key,
                           array_upper(partition.key, 1) as total
                      from partition_type
                           inner join _utility.partition
                                on partition.partition_type_id = partition_type.id
                )
                select partition_type.key as partition_type_key,
                       case when partition.total = 1 or partition.total is null then false else true end as multi,
                       array_to_string(_utility.partition_tree_get(partition.id), '_') as partition_name
                  into strPartitionTypeKey,
                       bPartitionMulti,
                       strPartitionName
                  from partition_type
                       left outer join partition
                            on partition.partition_type_id = partition_type.id
                           and partition.key = lKey::text;

                -- If partitioned with only one key per partition then do a truncate
                if strPartitionTypeKey is not null and bPartitionMulti = false then
                    -- If the key exists then truncate
                    if strPartitionName is not null then
                        execute 'truncate table ' || xJobTableResult.schema_name || '_partition.' || xJobTableResult.table_name || '_' || strPartitionName;

                        if strDetail is not null then
                            strDetail = strDetail || E'\r\n';
                        else
                            strDetail = '';
                        end if;

                        strDetail = strDetail ||
                                    'Truncated ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name || ' partition ' || strPartitionName;

                        raise notice 'Truncated %.% partition %', xJobTableResult.schema_name, xJobTableResult.table_name, strPartitionName;

                        raise notice 'Analyzing %_partition.%_%', xJobTableResult.schema_name, xJobTableResult.table_name, strPartitionName;
                        execute 'analyze ' || xJobTableResult.schema_name || '_partition.' || xJobTableResult.table_name || '_' || strPartitionName;

                    -- Else the key does not exist so nothing to truncate
                    else
                        strDetail = strDetail ||
                                    'No instance of ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name || ' partition type ' || strPartitionTypeKey || ' - truncate not required';

                        raise notice 'No instance of %.% partition type % - truncate not required', xJobTableResult.schema_name, xJobTableResult.table_name, strPartitionTypeKey;
                    end if;

                -- If not partitioned (or multiple keys per partition) then a delete is required
                else
                    raise notice 'Deleting from %.% where key = %', xJobTableResult.schema_name, xJobTableResult.table_name, lKey;
                    
                    execute 'delete from ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name || ' where ' || 
                            strKeyField || ' = ' || lKey;

                    get diagnostics iRowsDeleted = row_count;

                    if strDetail is not null then
                        strDetail = strDetail || E'\r\n';
                    else
                        strDetail = '';
                    end if;

                    strDetail = strDetail ||
                                iRowsDeleted || ' row(s) deleted from ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name;

                    raise notice 'Deleted % rows from %.% where key = %', iRowsDeleted, xJobTableResult.schema_name, xJobTableResult.table_name, lKey;

                    if iRowsDeleted > 0 then
                        if strPartitionName is not null then
                            raise notice 'Analyzing %_partition.%_%', xJobTableResult.schema_name, xJobTableResult.table_name, strPartitionName;
                            execute 'analyze ' || xJobTableResult.schema_name || '_partition.' || xJobTableResult.table_name || '_' || strPartitionName;
                        else
                            raise notice 'Analyzing %.%', xJobTableResult.schema_name, xJobTableResult.table_name;
                            execute 'analyze ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name;
                        end if;
                    end if;
                end if;
            end if;

        -- else preserve the rows
        else
            raise notice 'Preserving %.%', xJobTableResult.schema_name, xJobTableResult.table_name;
            strDetail = strDetail || 'Preserved ' || xJobTableResult.schema_name || '.' || xJobTableResult.table_name;
        end if;
          
        execute _workflow.table_trigger_create(false, xJobTableResult.schema_name, xJobTableResult.table_name);
    end loop;

    delete from _workflow.job_validate_result
     where job_id = iJobId;

    if strState = 'ready' then
        execute _workflow.job_log(iJobId, 'rollback', session_user, strDetail, false);
    else
        execute _workflow.job_log(iJobId, 'rollback', current_user, strDetail, false);
    end if;

    -- Run any events associated with the state change
    perform _workflow.job_event_run(iJobId, 'rollback');
    
    -- Change to the final state (ready or pending);
    execute _workflow.job_log(iJobId, strState, current_user, null, true);

    raise notice 'Rollback of job % completed', iJobId;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* JOB_RETRY Function
***********************************************************************************************************************************/
create or replace function _workflow.job_retry
(
    iJobId int
)
    returns void as $$
declare
    iConfigJobId int;

    iRetryRepeatTotal int;
    strErrorType text;
    vInterval interval;

    iRetryGroup int = null;
    iRetryRepeat int = null;
    tsRetryTimestamp timestamp with time zone = null;
    bError boolean = true;
begin
    -- Get the config job id
    select config_job.id
      into iConfigJobId
      from _workflow.job
           inner join _workflow.workflow
                on workflow.id = job.workflow_id
           inner join _workflow.config_workflow
                on config_workflow.name = workflow.name
           inner join _workflow.config_job
                on config_job.config_workflow_id = config_workflow.id
               and config_job.name = job.name
     where job.id = iJobId;

    -- Are there retries for this job?
    if
    (
        select count(*) > 0
          from _workflow.config_job_retry
         where config_job_retry.config_job_id = iConfigJobId
          limit 1
    ) then
        -- Find the most recent retry
        select retry_group,
               retry_repeat
          into iRetryGroup,
               iRetryRepeat
          from _workflow.job_log
         where job_id = iJobId
           and state = 'error'
           and id >
        (
            select coalesce(max(id), 0)
              from _workflow.job_log
             where job_id = iJobId
               and state in ('complete', 'pause')
        )
         order by id desc
         limit 1;

        -- If there is no retry then start
        if iRetryGroup is null then
            iRetryGroup = 1;
            iRetryRepeat = null;
        end if;

        -- Get the retry info
        select interval,
               repeat,
               error
          into vInterval,
               iRetryRepeatTotal,
               strErrorType
          from _workflow.config_job_retry
         where config_job_id = iConfigJobId
         order by id
         limit 1 offset iRetryGroup - 1;

        -- Make sure that the group is valid
        if not found then
            raise exception 'Config job id %, retry group % is not valid', iConfigJobId, iRetryGroup;
        end if;

        -- Error if the repeat value is invalid
        if iRetryRepeat is not null and iRetryRepeat not between 1 and iRetryRepeatTotal then
            raise exception 'Config job id %, retry group %, repeat % is not valid (repeat max is %)', iConfigJobId, iRetryGroup, iRetryRepeat, iRetryRepeatTotal;
        end if;

        -- Check the error condition of the retry
        if (strErrorType = 'none') or
           (strErrorType = 'last' and iRetryRepeat <> iRetryRepeatTotal) then
            bError = false;
        end if;

        -- If this is the last retry, then get the next group
        if iRetryRepeat = iRetryRepeatTotal then
            iRetryGroup = iRetryGroup + 1;
            iRetryRepeat = 1;
        
            select interval,
                   repeat,
                   error
              into vInterval,
                   iRetryRepeatTotal,
                   strErrorType
              from _workflow.config_job_retry
             where config_job_id = iConfigJobId
             order by id
             limit 1 offset iRetryGroup - 1;

            if not found then
                iRetryGroup = null;
                iRetryRepeat = null;
                vInterval = null;
            end if;
        else
            iRetryRepeat = coalesce(iRetryRepeat + 1, 1);
        end if;

        tsRetryTimestamp = clock_timestamp() + vInterval;
    end if;

    execute _workflow.job_log(iJobId, 'error', session_user, null, false, bError, iRetryGroup, iRetryRepeat, tsRetryTimestamp);
end;
$$ language plpgsql security definer;

create or replace function _workflow.job_validate(iJobId int, strOldState text) returns void as $$
declare
    iWorkflowId int;
    strKeyField text;
    lKey bigint;
    xValidateResult record;
    xResult record;
    strDetail text = null;
    iJobLogId int = null;
    lCount bigint;
    lValidateCount bigint = 0;
    lErrorCount bigint = 0;
    xTableResult record;
    strTable text;
    strPartitionName text;
    bExclude boolean;
begin
    select workflow.id,
           workflow.key_field,
           workflow.key
      into iWorkflowId,
           strKeyField,
           lKey
      from _workflow.job, _workflow.workflow
     where job.id = iJobId
       and job.workflow_id = workflow.id;

    if strOldState = 'error' then       
        if not pg_has_role(session_user, _utility.role_get('admin'), 'usage') then
            raise exception 'Only members of % can completed an errored job', _utility.role_get('admin');
        end if;
    else
        for xTableResult in
            select schema_name,
                   table_name
              from _workflow.job_table
             where job_table.job_id = iJobId
             order by id
        loop
            -- See if the workflow key is contained in a partition
            with partition_type as
            (
                select partition_type.id,
                       partition_type.key
                  from _utility.partition_table
                       inner join _utility.partition_type
                            on partition_type.partition_table_id = partition_table.id
                           and partition_type.key = strKeyField
                       inner join _utility.partition_type_map
                            on partition_type_map.id = partition_type.id
                           and partition_type_map.level = 0
                           and partition_type_map.depth = 0
                 where partition_table.schema_name = xTableResult.schema_name
                   and partition_table.name = xTableResult.table_name
            ),
            partition as
            (
                select partition_type.id as partition_type_id,
                       partition.id as id,
                       unnest(partition.key) as key,
                       array_upper(partition.key, 1) as total
                  from partition_type
                       inner join _utility.partition
                            on partition.partition_type_id = partition_type.id
            )
            select array_to_string(_utility.partition_tree_get(partition.id), '_') as partition_name
              into strPartitionName
              from partition_type
                   left outer join partition
                        on partition.partition_type_id = partition_type.id
                       and partition.key = lKey::text;

            -- If in a partition then analyze the partition only
            if strPartitionName is not null then
                execute 'analyze ' || xTableResult.schema_name || '_partition.' || xTableResult.table_name || '_' || strPartitionName;

            -- Else analyze the entire table
            else
                execute 'analyze ' || xTableResult.schema_name || '.' || xTableResult.table_name;
            end if;
        end loop;
    
        for xValidateResult in
            select id,
                   state,
                   action,
                   name,
                   sql
              from _workflow.job_validate
             where job_id = iJobId
               and type = 'object_list'
             order by ordering
        loop
            if iJobLogId is null then
                iJobLogId = _workflow.job_log_id(iJobId, 'validate', current_user, strDetail, false);
            end if;

            lCount = 0;

            if xValidateResult.state = 'warning' then
                bExclude = false;
            else
                bExclude = true;
            end if;

            for xResult in
                execute xValidateResult.sql
            loop
                insert into _workflow.job_validate_result ( job_id,    job_validate_id,  object_id,  exclude)
                                                  values ( iJobId, xValidateResult.id, xResult.id, bExclude);

                lCount = lCount + 1;
            end loop;

            lValidateCount = lValidateCount + lCount;

            if xValidateResult.state = 'error' and xValidateResult.action = 'fail' then
                lErrorCount = lErrorCount + lCount;
            end if;

            if lCount > 0 then
                if strDetail is not null then
                    strDetail = strDetail || E'\r\n';
                else
                    strDetail = '';
                end if;

                strDetail = strDetail ||
                            xValidateResult.name || ': ' || lCount;

                if xValidateResult.state = 'error' then
                    strDetail = strDetail || ' errored';
                else
                    if xValidateResult.state = 'drop' then
                        strDetail = strDetail || ' dropped';
                    else
                        strDetail = strDetail || ' warned';
                    end if;
                end if;
            end if;
        end loop;

        if iJobLogId is not null then
            update _workflow.job_log
               set detail = strDetail
             where id = iJobLogId;
        end if;

        if lErrorCount > 0 then
            execute _workflow.job_log(iJobId, 'error', current_user, 'There were ' || lErrorCount || ' validation error(s).  See _workflow.job_validate_result.job_id = ' || iJobId || ' for details.', false);

            update _workflow.job
               set state = 'error'
             where id = iJobId;

            return;
        end if;
    end if;

    -- Only analyze the job_validate_result table if validation records were inserted
    if lValidateCount > 0 then
        analyze _workflow.job_validate_result;
    end if;
    
    execute _workflow.job_log(iJobId, 'complete', session_user, null, false);
end;
$$ language plpgsql security definer;

create or replace function _workflow.state_rollback(iJobId int, strOldState text) returns void as $$
declare
    xJobResult record;
    strState text;
    bRollbackAllow boolean;
    strName text;
begin
    if not pg_has_role(session_user, _utility.role_get('admin'), 'usage') and
       not pg_has_role(current_user, _utility.role_get(), 'usage') then
        raise exception 'Only members of % can rollback a job (state = ''ready'')', _utility.role_get('admin');
    end if;

    select state,
           rollback_allow,
           name
      into strState,
           bRollbackAllow,
           strName
      from _workflow.job
     where job.id = iJobId;

    if not bRollbackAllow and strOldState = 'complete' then
        raise exception 'Job % (%) cannot be rolled back', iJobId, strName;
    end if;

    for xJobResult in
        select job.id, job.state
        from _workflow.job_map, _workflow.job
        where job_map.parent_id = iJobId
          and job_map.id = job.id
          and job.state <> 'pending'
            union
        select job.id, job.state
          from _workflow.job job_parent, 
               _workflow.workflow workflow_parent,
               _workflow.workflow,
               _workflow.job
         where job_parent.id = iJobId
           and job_parent.workflow_id = workflow_parent.id
           and workflow_parent.name = workflow.name
           and workflow_parent.key_field = workflow.key_field
           and workflow_parent.key = workflow.key_serialize
           and workflow.id = job.workflow_id
           and job.serialize = true
           and job.name = job_parent.name
           and job.state <> 'pending'
    loop
        update _workflow.job
           set state = 'pending'
         where id = xJobResult.id;

        if xJobResult.state <> 'ready' then
            execute _workflow.job_rollback(xJobResult.id);
        end if;            
    end loop;

    if strState = 'ready' then
        execute _workflow.job_rollback(iJobId);
    end if;
end;
$$ language plpgsql security definer;

create or replace function _workflow.job_log
(
    iJobId int,
    strState text,
    strUserName text,
    strDetail text,
    bRunIncrement boolean,
    bReportError boolean default false,
    iRetryGroup int default null,
    iRetryRepeat int default null,
    tsRetryTimestamp timestamp with time zone default null
) 
    returns void as $$
declare
    iJobLogId int;
begin
    iJobLogId = _workflow.job_log_id(iJobId, strState, strUserName, strDetail, bRunIncrement, bReportError, iRetryGroup,
                                     iRetryRepeat, tsRetryTimestamp);
end;
$$ language plpgsql security definer;

create or replace function _workflow.job_log_id
(
    iJobId int,
    strState text,
    strUserName text,
    strDetail text,
    bRunIncrement boolean,
    bReportError boolean default false,
    iRetryGroup int default null,
    iRetryRepeat int default null,
    tsRetryTimestamp timestamp with time zone default null
) 
    returns int as $$
declare
    iWorkflowId int;
    iJobLogId int;
    iRunId int;
begin
    if strUserName = 'postgres' then
        raise exception 'Logged operations in workflow cannot be executed as the ''postgres'' user';
    end if;

    select workflow_id
      into iWorkflowId
      from _workflow.job
     where id = iJobId;

    select max(run_id)
      into iRunId
      from _workflow.job_log
     where job_id = iJobId;

    if iRunId is null then
        iRunId = 1;
    end if;

    if bRunIncrement then
        iRunId = iRunId + 1;
    end if;

    select nextval('_workflow.object_id_seq')
      into iJobLogId;

    insert into _workflow.job_log (workflow_id, job_id, id, run_id, state, user_name, detail, report_error, retry_group, 
                                   retry_repeat, retry_timestamp)
                           values (iWorkflowId, iJobId, iJobLogId, iRunId, strState, strUserName, strDetail, bReportError,
                                   iRetryGroup, iRetryRepeat, tsRetryTimestamp);

    return iJobLogId;
end;
$$ language plpgsql security definer;

create or replace function _workflow.job_event_run(iJobId int, strState text) returns void as $$
declare
    rEvent record;
    lWorkflowKey bigint;
begin
    select workflow.key
      into lWorkflowKey
      from _workflow.job
           inner join _workflow.workflow
                on workflow.id = job.workflow_id
     where job.id = iJobId;

    for rEvent in
        select sql
          from _workflow.job_event
         where job_id = iJobId
           and state = strState
         order by id
    loop
        execute replace(E'do \$\$\nbegin\n' || rEvent.sql || E'\nend \$\$', '%$KEY$%', coalesce(lWorkflowKey::text, 'null'));
    end loop;
end;
$$ language plpgsql security definer;
