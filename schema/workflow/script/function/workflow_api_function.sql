/***********************************************************************************************************************************
START Function
***********************************************************************************************************************************/
create or replace function _workflow.start(strName text, lKey bigint default null, lKeySerialize bigint default null) returns int as $$
begin
    return _workflow.workflow_add(strName, lKey, lKeySerialize);
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _workflow.start(text, bigint, bigint) to ' || _utility.role_get('admin'); end $$;

/***********************************************************************************************************************************
UPDATE Function
***********************************************************************************************************************************/
create or replace function _workflow.update(iWorkflowId int) returns void as $$
declare
    iConfigWorkflowId int;
    rJob record;
    iJobId int;
    rTable record;
    rValidate record;
begin
    -- Make sure the workflow is complete
    if
    (
        select state <> 'complete'
          from _workflow.workflow
         where id = iWorkflowId
    ) then
        raise exception 'Unable to upgrade unfinished workflow';
    end if;
    
    -- Make sure the workflow is not locked
    if
    (
        select locked = true
          from _workflow.workflow
         where id = iWorkflowId
    ) then
        raise exception 'Unable to upgrade locked workflow';
    end if;

    -- Lock all the affected tables
    lock table _workflow.config_workflow in exclusive mode;
    lock table _workflow.config_job in exclusive mode;
    lock table _workflow.config_job_validate in exclusive mode;
    lock table _workflow.config_job_table in exclusive mode;
    lock table _workflow.config_job_map in exclusive mode;
    lock table _workflow.workflow in exclusive mode;
    lock table _workflow.job in exclusive mode;
    lock table _workflow.job_validate in exclusive mode;
    lock table _workflow.job_table in exclusive mode;
    lock table _workflow.job_map in exclusive mode;

    -- Disable the job trigger so states can be changed
    alter table _workflow.job disable trigger job_trigger_update_after;

    -- Get the workflow config id
    select config_workflow.id
      into iConfigWorkflowId
      from _workflow.workflow, _workflow.config_workflow
     where workflow.id = iWorkflowId
       and workflow.name = config_workflow.name;

    if iConfigWorkflowId is null then
        raise exception 'There is no configuration for workflow %, (%)', iWorkflowId, 
                        (select name from _workflow.workflow where id = iWorkflowId);
    end if;

    create temp table temp_job
    (
        id int not null,
        config_id int not null,
        name text not null,
        new boolean not null,
        rollback boolean not null
    );

    -- Remove the old mappings
    delete from _workflow.job_map
     where workflow_id = iWorkflowId;

    -- Remove old jobs
    for rJob in 
        select job.id,
               job.name
          from _workflow.job
         where job.workflow_id = iWorkflowId
           and job.name in
        (
            select job.name
              from _workflow.job
             where workflow_id = iWorkflowId
                except
            select config_job.name
              from _workflow.config_job
             where config_workflow_id = iConfigWorkflowId
        )
    loop
        delete 
          from _workflow.job
         where id = rJob.id;
    end loop;
     
    -- Add the new jobs
    for rJob in
        select config_job.id,
               config_job.name
          from _workflow.config_job
         where config_job.config_workflow_id = iConfigWorkflowId
           and config_job.name in
        (
            select config_job.name
              from _workflow.config_job
             where config_workflow_id = iConfigWorkflowId
                except
            select job.name
              from _workflow.job
             where workflow_id = iWorkflowId
        )
         order by config_job.id
    loop
        iJobId = _workflow.job_add(iWorkflowId, rJob.id);
        update _workflow.job set state = 'complete' where id = iJobId;
        
        insert into temp_job (config_id, id, name, new, rollback) values (rJob.id, iJobId, rJob.name, true, true);
    end loop;

    -- Check for modified jobs
    for rJob in
        select job.id,
               config_job.id as config_id
          from _workflow.job
               inner join _workflow.workflow
                    on workflow.id = job.workflow_id
               inner join _workflow.config_workflow
                    on config_workflow.name = workflow.name
               inner join _workflow.config_job
                    on config_job.config_workflow_id = config_workflow.id
                   and config_job.name = job.name
         where job.workflow_id = iWorkflowId
         order by config_job.id
    loop
        -- Check for added tables
        for rTable in
            select id as config_id
              from _workflow.config_job_table
             where config_job_id = rJob.config_id
               and schema_name || '.' || table_name in
            (
                select schema_name || '.' || table_name
                  from _workflow.config_job_table
                 where config_job_id = rJob.config_id
                    except 
                select schema_name || '.' || table_name
                  from _workflow.job_table
                 where job_id = rJob.id
            )
             order by config_job_id
        loop
            perform _workflow.job_table_add(rJob.Id, rTable.config_id);
        end loop;

        -- Check for dropped tables
        for rTable in
            select id
              from _workflow.job_table
             where job_id = rJob.id
               and schema_name || '.' || table_name in
            (
                select schema_name || '.' || table_name
                  from _workflow.job_table
                 where job_id = rJob.id
                    except 
                select schema_name || '.' || table_name
                  from _workflow.config_job_table
                 where config_job_id = rJob.config_id
            )
             order by job_id
        loop
            delete from _workflow.job_table
             where id = rTable.id;
        end loop;

        -- Check for modified tables
        for rTable in
            select schema_name, table_name, rollback_action
              from _workflow.config_job_table
             where config_job_id = rJob.config_id
                except
            select schema_name, table_name, rollback_action
              from _workflow.job_table
             where job_id = rJob.id
        loop
            update _workflow.job_table
               set rollback_action = rTable.rollback_action
             where job_id = rJob.id
               and schema_name = rTable.schema_name
               and table_name = rTable.table_name;
        end loop;

        -- Check for added validations
        for rValidate in
            select id as config_id
              from _workflow.config_job_validate
             where config_job_id = rJob.config_id
               and name in
            (
                select name
                  from _workflow.config_job_validate
                 where config_job_id = rJob.config_id
                    except 
                select name
                  from _workflow.job_validate
                 where job_id = rJob.id
            )
             order by config_job_id
        loop
            perform _workflow.job_validate_add(rJob.Id, rValidate.config_id);
        end loop;

        -- Check for dropped validations
        for rValidate in
            select id
              from _workflow.job_validate
             where job_id = rJob.id
               and name in
            (
                select name
                  from _workflow.job_validate
                 where job_id = rJob.id
                    except
                select name
                  from _workflow.config_job_validate
                 where config_job_id = rJob.config_id
            )
             order by id
        loop
            delete from _workflow.job_validate
             where id = rValidate.id;
        end loop;

        -- Check for modified validations
        for rValidate in
            select job_validate.id,
                   workflow.key as workflow_key,
                   config_job_validate.type,
                   config_job_validate.state,
                   config_job_validate.action,
                   config_job_validate.sql
              from _workflow.job_validate
                   inner join _workflow.job
                        on job.id = job_validate.job_id
                   inner join _workflow.workflow
                        on workflow.id = job.workflow_id
                   inner join _workflow.config_job
                        on config_job.config_workflow_id = iConfigWorkflowId
                       and config_job.name = job.name
                   inner join _workflow.config_job_validate
                        on config_job_validate.config_job_id = config_job.id
                       and config_job_validate.name = job_validate.name
             where job_validate.job_id = rJob.id
               and job_validate.name in
            (
                select name
                  from
                (
                    select name, sql, type, state, action
                      from _workflow.job_validate
                     where job_id = rJob.id
                        except
                    select name, sql, type, state, action
                      from _workflow.config_job_validate
                     where config_job_id = rJob.config_id
                ) validation
            )
             order by job_validate.id
        loop
            update _workflow.job_validate
               set type = rValidate.type,
                   state = rValidate.state,
                   action = rValidate.action,
                   sql = replace(rValidate.sql, '%$KEY$%', rValidate.workflow_key::text)
             where id = rValidate.id;
        end loop;
        
        --update job serialize only if something has changed
        update _workflow.job
           set serialize = job_serialize_changed.serialize
            from 
            (
                select job.id as job_id, 
                        config_job.serialize
                    from _workflow.config_workflow
                        inner join _workflow.config_job
                            on config_workflow.id = config_job.config_workflow_id
                        inner join _workflow.workflow
                            on workflow.name = config_workflow.name
                        inner join _workflow.job
                            on job.name = config_job.name
                           and job.workflow_id = workflow.id
                   where config_workflow.name = workflow.name
                     and job.id = rJob.id
                     and config_job.id = rJob.config_id
                     and job.serialize <> config_job.serialize
            ) as job_serialize_changed
        where job.id = job_serialize_changed.job_id;
    
        -- Update the validation ordering only if something has changed
        update _workflow.job_validate
           set ordering = config_job_validate.ordering
          from
            (
                select name,
                       ordering
                  from _workflow.config_job_validate
                 where config_job_id = rJob.config_id
            ) config_job_validate
         where job_validate.job_id = rJob.id
           and job_validate.name = config_job_validate.name
           and job_validate.ordering <> config_job_validate.ordering;
    end loop;

    -- Insert new mappings (excepting new jobs)
    insert into _workflow.job_map (workflow_id, id, parent_id) 
    select iWorkflowId as workflow_id,
           job.id,
           job_parent.id as parent_id
      from _workflow.config_job_map
           inner join _workflow.config_job
                on config_job.id = config_job_map.id
               and config_job.id not in (select config_id from temp_job where new = true)
           inner join _workflow.config_job config_job_parent
                on config_job_parent.id = config_job_map.parent_id
           inner join _workflow.job
                on job.workflow_id = iWorkflowId
               and job.name = config_job.name
           inner join _workflow.job job_parent
                on job_parent.workflow_id = iWorkflowId
               and job_parent.name = config_job_parent.name
     where config_job_map.config_workflow_id = iConfigWorkflowId
     order by job.id, job_parent.id;

    -- ??? What about job ordering?

    -- Enabled the job trigger
    alter table _workflow.job enable trigger job_trigger_update_after;

    -- Rollback jobs marked to be rolled back
/*    update _workflow.job
       set state = 'ready'
     where id in
    (
        select id
          from temp_job
         where rollback = true
    );*/

    -- Drop the temp job table
    drop table temp_job;
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _workflow.update(int) to ' || _utility.role_get('admin'); end $$;

/***********************************************************************************************************************************
* JOB_SQL_TOKEN_REPLACE Function
***********************************************************************************************************************************/
create or replace function _workflow.job_sql_token_replace
(
    iJobId int,
    strSql text
)
    returns text as $$
declare
    iConfigJobId int;
    iWorkflowId int;
    lWorkflowKey bigint;
    iConfigWorkflowId int;
begin
    select config_job.id,
           workflow.id,
           workflow.key,
           config_workflow.id
      into iConfigJobId,
           iWorkflowId,
           lWorkflowKey,
           iConfigWorkflowId
      from _workflow.job
           inner join _workflow.workflow
                on workflow.id = job.workflow_id
           inner join _workflow.config_workflow
                on config_workflow.name = workflow.name
           inner join _workflow.config_job
                on config_job.config_workflow_id = config_workflow.id
               and config_job.name = job.name
     where job.id = iJobId;

    strSql = replace(strSql, '%$JOB_ID$%', iJobId::text);
    strSql = replace(strSql, '%$CONFIG_JOB_ID$%', iConfigJobId::text);
    strSql = replace(strSql, '%$WORKFLOW_ID$%', iWorkflowId::text);
    strSql = replace(strSql, '%$WORKFLOW_KEY$%', coalesce(lWorkflowKey::text,'null'));
    strSql = replace(strSql, '%$CONFIG_WORKFLOW_ID$%', iConfigWorkflowId::text);

    return strSql;
end;
$$ language plpgsql security definer;

do $$ 
begin 
    execute '
        grant execute
           on function _workflow.job_sql_token_replace(int, text)
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* JOB_SCHEDULE Function
***********************************************************************************************************************************/
create or replace function _workflow.job_schedule
(
    iJobId int
)
    returns boolean as $$
declare
    strSchedule text = null;
    strResult text;
begin
    -- Get the job schedule
    select coalesce(config_job.schedule, config_workflow.schedule)
      into strSchedule
      from _workflow.job
           inner join _workflow.workflow
                on workflow.id = job.workflow_id
           inner join _workflow.config_workflow
                on config_workflow.name = workflow.name
           inner join _workflow.config_job
                on config_job.config_workflow_id = config_workflow.id
               and config_job.name = job.name
     where job.id = iJobId;

    if not found then
        raise exception 'Schedule for job % was not found', iJobId;
    end if;

    -- If the schedule is null return true
    if strSchedule is null then
        return true;
    end if;

    execute _workflow.job_sql_token_replace(iJobId, strSchedule) into strResult;

    -- If the result is boolean then return
    begin
        return strResult::boolean;
    exception
        when invalid_text_representation then
            null;
    end;

    -- Else evaluate the result as an expression
    execute strResult into strResult;

    return strResult::boolean;
end;
$$ language plpgsql security definer;

do $$ 
begin 
    execute '
        grant execute
           on function _workflow.job_schedule(int)
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
* JOB_REPORT_ERROR Function
***********************************************************************************************************************************/
create or replace function _workflow.job_report_error
(
    iJobId int
)
    returns boolean as $$
declare
    bReportError boolean;
begin
    select report_error
      into bReportError
      from _workflow.job_log
     where job_id = iJobId
     order by id desc
     limit 1;

    if not found then
        raise exception 'No log entries found for job %', iJobId;
    end if;

    return bReportError;
end;
$$ language plpgsql security definer;

do $$ 
begin 
    execute '
        grant execute
           on function _workflow.job_report_error(int)
           to ' || _utility.role_get('admin') || ', ' || _utility.role_get('etl');
end $$;

/***********************************************************************************************************************************
WORKFLOW Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_workflow_add
(
    strName text,
    strKeyField text default null,
    strSchedule text default null
) returns int as $$
declare
    iConfigWorkflowId int;
begin
    select nextval('_workflow.config_id_seq')
      into iConfigWorkflowId;

    insert into _workflow.config_workflow (               id,    name,   key_field,    schedule)
                                   values (iConfigWorkflowId, strName, strKeyField, strSchedule);

    return(iConfigWorkflowId);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.workflow_add(strName text, lKey bigint, lKeySerialize bigint) returns int as $$
declare
    iConfigWorkflowId int;
    iWorkflowId int;
    strWorkflowKeyField text;
    iJobId int;

    xJobResult record;
begin
    -- Get config params
    select id,
           key_field
      into iConfigWorkflowId,
           strWorkflowKeyField
      from _workflow.config_workflow
     where name = strName;

    -- Get the workflow id
    select nextval('_workflow.object_id_seq') 
      into iWorkflowId;

    -- Insert the new workflow
    insert into _workflow.workflow (         id,    name,           key_field,  key, key_serialize)
                            values (iWorkflowId, strName, strWorkflowKeyField, lKey, lKeySerialize);
                            
    -- Iterate through all jobs and add them
    for iJobId in (select config_job.id
                            from _workflow.config_job
                           where config_workflow_id = iConfigWorkflowId
                           order by id)
    loop 
        perform _workflow.job_add(iWorkflowId, iJobId);
    end loop;

    return(iWorkflowId);
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_add(iConfigWorkflowId int, strName text, bSerialize boolean default false, bRollbackAllow boolean default true, strSchedule text default null) returns int as $$
declare
    iConfigJobId int;
begin
    select nextval('_workflow.config_id_seq')
      into iConfigJobId;

    insert into _workflow.config_job (config_workflow_id,           id,    name,  serialize,  rollback_allow, schedule)
                              values ( iConfigWorkflowID, iConfigJobId, strName,  bSerialize, bRollbackAllow, strSchedule);

    return(iConfigJobId);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_add(iWorkflowId int, iConfigJobId int) returns int as $$
declare
    strName text;
    bSerialize boolean;
    bRollbackAllow boolean;
begin
    select name,
           serialize,
           rollback_allow
      into strName,
           bSerialize,
           bRollbackAllow
      from _workflow.config_job
     where id = iConfigJobId;

    return _workflow.job_add(iWorkflowId, strName, bSerialize, bRollbackAllow);
end
$$ language plpgsql security invoker;

create or replace function _workflow.job_add(iWorkflowId int, strName text, bSerialize boolean, bRollbackAllow boolean) returns int as $$
declare
    iConfigWorkflowId int;
    iConfigJobId int;
    lKeySerialize bigint;
    lKey bigint;
    iJobId int;
    strWorkflowKeyField text;

    xJobMapResult record;
    iMapTotal int = 0;
    
    xJobTableResult record;
    xJobValidateResult record;
begin
    -- Get config params
    select config_workflow.id,
           config_job.id,
           workflow.key_serialize,
           workflow.key,
           config_workflow.key_field
      into iConfigWorkflowId,
           iConfigJobId,
           lKeySerialize,
           lKey,
           strWorkflowKeyField
      from _workflow.workflow, _workflow.config_workflow, _workflow.config_job
     where workflow.id = iWorkflowId
       and workflow.name = config_workflow.name
       and config_workflow.id = config_job.config_workflow_id
       and config_job.name = strName;

    -- Get the job id
    select nextval('_workflow.object_id_seq') 
      into iJobId;

    -- Insert the job
    insert into _workflow.job (workflow_id,     id,    name,     state,  serialize, rollback_allow)
                       values (iWorkflowId, iJobId, strName, 'pending', bSerialize, bRollbackAllow);

    -- Set the map total to zero
    iMapTotal = 0;
    
    -- Find and insert parent mappings
    for xJobMapResult in
        select job.id
          from _workflow.config_job_map, 
               _workflow.config_job, 
               _workflow.job
         where config_job_map.id = iConfigJobId
           and config_job_map.parent_id = config_job.id
           and config_job.config_workflow_id = iConfigWorkflowId
           and job.workflow_id = iWorkflowId
           and config_job.name = job.name
    loop
        insert into _workflow.job_map (workflow_id,     id,        parent_id)
                               values (iWorkflowId, iJobId, xJobMapResult.id);

        iMapTotal = iMapTotal + 1;                                  
    end loop;

    -- Determine if there is a workflow serialization mapping
    if bSerialize and lKeySerialize is not null then
        if
        (
            select job.state
              from _workflow.workflow, _workflow.job
             where workflow.key = lKeySerialize
               and workflow.name = strName
               and workflow.key_field = strWorkflowKeyField
               and workflow.id = job.workflow_id
               and job.name = strName
        ) <> 'complete' then
            iMapTotal = iMapTotal + 1;
        end if;
    end if;
    -- Insert all job tables
    perform _workflow.job_table_add(iJobId, id)
       from _workflow.config_job_table 
      where config_job_id = iConfigJobId
      order by id;

    -- Insert all job events
    perform _workflow.job_event_add(iJobId, id)
       from _workflow.config_job_event
      where config_job_id = iConfigJobId
      order by id;

    -- Insert all job validations
    perform _workflow.job_validate_add(iJobId, id)
       from _workflow.config_job_validate 
      where config_job_id = iConfigJobId
      order by ordering;

    -- Set the job to ready only if there are no mappings
    if iMapTotal = 0 then
        update _workflow.job
           set state = 'ready'
         where id = iJobId;
    end if;

    -- Return the job id
    return iJobId;
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_RETRY Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_retry_add(iConfigJobId int, vInterval interval, iRepeat int, strError text default 'last') returns int as $$
declare
    iConfigWorkflowId int;
    iConfigJobRetryId int;
begin
    select config_workflow_id
      into iConfigWorkflowId
      from _workflow.config_job
     where id = iConfigJobId;

    select nextval('_workflow.config_id_seq')
      into iConfigJobRetryId;

    insert into _workflow.config_job_retry (config_workflow_id, config_job_id,                id,   interval,  repeat,    error)
                                    values ( iConfigWorkflowID,  iConfigJobId, iConfigJobRetryId,  vInterval, iRepeat, strError);

    return(iConfigJobRetryId);
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_TABLE Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_table_add(iConfigJobId int, strSchemaName text, strTableName text, strRollbackAction text default 'delete') returns int as $$
declare
    iConfigWorkflowId int;
    iConfigJobTableId int;
begin
    select config_workflow_id
      into iConfigWorkflowId
      from _workflow.config_job
     where id = iConfigJobId;

    select nextval('_workflow.config_id_seq') 
      into iConfigJobTableId;

    insert into _workflow.config_job_table (config_workflow_id, config_job_id,                id,    schema_name,     table_name,   rollback_action) 
                                    values ( iConfigWorkflowID,  iConfigJobId, iConfigJobTableId,  strSchemaName,   strTableName, strRollbackAction);

    return(iConfigJobTableId);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_table_add(iJobId int, iConfigJobTableId int) returns int as $$
declare
    strSchemaName text;
    strTableName text;
    strRollbackAction text;
begin
    select schema_name,
           table_name,
           rollback_action
      into strSchemaName,
           strTableName,
           strRollbackAction
      from _workflow.config_job_table
     where id = iConfigJobTableId;
     
    return _workflow.job_table_add(iJobId, strSchemaName, strTableName, strRollbackAction);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_table_add(iJobId int, strSchemaName text, strTableName text, strRollbackAction text) returns int as $$
declare
    iJobTableId int = nextval('_workflow.object_id_seq');
    iWorkflowId int = (select workflow_id from _workflow.job where id = iJobId);
begin
    insert into _workflow.job_table (workflow_id,
                                    job_id,
                                    id,
                                    schema_name,
                                    table_name,
                                    rollback_action)
                            values (iWorkflowId,
                                    iJobId,
                                    iJobTableId,
                                    strSchemaName,
                                    strTableName,
                                    strRollbackAction);

    return(iJobTableId);                                    
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_EVENT Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_event_add(iConfigJobId int, strState text, strSql text) returns int as $$
declare
    iConfigWorkflowId int;
    iConfigJobEventId int;
begin
    select config_workflow_id
      into iConfigWorkflowId
      from _workflow.config_job
     where id = iConfigJobId;

    select nextval('_workflow.config_id_seq')
      into iConfigJobEventId;

    insert into _workflow.config_job_event (config_workflow_id, config_job_id,                id,    state,    sql)
                                    values ( iConfigWorkflowID,  iConfigJobId, iConfigJobEventId, strState, strSql);

    return(iConfigJobEventId);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_event_add(iJobId int, iConfigJobEventId int) returns int as $$
declare
    strState text;
    strSql text;
begin
    select state,
           sql
      into strState,
           strSql
      from _workflow.config_job_event
     where id = iConfigJobEventId;
     
    return _workflow.job_event_add(iJobId, strState, strSql);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_event_add(iJobId int, strState text, strSql text) returns int as $$
declare
    iJobEventId int = nextval('_workflow.object_id_seq');
    iWorkflowId int = (select workflow_id from _workflow.job where id = iJobId);
begin
    insert into _workflow.job_event (workflow_id,
                                     job_id,
                                     id,
                                     state,
                                     sql)
                             values (iWorkflowId,
                                     iJobId,
                                     iJobEventId,
                                     strState,
                                     strSql);

    return(iJobEventId);                                    
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_VALIDATE Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_validate_add(iConfigJobId int, eType _workflow.job_validate_type,
                                                             eState _workflow.job_validate_state, 
                                                             eAction _workflow.job_validate_action, 
                                                             strName text, strSql text) returns int as $$
declare
    iConfigWorkflowId int;
    iConfigJobValidateId int;
    iOrdering int;
begin
    select config_workflow_id
      into iConfigWorkflowId
      from _workflow.config_job
     where id = iConfigJobId;
     
    select coalesce(max(ordering) + 1, 1)
      into iOrdering
      from _workflow.config_job_validate
     where config_job_id = iConfigJobId;

    select nextval('_workflow.config_id_seq') 
      into iConfigJobValidateId;

    insert into _workflow.config_job_validate (config_workflow_id, config_job_id,                   id,  type,  state,  action,  ordering,    name,    sql) 
                                       values ( iConfigWorkflowID,  iConfigJobId, iConfigJobValidateId, eType, eState, eAction, iOrdering, strName, strSql);

    return(iConfigJobValidateId);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_validate_add(iJobId int, iConfigJobValidateId int) returns int as $$
declare
    eType _workflow.job_validate_type;
    eState _workflow.job_validate_state;
    eAction _workflow.job_validate_action;
    strName text;
    strSql text;
begin
    select type,
           state,
           action,
           name,
           sql
      into eType,
           eState,
           eAction,
           strName,
           strSql
      from _workflow.config_job_validate
     where id = iConfigJobValidateId;
     
    return _workflow.job_validate_add(iJobId, eType, eState, eAction, strName, strSql);
end;
$$ language plpgsql security invoker;

create or replace function _workflow.job_validate_add(iJobId int, eType _workflow.job_validate_type, 
                                                      eState _workflow.job_validate_state, 
                                                      eAction _workflow.job_validate_action, 
                                                      strName text, strSql text) returns int as $$
declare
    iWorkflowId int = (select workflow_id from _workflow.job where id = iJobId);
    lKey bigint = (select key from _workflow.workflow where id = iWorkflowId);
    iJobValidateId int = nextval('_workflow.object_id_seq');
    iOrdering int;
begin
    select coalesce(max(ordering) + 1, 1)
      into iOrdering
      from _workflow.job_validate
     where job_id = iJobId;
     
    insert into _workflow.job_validate (workflow_id,
                                       job_id,
                                       id,
                                       type,
                                       state,
                                       action,
                                       ordering,
                                       name,
                                       sql)
                               values (iWorkflowId,
                                       iJobId,
                                       iJobValidateId,
                                       eType,
                                       eState,
                                       eAction,
                                       iOrdering,
                                       strName,
                                       case when lKey is null then strSql else replace(strSql, '%$KEY$%', lKey::text) end);

    return(iJobValidateId);
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_MAP Functions
***********************************************************************************************************************************/
create or replace function _workflow.config_job_map_add(iConfigJobId int, iConfigJobParentId int) returns void as $$
declare
    iConfigWorkflowId int;
begin
    select config_workflow_id
      into iConfigWorkflowId
      from _workflow.config_job
     where id = iConfigJobId;

    insert into _workflow.config_job_map (config_workflow_id,            id,          parent_id) 
                                  values ( iConfigWorkflowID,  iConfigJobId, iConfigJobParentId);
end;
$$ language plpgsql security invoker;

/***********************************************************************************************************************************
JOB_STATE_UPDATE Functions
***********************************************************************************************************************************/
create or replace function _workflow.job_state_update(iWorkflowId int, strJobName text, strState text) returns void as $$
begin
    update _workflow.job
       set state = strState
     where workflow_id = iWorkflowId
       and name = strJobName;
     
    if not found then
        raise exception 'Workflow %, Job "%" was not updated to "%" because it does not exist', iWorkflowId, strJobName, strState;
    end if;
end;
$$ language plpgsql security invoker;

do $$
begin
    execute 'grant execute on function _workflow.job_state_update(int, text, text) to ' || _utility.role_get('etl'); 
    execute 'grant execute on function _workflow.job_state_update(int, text, text) to ' || _utility.role_get('admin'); 
end $$;

/***********************************************************************************************************************************
JOB_STATE_UPDATE Functions
***********************************************************************************************************************************/
create or replace function _workflow.job_state_update(iJobId int, strState text) returns void as $$
begin
    perform _workflow.job_state_update(workflow_id, name, strState)
       from _workflow.job
      where id = iJobId;
end;
$$ language plpgsql security invoker;

do $$
begin
    execute 'grant execute on function _workflow.job_state_update(int, text) to ' || _utility.role_get('etl'); 
    execute 'grant execute on function _workflow.job_state_update(int, text) to ' || _utility.role_get('admin'); 
end $$;
