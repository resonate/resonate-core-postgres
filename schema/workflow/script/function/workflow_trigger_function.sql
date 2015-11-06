/***********************************************************************************************************************************
WORKFLOW Trigger Functions
***********************************************************************************************************************************/

/***********************************************************************************************************************************
CONFIG_JOB_TABLE Insert Trigger

Once a table has been added to a workflow it should be limited by the _workflow.
***********************************************************************************************************************************/
create or replace function _workflow.configjobtable_trigger_insert_after() returns trigger as $$
begin
    if
    (
        select count(*) = 0
          from _workflow.config_job_table
         where schema_name = new.schema_name
           and table_name = new.table_name
           and id <> new.id
    )
        and
    (
        select count(*) = 0
          from _workflow.job_table
         where schema_name = new.schema_name
           and table_name = new.table_name
    ) then
        execute _workflow.table_trigger_create(true, new.schema_name, new.table_name);
    else
        execute _workflow.table_trigger_create(false, new.schema_name, new.table_name);
    end if;

    return new;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
CONFIG_JOB_TABLE Delete Trigger

When a workflow config table is deleted make sure that the triggers are deleted unless they are used in a workflow intance.
***********************************************************************************************************************************/
create or replace function _workflow.configjobtable_trigger_delete_before() returns trigger as $$
begin
    if
    (
        select count(*) = 0
          from _workflow.config_job_table
         where schema_name = old.schema_name
           and table_name = old.table_name
           and id <> old.id
    )
        and
    (
        select count(*) = 0
          from _workflow.job_table
         where schema_name = old.schema_name
           and table_name = old.table_name
    ) then
        execute _workflow.table_trigger_drop(old.schema_name, old.table_name);
    else
        execute _workflow.table_trigger_create(false, old.schema_name, old.table_name);
    end if;

    return old;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
WORKFLOW Delete Trigger

When a workflow is deleted check to make sure that all jobs have been rolled back.
***********************************************************************************************************************************/
create or replace function _workflow.workflow_trigger_delete_before() returns trigger as $$
begin
    if 
    (
        select count(*) > 0
          from _workflow.job
         where workflow_id = old.id
           and state not in ('pending', 'ready')
    ) then
        raise exception 'Cannot delete a workflow that is not fully rolled back';
    end if;

    return old;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
JOB_TABLE Delete Trigger

When a workflow table is deleted make sure that the triggers are deleted unless they are used in a workflow instance or config.
***********************************************************************************************************************************/
create or replace function _workflow.jobtable_trigger_delete_before() returns trigger as $$
begin
    if
    (
        select count(*) = 0
          from _workflow.config_job_table
         where schema_name = old.schema_name
           and table_name = old.table_name
    )
        and
    (
        select count(*) = 0
          from _workflow.job_table
         where schema_name = old.schema_name
           and table_name = old.table_name
           and id <> old.id
    ) then
        execute _workflow.table_trigger_drop(old.schema_name, old.table_name);
    else
        execute _workflow.table_trigger_create(false, old.schema_name, old.table_name);
    end if;

    return old;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
WORKFLOW Insert Trigger

Make sure no duplicate workflows are created when key fields are null.
***********************************************************************************************************************************/
create or replace function _workflow.workflow_trigger_insert_after() returns trigger as $$
begin
    if 
    (
        select count(*)
          from _workflow.workflow
         group by name, key_field, key
        having count(*) > 1
    ) > 0 then
        raise exception 'Duplicate workflow with null keys inserted';
    end if;

    return new;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
WORKFLOW Update Trigger

Make sure no duplicate workflows are created when key fields are null.
***********************************************************************************************************************************/
create or replace function _workflow.workflow_trigger_update_before() returns trigger as $$
begin
    if old.locked = false and new.locked = true and new.state <> 'complete' then
        raise exception 'Workflow ''%'' (%) must be completed before it can be locked', new.name, new.id;
    end if;

    return new;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
JOB Insert Trigger

Make sure the initial log entry is created.
***********************************************************************************************************************************/
create or replace function _workflow.job_trigger_insert_after() returns trigger as $$
begin
    execute _workflow.job_log(new.id, new.state, session_user, null, false);

    return new;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
JOB Update Trigger

Allow the user to modify only the state field.
***********************************************************************************************************************************/
create or replace function _workflow.job_trigger_update_after() returns trigger as $$
begin
    if old.locked = false and new.locked = true and new.state <> 'complete' then
        raise exception 'Job ''%'' (%) must be completed before it can be locked', new.name, new.id;
    end if;

    if new.state is distinct from old.state then
        if new.locked = true or
        (
            select count(*) <> 0
              from _workflow.workflow
             where id = new.workflow_id
               and locked = true
        ) then
            raise exception 'Job ''%'' (%) [or workflow] is locked', new.name, new.id;
        end if;
    
        if current_user <> _utility.role_get('') and
           (old.state = 'pending' or
            old.state = 'ready' and new.state not in ('running', 'error', 'pause') or
            old.state = 'running' and new.state not in ('error', 'complete', 'ready', 'pause') or
            old.state = 'error' and new.state not in ('complete', 'ready', 'pause') or
            old.state = 'complete' and new.state not in ('ready')) then
            raise exception 'Invalid state change ''%'' => ''%'' for job ''%'' (%) attempted by %', old.state, new.state, new.name, new.id, current_user;
        end if;

        execute _workflow.job_trigger_update_internal(new.id, old.state, new.state, current_user);
    end if;

    return new;
end
$$ language plpgsql security invoker;

create or replace function _workflow.job_trigger_update_internal(iNewId int, strOldState text, strNewState text, strUserName text) returns void as $$
declare
    xJobTableResult record;
    iWorkflowId int;
    strCurrentState text;
    bProcessRetry boolean;
begin
    lock table _workflow.job in share row exclusive mode;

    if strNewState in ('ready', 'pending') and strOldState not in ('ready', 'pending') then
        execute _workflow.state_rollback(iNewId, strOldState);
    else
        -- If complete then run validations and update job states
        if strNewState = 'complete' then
            begin
                create temp table temp_jobtriggerupdateinternal_complete
                (
                    job_id int
                );
            exception
                when duplicate_table then
                    null;
            end;

            insert into temp_jobtriggerupdateinternal_complete (job_id) values (iNewId);

            execute _workflow.job_validate(iNewId, strOldState);
            execute _workflow.state_update(iNewId);

            delete from temp_jobtriggerupdateinternal_complete
             where job_id = iNewId;

        -- If error then check retries
        elsif strNewState = 'error' then
            begin
                select count(*) = 0
                  into bProcessRetry
                  from temp_jobtriggerupdateinternal_complete
                 where job_id = iNewId;
            exception
                when undefined_table then
                    bProcessRetry = true;
            end;

            if bProcessRetry then
                execute _workflow.job_retry(iNewId);
            else
                execute _workflow.job_log(iNewId, strNewState, strUserName, null, false);
            end if;

        -- Else log the state change
        else
            execute _workflow.job_log(iNewId, strNewState, strUserName, null, false);
        end if;
    end if;

    for xJobTableResult in
        select schema_name,
               table_name
          from _workflow.job_table
         where job_id = iNewId
    loop
        execute _workflow.table_trigger_create(false, xJobTableResult.schema_name, xJobTableResult.table_name);
    end loop;

    -- Update workflow state.
    -- Look over the jobs for this workflow, determine if any are error, running, etc, and set the workflow state appropriately.
    -- Note that we won't set the workflow state if it's already set to that state, to avoid firing any triggers unncesarily.
    select workflow_id
      into iWorkflowId
      from _workflow.job
     where id = iNewId;

    select state 
    into strCurrentState
    from _workflow.workflow
    where id = iWorkflowId;
    
    if  
    (
        select count(*)
          from _workflow.job
         where workflow_id = iWorkflowId
           and state <> 'complete'
    ) = 0 then
        if strCurrentState != 'complete' then
            update _workflow.workflow
               set state = 'complete'
             where id = iWorkflowId;
        end if;
    else 
        if
        (
            select count(*)
              from _workflow.job
             where workflow_id = iWorkflowId
               and state = 'error'
        ) <> 0 then
            if strCurrentState != 'error' then
                update _workflow.workflow
                   set state = 'error'
                 where id = iWorkflowId;
            end if;
        else 
            if
            (
                select count(*)
                  from _workflow.job
                 where workflow_id = iWorkflowId
                   and state in ('complete', 'running')
            ) <> 0 then
                if strCurrentState != 'running' then
                    update _workflow.workflow
                       set state = 'running'
                     where id = iWorkflowId;
                end if;
            else
                if strCurrentState != 'ready' then
                    update _workflow.workflow
                       set state = 'ready'
                     where id = iWorkflowId;
                end if;
            end if;
        end if;
    end if;

    perform _workflow.job_event_run(iNewId, strNewState);
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _workflow.job_trigger_update_internal(int, text, text, text) to ' || _utility.role_get('etl'); 
    execute 'grant execute on function _workflow.job_trigger_update_internal(int, text, text, text) to ' || _utility.role_get('admin'); 
end $$;
