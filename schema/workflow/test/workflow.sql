--rollback; begin; set role rn_ad; update _scd.account set comment = false; savepoint unit_test;
rollback to unit_test;

/***********************************************************************************************************************************
* WAIT_FOR_QUEUE Function
***********************************************************************************************************************************/
create or replace function pg_temp.wait_for_queue
(
    iJobId int,
    nWaitSeconds numeric default 10
)
    returns void as $$
declare
    tsStart timestamp with time zone = clock_timestamp();
begin
    loop
        exit when
        (
            select count(*) > 0
              from _workflow.vw_job_queue
             where job_id = iJobId
        );

        if clock_timestamp() - tsStart > (nWaitSeconds || ' seconds')::interval then
            raise exception 'Job % did not appear in queue within % seconds', iJobId, nWaitSeconds using errcode = 'TST01';
        end if;
        
        perform pg_sleep(.01);
    end loop;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* JOB_GET Function
***********************************************************************************************************************************/
create or replace function pg_temp.job_get
(
    iWorkflowId int,
    strJobName text
)
    returns int as $$
declare
    iJobId int;
begin
    -- Get the retry job and make sure it is valid
    select id
      into iJobId
      from _workflow.job
     where workflow_id = iWorkflowId
       and name = strJobName;

    if not found then
        raise exception 'Workflow %, job % not found', iWorkflowId, strJobName;
    end if;

    return iJobId;
end;
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* Test workflow
***********************************************************************************************************************************/
do $$
declare
    iConfigWorkflowId int;
    iConfigJobId int;

    iWorkflowId int;
    iJobId int;
begin
    /*******************************************************************************************************************************
    * Unit begin
    *******************************************************************************************************************************/
    perform _test.unit_begin('Workflow');
    
    /*******************************************************************************************************************************
    * Create a test workflow for retry
    *******************************************************************************************************************************/
    iConfigWorkflowId = _workflow.config_workflow_add('test.retry', 'id');

    -- Create steps to test retries
    iConfigJobID = _workflow.config_job_add(iConfigWorkflowId, 'job.retry', false, true);
    perform _workflow.config_job_retry_add(iConfigJobId, '.2 seconds', 1, 'none');
    perform _workflow.config_job_retry_add(iConfigJobId, '.2 seconds', 2, 'last');
    perform _workflow.config_job_retry_add(iConfigJobId, '.2 seconds', 1, 'each');

    perform _workflow.config_job_validate_add(iConfigJobID, 'object_list', 'error', 'fail', 'retry.validation', 'select 1 as id');

    -- Start the workflow
    iWorkflowId = _workflow.start('test.retry', 1);

    -- Get the retry job
    iJobId = pg_temp.job_get(iWorkflowId, 'job.retry');

    if not found then
        raise exception 'Workflow %, job % not found', iWorkflowId, iJobId;
    end if;

    -- Initial run
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    -- Should take a .2 seconds to retry, looking after .1 seconds should error out
    begin
        perform pg_temp.wait_for_queue(iJobId, .1);
    exception
        when sqlstate 'TST01' then
            null;
    end;

    -- retry group 1, repeat 1
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    perform pg_temp.wait_for_queue(iJobId, .3);

    -- retry group 2, repeat 1
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    perform _test.test_begin('Error reporting is off');

    if _workflow.job_report_error(iJobId) = false then
        perform _test.test_pass();
    else
        perform _test.test_fail('error reporting should be false');
    end if;

    perform pg_temp.wait_for_queue(iJobId, .3);

    -- retry group 2, repeat 2
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    perform _test.test_begin('Error reporting is on');

    if _workflow.job_report_error(iJobId) = true then
        perform _test.test_pass();
    else
        perform _test.test_fail('error reporting should be true');
    end if;

    perform pg_temp.wait_for_queue(iJobId, .3);

    -- retry group 3, repeat 1
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    -- The job should no longer be retried
    begin
        perform pg_temp.wait_for_queue(iJobId, .3);
    exception
        when sqlstate 'TST01' then
            null;
    end;

    -- Pause the job to reset retries
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'pause');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'ready');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'error');

    -- The job should be back in the retry queue
    perform pg_temp.wait_for_queue(iJobId, .3);

    -- Now complete the job and let validation fail - there should be no retry
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'running');
    perform _workflow.job_state_update(iWorkflowId, 'job.retry', 'complete');

    -- The job should now be in error state
    perform _test.test_begin('Validation failure does not retry');

    if
    (
        select count(*) = 1
          from _workflow.job
         where id = iJobId
           and state = 'error'
    ) then
        begin
            perform pg_temp.wait_for_queue(iJobId, .3);

            perform _test.test_fail('job should not be in queue');
        exception
            when sqlstate 'TST01' then
                perform _test.test_pass();
        end;
    else
        perform _test.test_fail('job should be in error state');
    end if;

    /*******************************************************************************************************************************
    * Create a test workflow for scheduling
    *******************************************************************************************************************************/
    iConfigWorkflowId = _workflow.config_workflow_add('test.schedule', 'id');

    -- Create steps
    perform _workflow.config_job_add(iConfigWorkflowId, 'job.schedule.expression_true', false, true,
                                     'select %$JOB_ID$% = %$JOB_ID$%');
    perform _workflow.config_job_add(iConfigWorkflowId, 'job.schedule.expression_false', false, true,
                                     'select clock_timestamp() < ''-infinity''');
    perform _workflow.config_job_add(iConfigWorkflowId, 'job.schedule.null', false, true);
    perform _workflow.config_job_add(iConfigWorkflowId, 'job.schedule.expression_indirect', false, true,
                                     'select ''select true''');

    -- Start the workflow
    iWorkflowId = _workflow.start('test.schedule', 1);

    -- Get the schedule expression true job and make sure it is valid
    perform _test.test_begin('Evaluate true schedule expression');

    iJobId = pg_temp.job_get(iWorkflowId, 'job.schedule.expression_true');

    if _workflow.job_schedule(iJobId) <> true then
        perform _test.test_fail('result was false but true was expected');
    else
        begin
            perform pg_temp.wait_for_queue(iJobId, 0);
            perform _test.test_pass();
        exception
            when sqlstate 'TST01' then
                perform _test.test_fail('job did not appear in queue');
        end;
    end if;

    -- Get the schedule expression false job and make sure it is valid
    perform _test.test_begin('Evaluate false schedule expression');

    iJobId = pg_temp.job_get(iWorkflowId, 'job.schedule.expression_false');

    if _workflow.job_schedule(iJobId) <> false then
        perform _test.test_fail('result was true but false was expected');
    else
        begin
            perform pg_temp.wait_for_queue(iJobId, 0);
            perform _test.test_fail('job appeared in queue');
        exception
            when sqlstate 'TST01' then
                perform _test.test_pass();
        end;
    end if;

    -- Get the schedule expression null job and make sure it is valid
    perform _test.test_begin('Evaluate null schedule expression');

    if _workflow.job_schedule(pg_temp.job_get(iWorkflowId, 'job.schedule.null')) <> true then
        perform _test.test_fail('result was false but true was expected');
    else
        perform _test.test_pass();
    end if;

    -- Get the schedule expression indirect job and make sure it is valid
    perform _test.test_begin('Evaluate indirect schedule expression');

    if _workflow.job_schedule(pg_temp.job_get(iWorkflowId, 'job.schedule.expression_indirect')) <> true then
        perform _test.test_fail('result was false but true was expected');
    else
        perform _test.test_pass();
    end if;

    /*******************************************************************************************************************************
    * Unit end
    *******************************************************************************************************************************/
    perform _test.unit_end();
end $$;
