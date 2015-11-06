create or replace view _workflow.vw_job_internal as
select workflow.id as workflow_id, 
       workflow.name as workflow_name,
       workflow.key_field as workflow_key_field,
       workflow.key as workflow_key,
       job.id as job_id,
       job.name as job_name,
       job.state as job_state,
       job_log.retry_timestamp as job_retry_timestamp,
       job_log.run_id as job_run_id,
       job_log.id as job_log_id,
       job_log.log_time as job_log_time,
       job_log.user_name as job_log_user_name,
       job_log.detail as job_log_detail
  from _workflow.job, _workflow.workflow, _workflow.job_log
 where workflow.id = job.workflow_id
   and job_log.id = 
(
    select max(id)
      from _workflow.job_log
     where job_id = job.id
)   
 order by workflow.key, job.id;

create or replace view _workflow.vw_job as
select *
  from _workflow.vw_job_internal;

do $$ begin execute 'grant select on _workflow.vw_job to ' || _utility.role_get('etl'); end $$;

create or replace view _workflow.vw_job_queue_internal as
select * 
  from _workflow.vw_job_internal
 where (job_state = 'ready' or (job_state = 'error' and clock_timestamp() >= job_retry_timestamp))
   and _workflow.job_schedule(job_id) = true;

create or replace view _workflow.vw_job_queue as
select *
  from _workflow.vw_job_queue_internal;

do $$ begin execute 'grant select on _workflow.vw_job_queue to ' || _utility.role_get('etl'); end $$;
