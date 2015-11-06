/***********************************************************************************************************************************
WORKFLOW Schema Table
***********************************************************************************************************************************/

/***********************************************************************************************************************************
CONFIG_WORKFLOW Table
***********************************************************************************************************************************/
create table _workflow.config_workflow
(
    id int not null,
    name text not null,
    key_field text,
    schedule text,

    constraint configworkflow_pk
        primary key (id),
    constraint configworkflow_name_unq
        unique (name)
);

--- INDEX/CLUSTER ------------------------------------------------
alter table _workflow.config_workflow
    cluster on configworkflow_name_unq;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_workflow is
'Contains master workflow configurations.  All the tables that start with {{config_}} ultimately refer to a row in
this table';

comment on column _workflow.config_workflow.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_workflow.name is
'Unique name for the workflow.';
comment on column _workflow.config_workflow.key_field is
'Key field defining the name of the table column that contains the workflow key.  This is used during workflow to
write triggers and determinate which rows should be deleted during rollbacks.  Key field is only used if tables
are defined in config_job_table but is not required.';
comment on column _workflow.config_workflow.schedule is
'Schedule is an expression (query, function, etc) that determines whether the worflow should be run or not.  It 
is expected to return a boolean.  Schedule expression is evaluated in vw_job_queue to determine if a job should
appear in the queue.';

/***********************************************************************************************************************************
CONFIG_JOB Table
***********************************************************************************************************************************/
create table _workflow.config_job
(
    config_workflow_id int not null
        constraint configjob_configworkflowid_fk references _workflow.config_workflow (id) on delete cascade,
    id int not null,
    name text not null,
    serialize boolean not null,
    rollback_allow boolean not null,
    schedule text,

    constraint configjob_pk
        primary key (id),
    constraint configjob_configworkflowid_name_unq
        unique (config_workflow_id, name),
    constraint configjob_configworkflowid_id_unq
        unique (config_workflow_id, id)
);

--- INDEX/CLUSTER ------------------------------------------------
alter table _workflow.config_job
    cluster on configjob_configworkflowid_name_unq;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job is
'Contains the jobs that make up a workflow.';

comment on column _workflow.config_job.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_job.name is
'Job name, must be unique per {{config_workflow_id}}.';
comment on column _workflow.config_job.serialize is
'If serialize is set then a job may not be run until the same job in the previous workflow has been completed.
The previous workflow is specified in {{_workflow.workflow.key_serialize}} and only one can be set.';
comment on column _workflow.config_job.rollback_allow is
'Can this job be rolled back after it has been completed?';
comment on column _workflow.config_job.schedule is
'Schedule is an expression (query, function, etc) that determines whether the job should be run or not.  It 
is expected to return a boolean.  Schedule expression is evaluated in vw_job_queue to determine if a job should
appear in the queue.  If this column is not null it overrides the value in {{_workflow.config_workflow.schedule}}.';

/***********************************************************************************************************************************
CONFIG_JOB_RETRY Table
***********************************************************************************************************************************/
create table _workflow.config_job_retry
(
    config_workflow_id int not null,
    config_job_id int not null,
    id int not null,
    interval interval not null,
    repeat int not null
        constraint configjobretry_repeat_ck
            check (repeat >= 1),
    error text not null
        constraint configjobretry_error_ck
            check (error in ('none', 'each', 'last')),

    constraint configjobretry_pk
        primary key (id),

    constraint configjobretry_configworkflowid_configjobid_id_unq
        unique (config_workflow_id, config_job_id, id),

    constraint configjobretry_configworkflowid_configjobid_fk
        foreign key (config_workflow_id, config_job_id) 
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
alter table _workflow.config_job_retry
    cluster on configjobretry_configworkflowid_configjobid_id_unq;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job_retry is
'Contains the retry information for a job.  More than one type of retry can be specified and they will be run in
order.  ETL should call _workflow.job_report_error() and after a job has been set to error to see if it should report the error.';

comment on column _workflow.config_job_retry.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job_retry.config_job_id is
'References {{_workflow.config_job.id}}.';
comment on column _workflow.config_job_retry.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_job_retry.interval is
'Interval to wait after an error before retrying.';
comment on column _workflow.config_job_retry.repeat is
'Number of times to retry.';
comment on column _workflow.config_job_retry.error is
'When to return an error:
<br/><br/>
{{none}} - do not error<br/>
{{each}} - error on each retry<br/>
{{last}} - error on last retry';

/***********************************************************************************************************************************
CONFIG_JOB_EVENT Table
***********************************************************************************************************************************/
create table _workflow.config_job_event
(
    config_workflow_id int not null,
    config_job_id int not null,
    id int not null,
    state text not null
        constraint configjobevent_state_ck
            check (state in ('complete', 'running', 'rollback', 'ready', 'error')),
    sql text not null,

    constraint configjobevent_pk
        primary key (id),

    constraint configjobevent_configworkflowid_configjobid_id_unq
        unique (config_workflow_id, config_job_id, id),

    constraint configjobevent_configworkflowid_configjobid_fk
        foreign key (config_workflow_id, config_job_id)
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
alter table _workflow.config_job_event
    cluster on configjobevent_configworkflowid_configjobid_id_unq;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job_event is
'Contain the events that get fired when a job enters a certain state.';

comment on column _workflow.config_job_event.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job_event.config_job_id is
'References {{_workflow.config_job.id}}.';
comment on column _workflow.config_job_event.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_job_event.state is
'State that the job has to enter before the event is fired.';
comment on column _workflow.config_job_event.sql is
'Function to be fired.  This should be the function name which can optionally have one parameter, %$KEY$%, which will
be replaced with the workflow key.  For example:
<br/><br/>
{{perform rollback_event()}}<br/>
    or<br/>
{{perform complete_event(%$KEY$%)}}';

/***********************************************************************************************************************************
CONFIG_JOB_VALIDATE Table
***********************************************************************************************************************************/
create table _workflow.config_job_validate
(
    config_workflow_id int not null,
    config_job_id int not null,
    id int not null,
    type text not null
        constraint configjobvalidate_type_ck
            check (type in ('object_list')),
    state text not null
        constraint configjobvalidate_state_ck
            check (state in ('error', 'drop', 'warning')),
    action text not null
        constraint configjobvalidate_action_ck
            check (action in ('pass', 'fail') and (state = 'error' or action = 'pass')),
    ordering int not null,
    name text not null,
    sql text not null,

    constraint configjobvalidate_pk
        primary key (id),

    constraint configjobvalidate_configjobid_sql_unq
        unique (config_job_id, sql),
    constraint configjobvalidate_configjobid_name_unq
        unique (config_job_id, name),
    constraint configjobvalidate_configjobid_ordering_unq
        unique (config_job_id, ordering) deferrable initially deferred,

    constraint configjobvalidate_configworkflowid_configjobid_fk
        foreign key (config_workflow_id, config_job_id)
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create index configjobvalidate_configworkflowid_configjobid_idx
    on _workflow.config_job_validate (config_workflow_id, config_job_id);

alter table _workflow.config_job_validate
    cluster on configjobvalidate_configworkflowid_configjobid_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job_validate is
'Contains the validation queries for the job.  Validation queries are used to indicate whether what state a row is in
(error, drop, pass) and whether the process should continue (pass, fail).  Valid combinations of state/action are:
<br/><br/>
{{error/pass}} - error the row but allow the process to continue<br/>
{{error/fail}} - error the row and raise a exception so the process stops<br/>
{{drop/pass}} - mark the row as dropped and allow the process to continue<br/>
{{warning/pass}} - mark the row as warning and allow the process to continue
<br/><br/>
No modifications are made to the original table - all validations are stored in the {{_workflow.job_validate}} table and must
be queried.';

comment on column _workflow.config_job_validate.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job_validate.config_job_id is
'References {{_workflow.config_job.id}}.';
comment on column _workflow.config_job_validate.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_job_validate.type is
'Defines the validation type:
<br/><br/>
{{object_list}} - a list of objects that match the validation is returned by the query in the {{sql}} column.';
comment on column _workflow.config_job_validate.state is
'Defines the state to label each object:
<br/><br/>
{{error}} - the row is in error and should not be included in further processing<br/>
{{drop}} - the row has been dropped and should not be included in further processing<br/>
{{warning}} - the row has a warning but should be included for further processing';
comment on column _workflow.config_job_validate.action is
'Action to take on the object:
<br/><br/>
{{fail}} - only valid for the {{error}} state, raise an exception which will stop processing<br/>
{{pass}} - valid for any state, continue without raising an exception but exclude the row from further processing based on
the {{state}} rules';
comment on column _workflow.config_job_validate.ordering is
'Order to run the validations.';
comment on column _workflow.config_job_validate.name is
'Name of the validation, must be unique per {{config_job_id}}.';
comment on column _workflow.config_job_validate.sql is
'Contains the sql that performs the validation based on the value in the {{type}} column:
<br/><br/>
{{object_list}} - query returns a single column (id bigint) that identifies objects that match the validation';

/***********************************************************************************************************************************
CONFIG_JOB_TABLE Table
***********************************************************************************************************************************/
create table _workflow.config_job_table
(
    config_workflow_id int not null,
    config_job_id int not null,
    id int not null,
    schema_name text not null,
    table_name text not null,
    rollback_action text not null
        constraint configjobtable_rollbackaction_ck
            check (rollback_action in ('delete', 'preserve')),

    constraint configjobtable_pk
        primary key (id),

    constraint configjobtable_configworkflowid_schemaname_tablename_unq
        unique (config_workflow_id, schema_name, table_name),

    constraint configjobtable_configworkflowid_configjobid_fk
        foreign key (config_workflow_id, config_job_id) 
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create index configjobtable_configworkflowid_configjobid_idx
    on _workflow.config_job_table (config_workflow_id, config_job_id);

alter table _workflow.config_job_table
    cluster on configjobtable_configworkflowid_configjobid_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job_table is
'Define tables that are updated in a workflow job.  If {{_workflow.config_worklow.key_field}} is specified, then a trigger
will be added to the table that will prevent modification when the job is not running.  In addition, rows will be deleted or
truncated from the table based on the workflow key.  Deleting is the default, but truncates will occur if the table is partitioned
on the column in {{_workflow.config_worklow.key_field}}.';

comment on column _workflow.config_job_table.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job_table.config_job_id is
'References {{_workflow.config_job.id}}.';
comment on column _workflow.config_job_table.id is
'Synthetic primary key, assigned from {{_workflow.config_id_seq}}.';
comment on column _workflow.config_job_table.schema_name is
'Schema name.';
comment on column _workflow.config_job_table.table_name is
'Table name.';
comment on column _workflow.config_job_table.rollback_action is
'What will be done with rows associated with the workflow key:
<br/><br/>
{{delete}} - delete or truncate the rows
{{preserve}} - preserve the rows';

/***********************************************************************************************************************************
CONFIG_JOB_MAP Table
***********************************************************************************************************************************/
create table _workflow.config_job_map
(
    config_workflow_id int not null,
    id int not null,
    parent_id int not null
        constraint configjobmap_parentid_ck
            check (parent_id <> id),

    constraint configjobmap_pk
        primary key (id, parent_id),

    constraint configjobmap_configworkflowid_id_fk
        foreign key (config_workflow_id, id) 
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade,
    constraint configjobmap_configworkflowid_parentid_fk
        foreign key (config_workflow_id, parent_id) 
        references _workflow.config_job (config_workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create unique index configjobmap_parentid_id_unq
    on _workflow.config_job_map (parent_id, id);

create index configjobmap_configworkflowid_id_idx
    on _workflow.config_job_map (config_workflow_id, id);

create index configjobmap_configworkflowid_parentid_idx
    on _workflow.config_job_map (config_workflow_id, parent_id);

alter table _workflow.config_job_map
    cluster on configjobmap_configworkflowid_id_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.config_job_map is
'Contains relationships between jobs in a workflow.  A workflow is a state machine, so all the parents of a job must be complete
before a job will be ready to run.  If a job is rolled back, all decendents will be rolled back as well.';

comment on column _workflow.config_job_map.config_workflow_id is
'References {{_workflow.config_workflow.id}}.';
comment on column _workflow.config_job_map.id is
'References {{_workflow.config_job.id}}.';
comment on column _workflow.config_job_map.parent_id is
'References {{_workflow.config_job.id}} and defines the parent of the job in the {{id}} column.';

/***********************************************************************************************************************************
WORKFLOW Table
***********************************************************************************************************************************/
do $$
begin
    create table _workflow.workflow
    (
        id int not null,
        name text not null,
        key_field text,
        key bigint
            constraint workflow_key_ck
                check ((key is not null and key_field is not null) or (key is null and key_field is null)),
        key_serialize bigint
            constraint workflow_keyserialize_ck
                check ((key is null and key_serialize is null) or (key is not null)),
        state text not null default 'ready'
            constraint workflow_state_ck
                check (state in ('ready', 'running', 'error', 'complete')),
        locked boolean not null default false,

        constraint workflow_pk
            primary key (id),

        constraint workflow_name_keyfield_key_unq
            unique (name, key_field, key),

        constraint workflow_name_keyfield_keyserialize_fk
            foreign key (name, key_field, key_serialize)
            references _workflow.workflow (name, key_field, key)
    );

    --- INDEX/CLUSTER ------------------------------------------------
    create unique index workflow_name_keyfield_keyserialize_unq
        on _workflow.workflow (name, key_field, key_serialize);

    alter table _workflow.workflow
        cluster on workflow_name_keyfield_key_unq;

    --- PERMISSION ---------------------------------------------------
    execute '
        grant select
           on _workflow.workflow
           to ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on table _workflow.workflow is
'Contains an instance of a workflow as defined by {{_workflow.config_workflow}}.';

comment on column _workflow.workflow.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.workflow.name is
'See {{_workflow.config_workflow.name}}.';
comment on column _workflow.workflow.key is
'ID for the object that the workflow is processing.  This value should be found if the {{key_field}} column of any tables that are
involved in the workflow.';
comment on column _workflow.workflow.key_serialize is
'Workflow key that this workflow should be serialized with.  This affects jobs that have the serialize flag set to true - see
{{_workflow.job.serialize}}';
comment on column _workflow.workflow.state is
'Current state of the workflow:
<br/><br/>
{{ready}} - workflow is ready to run<br/>
{{running}} - workflow is in progress (at least one job has started running)<br/>
{{error}} - at least one job is in the error state<br/>
{{complete}} - all jobs are complete';
comment on column _workflow.workflow.locked is
'Workflow is locked and no further state changes are allowed.  Only completed workflows can be locked.';

/***********************************************************************************************************************************
JOB Table
***********************************************************************************************************************************/
do $$
begin
    create table _workflow.job
    (
        workflow_id int not null
            constraint job_workflowid_fk
                references _workflow.workflow (id)
                on delete cascade,
        id int not null,
        name text not null,
        serialize boolean not null,
        rollback_allow boolean not null,
        state text not null
            constraint job_state_ck
                check (state in ('pending', 'ready', 'running', 'error', 'pause', 'complete')),
        locked boolean not null default false,

        constraint job_pk
            primary key (id),

        constraint job_workflowid_name_unq
            unique (workflow_id, name),
        constraint job_workflowid_id_unq
            unique (workflow_id, id)
    );

    --- INDEX/CLUSTER ------------------------------------------------
    create index job_workflowid_id_idx
        on _workflow.job (workflow_id, id);

    create index job_state_id_idx
        on _workflow.job (state, id);

    alter table _workflow.job
        cluster on job_workflowid_id_unq;

    --- PERMISSION ---------------------------------------------------
    execute '
        grant select,
              update (state)
           on _workflow.job to ' || _utility.role_get('admin') || ', ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job is
'Contains an instance of a job as defined by {{_workflow.config_job}}.';

comment on column _workflow.job.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.job.name is
'See {{_workflow.config_job.name}}.';
comment on column _workflow.job.serialize is
'See {{_workflow.config_job.serialize}}.';
comment on column _workflow.job.rollback_allow is
'See {{_workflow.config_job.rollback_allow}}.';
comment on column _workflow.job.state is
'Current state of the job:
<br/><br/>
{{pending}} - parent jobs not complete, cannot be run<br/>
{{ready}} - ready to run<br/>
{{running}} - running<br/>
{{error}} - in error state, no dependent jobs can be processed<br/>
{{pause}} - in pause state, no dependent jobs can be processed (used to park jobs that cannot be completed yet)<br/>
{{complete}} - job is complete';
comment on column _workflow.job.locked is
'Job is locked and no further state changes are allowed.  Only completed jobs can be locked.';

/***********************************************************************************************************************************
JOB_LOG Table
***********************************************************************************************************************************/
do $$
begin
    create table _workflow.job_log
    (
        workflow_id int not null,
        job_id int not null,
        id int not null,
        run_id int not null,
        log_time timestamp without time zone default clock_timestamp() not null,
        state text not null
            constraint joblog_state_ck
                check (state in ('pending', 'ready', 'running', 'error', 'pause', 'complete', 'rollback', 'validate')),
        retry_group int,
        retry_repeat int,
        retry_timestamp timestamp with time zone,
        report_error boolean not null default false
            constraint joblog_reporterror_ck
                check (report_error = false or state = 'error'),
        user_name text not null,
        detail text,

        constraint joblog_pk primary key (id),

        constraint joblog_workflowid_jobid_id_unq
            unique (workflow_id, job_id, id),

        constraint joblog_workflowid_jobid_fk
            foreign key (workflow_id, job_id) 
            references _workflow.job (workflow_id, id)
            on delete cascade
    );

    --- INDEX/CLUSTER ------------------------------------------------
    create index joblog_jobid_id_idx
        on _workflow.job_log (job_id, id);

    alter table _workflow.job_log
        cluster on joblog_workflowid_jobid_id_unq;

    --- PERMISSION ---------------------------------------------------
    execute '
        grant select
           on _workflow.job_log
           to ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_log is
'Contains a log entry for each job state change.';

comment on column _workflow.job_log.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job_log.job_id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_log.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.job_log.run_id is
'The run_id starts at 1 and is incremented each time the job is rolled back to pending/ready.';
comment on column _workflow.job_log.log_time is
'Timestamp of the log entry.';
comment on column _workflow.job_log.state is
'State that the job transitioned to as described in {{_workflow.job.state}}.  There are also two special states that only exist in
the log:
<br/><br/>
{{rollback}} - a rollback was requested<br/>
{{validate}} - validations were run';
comment on column _workflow.job_log.retry_group is
'If retries are enabled, the retry group that was in effect for the state change.';
comment on column _workflow.job_log.retry_repeat is
'If retries are enabled, how many repeats have happened in the group.';
comment on column _workflow.job_log.retry_timestamp is
'If retries are enabled, when the job can next be retried.';
comment on column _workflow.job_log.report_error is
'Determines if an error should be reported, always true if retries are disabled, else based on the retry settings.  ETL should call
_workflow.job_report_error() after a job has been set to error to see if it should report the error (rather than reading this
column).';
comment on column _workflow.job_log.user_name is
'Database user that changed the job state.';
comment on column _workflow.job_log.detail is
'Additional detail about the state change.';

/***********************************************************************************************************************************
JOB_VALIDATE Table
***********************************************************************************************************************************/
do $$
begin
    create table _workflow.job_validate
    (
        workflow_id int not null,
        job_id int not null,
        id int not null,
        type text not null
            constraint jobvalidate_type_ck
                check (type in ('object_list')),
        state text not null
            constraint jobvalidate_state_ck
                check (state in ('error', 'drop', 'warning')),
        action text not null
            constraint jobvalidate_action_ck
                check (action in ('pass', 'fail') and (state = 'error' or action = 'pass')),
        ordering int not null,
        name text not null,
        sql text not null,

        constraint jobvalidate_pk
            primary key (id),

        constraint jobvalidate_jobid_sql_unq
            unique (job_id, sql),
        constraint jobvalidate_jobid_name_unq
            unique (job_id, name),
        constraint jobvalidate_jobid_ordering_unq
            unique (job_id, ordering) deferrable initially deferred,
        constraint jobvalidate_workflowid_jobid_id_unq
            unique (workflow_id, job_id, id),

        constraint jobvalidate_workflowid_jobid_fk
            foreign key (workflow_id, job_id) 
            references _workflow.job (workflow_id, id)
            on delete cascade
    );


    --- INDEX/CLUSTER ------------------------------------------------
    alter table _workflow.job_validate
        cluster on jobvalidate_workflowid_jobid_id_unq;

    --- PERMISSION ---------------------------------------------------
    execute '
        grant select
           on _workflow.job_validate
           to ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_validate is
'Contains instances of job validations as defined in {{_workflow.config_job_validate}}.';

comment on column _workflow.job_validate.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job_validate.job_id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_validate.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.job_validate.type is
'See {{_workflow.config_job_validate.type}}.';
comment on column _workflow.job_validate.state is
'See {{_workflow.config_job_validate.state}}.';
comment on column _workflow.job_validate.action is
'See {{_workflow.config_job_validate.action}}.';
comment on column _workflow.job_validate.ordering is
'See {{_workflow.config_job_validate.ordering}}.';
comment on column _workflow.job_validate.name is
'See {{_workflow.config_job_validate.name}}.';
comment on column _workflow.job_validate.sql is
'See {{_workflow.config_job_validate.sql}}.';

/***********************************************************************************************************************************
JOB_VALIDATE_RESULT Table

David's Notes:
- Not sure why object_id is nullable - it certainly does not fit current usage.
***********************************************************************************************************************************/
do $$
begin
    create table _workflow.job_validate_result
    (
        job_id int not null
            constraint jobvalidateresult_jobid_fk
                references _workflow.job (id)
                on delete cascade,
        job_validate_id int not null,
        object_id bigint,
        exclude boolean not null,

        constraint jobvalidateresult_pk
            primary key (job_id, job_validate_id, object_id)
    );

    --- INDEX/CLUSTER ------------------------------------------------
    create index jobvalidateresult_exclude_objectid_idx
        on _workflow.job_validate_result (exclude, object_id);

    alter table _workflow.job_validate_result
        cluster on jobvalidateresult_exclude_objectid_idx;

    --- PERMISSION ---------------------------------------------------
    execute '
        grant select
           on _workflow.job_validate_result
           to ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_validate_result is
'Contains the results of job validation.  Each object_id that was returned from a validation has a row that determines whether it
should be exluded from further processing.';

comment on column _workflow.job_validate_result.job_id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_validate_result.job_validate_id is
'References {{_workflow.job_validate_result.id}}.';
comment on column _workflow.job_validate_result.object_id is
'Object returned by the validate.';
comment on column _workflow.job_validate_result.exclude is
'True if the object should be excluded from further processing.';

/***********************************************************************************************************************************
JOB_TABLE Table
***********************************************************************************************************************************/
create table _workflow.job_table
(
    workflow_id int not null,
    job_id int not null,
    id int not null,
    schema_name text not null,
    table_name text not null,
    rollback_action text not null
        constraint jobtable_rollbackaction_ck
            check (rollback_action in ('delete', 'preserve')),

    constraint jobtable_pk
        primary key (id),

    constraint jobtable_workflowid_schemaname_tablename_unq
        unique (workflow_id, schema_name, table_name),

    constraint jobtable_workflowid_jobid_fk
        foreign key (workflow_id, job_id) 
        references _workflow.job (workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create index jobtable_workflowid_jobid_idx
    on _workflow.job_table (workflow_id, job_id);

alter table _workflow.job_table
    cluster on jobtable_workflowid_jobid_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_table is
'Contains instances of job tables as defined in {{_workflow.config_job_table}}.';

comment on column _workflow.job_table.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job_table.job_id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_table.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.job_table.schema_name is
'See {{_workflow.config_job_table.schema_name}}.';
comment on column _workflow.job_table.table_name is
'See {{_workflow.config_job_table.table_name}}.';
comment on column _workflow.job_table.rollback_action is
'See {{_workflow.config_job_table.rollback_action}}.';

/***********************************************************************************************************************************
JOB_EVENT Table
***********************************************************************************************************************************/
create table _workflow.job_event
(
    workflow_id int not null,
    job_id int not null,
    id int not null,
    state text not null
        constraint jobevent_state_ck
            check (state in ('complete', 'running', 'rollback', 'ready', 'error')),
    sql text not null,

    constraint jobevent_pk
        primary key (id),

    constraint jobevent_workflowid_jobid_fk
        foreign key (workflow_id, job_id) 
        references _workflow.job (workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create index jobevent_workflowid_jobid_idx
    on _workflow.job_event (workflow_id, job_id);

alter table _workflow.job_event
    cluster on jobevent_workflowid_jobid_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_event is
'Contains instances of job events as defined in {{_workflow.config_job_event}}.';

comment on column _workflow.job_event.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job_event.job_id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_event.id is
'Synthetic primary key, assigned from {{_workflow.object_id_seq}}.';
comment on column _workflow.job_event.state is
'See {{_workflow.config_job_event.state}}.';
comment on column _workflow.job_event.sql is
'See {{_workflow.config_job_event.sql}}.';

/***********************************************************************************************************************************
JOB_MAP Table

David's Notes:
- The jobmap_parentid_id_unq index should be dropped and the jobmap_workflowid_parentid_idx should have its order changed to
  (parent_id, workflow_id).
***********************************************************************************************************************************/
create table _workflow.job_map
(
    workflow_id int not null,
    id int not null,
    parent_id int not null
        constraint jobmap_parentid_ck
            check (parent_id <> id),

    constraint jobmap_pk
        primary key (id, parent_id),

    constraint jobmap_workflowid_id_fk
        foreign key (workflow_id, id) 
        references _workflow.job (workflow_id, id)
        on delete cascade,
    constraint jobmap_workflowid_parentid_fk
        foreign key (workflow_id, parent_id) 
        references _workflow.job (workflow_id, id)
        on delete cascade
);

--- INDEX/CLUSTER ------------------------------------------------
create unique index jobmap_parentid_id_unq
    on _workflow.job_map (parent_id, id);

create index jobmap_workflowid_id_idx
    on _workflow.job_map (workflow_id, id);

create index jobmap_workflowid_parentid_idx
    on _workflow.job_map (workflow_id, parent_id);

alter table _workflow.job_map
    cluster on jobmap_workflowid_parentid_idx;

--- COMMENT ------------------------------------------------------
comment on table _workflow.job_map is
'Contains instances of job maps as defined in {{_workflow.config_job_map}}.';

comment on column _workflow.job_map.workflow_id is
'References {{_workflow.workflow.id}}.';
comment on column _workflow.job_map.id is
'References {{_workflow.job.id}}.';
comment on column _workflow.job_map.parent_id is
'See {{_workflow.config_job_map.parent_id}}.';
