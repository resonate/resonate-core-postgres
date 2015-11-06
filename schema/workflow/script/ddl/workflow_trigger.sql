/***********************************************************************************************************************************
WORKFLOW Triggers
***********************************************************************************************************************************/
create trigger configjobtable_trigger_insert_after
    after insert on _workflow.config_job_table
    for each row execute procedure _workflow.configjobtable_trigger_insert_after();

create trigger configjobtable_trigger_delete_before
    before delete on _workflow.config_job_table
    for each row execute procedure _workflow.configjobtable_trigger_delete_before();

create trigger workflow_trigger_delete_before
    before delete on _workflow.workflow
    for each row execute procedure _workflow.workflow_trigger_delete_before();

create trigger jobtable_trigger_delete_before
    before delete on _workflow.job_table
    for each row execute procedure _workflow.jobtable_trigger_delete_before();

create trigger workflow_trigger_insert
    after insert on _workflow.workflow
    for each row execute procedure _workflow.workflow_trigger_insert_after();

create trigger workflow_trigger_update_before
    before update on _workflow.workflow
    for each row execute procedure _workflow.workflow_trigger_update_before();

create trigger job_trigger_insert_after
    after insert on _workflow.job
    for each row execute procedure _workflow.job_trigger_insert_after();

create trigger job_trigger_update_after
    after update on _workflow.job
    for each row execute procedure _workflow.job_trigger_update_after();
    