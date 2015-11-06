/***********************************************************************************************************************************
WORKFLOW Type
***********************************************************************************************************************************/

create type _workflow.job_validate_type as enum ('object_list');
create type _workflow.job_validate_state as enum ('error', 'drop', 'warning');
create type _workflow.job_validate_action as enum ('pass', 'fail');
