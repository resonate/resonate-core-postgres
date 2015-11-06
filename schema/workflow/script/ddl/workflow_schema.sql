/***********************************************************************************************************************************
WORKFLOW Schema
***********************************************************************************************************************************/
create schema _workflow;

--- PERMISSION ---------------------------------------------------
do $$
begin
    execute '
        grant usage
           on schema _workflow
           to ' || _utility.role_get('etl');
end $$;

--- COMMENT ------------------------------------------------------
comment on schema _workflow is
'Implements workflow and job handling.';

/***********************************************************************************************************************************
OBJECT_ID_SEQ Sequence
***********************************************************************************************************************************/
create sequence _workflow.object_id_seq;

--- COMMENT ------------------------------------------------------
comment on sequence _workflow.object_id_seq is
'Generates IDs for all workflow tables that do not begin with {{config_}}.';

/***********************************************************************************************************************************
CONFIG_ID_SEQ Sequence
***********************************************************************************************************************************/
create sequence _workflow.config_id_seq;

--- COMMENT ------------------------------------------------------
comment on sequence _workflow.config_id_seq is
'Generates IDs for all workflow tables that begin with {{config_}}.';
