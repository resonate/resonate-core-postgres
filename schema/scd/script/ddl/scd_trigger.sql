/***********************************************************************************************************************************
SCD Triggers
***********************************************************************************************************************************/
create trigger config_trigger_insert
    before insert on _scd.config
    for each row execute procedure _scd.config_trigger_insert();

create trigger config_trigger_delete 
    before delete on _scd.config
    for each row execute procedure _scd.config_trigger_delete();
