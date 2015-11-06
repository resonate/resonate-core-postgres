/***********************************************************************************************************************
* UTILITY Release Triggers
***********************************************************************************************************************/
create trigger release_trigger_insert
    before insert on _utility.release
    for each row execute procedure _utility.release_trigger_insert();

create trigger release_trigger_update
    before update on _utility.release
    for each row execute procedure _utility.release_trigger_update();

create trigger release_trigger_delete
    before delete on _utility.release
    for each row execute procedure _utility.release_trigger_delete();
