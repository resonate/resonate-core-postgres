
create trigger partition_10_trigger_insert_update_before
    before insert or update on _utility.partition
    for each row execute procedure _utility.partition_trigger_insert_update_before();

