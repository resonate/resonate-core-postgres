/***********************************************************************************************************************************
SCD Triggers
***********************************************************************************************************************************/

/***********************************************************************************************************************************
CONFIG Insert Trigger

Make sure that there is never more than one row in config table.
***********************************************************************************************************************************/
create or replace function _scd.config_trigger_insert() returns trigger as $$
declare
    iCount int;
begin
    select count(*)
      into iCount
      from _scd.config;

    if iCount <> 0 then
        raise exception '_scd.config can only have one row';
    end if;

    return new;
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
CONFIG Delete Trigger

Make sure that the config row is never deleted.
***********************************************************************************************************************************/
create or replace function _scd.config_trigger_delete() returns trigger as $$
begin
    raise exception 'Cannot delete from _scd.config - this table should always have one row';

    return null;
end
$$ language plpgsql security definer;
