/***********************************************************************************************************************************
PARTITION Insert/Update Before Trigger
***********************************************************************************************************************************/
CREATE OR REPLACE FUNCTION _utility.partition_trigger_insert_update_before()
  RETURNS trigger AS
$$
declare
    stryText text[];
begin
    --check to see if there are any keys for this parent_id that overlap with this key (&& is the postgres operator to check overlap between two arrays)
    select key 
    into stryText
    from _utility.partition 
    where parent_id is not distinct from new.parent_id 
    and partition_table_id = new.partition_table_id
    and partition_type_id = new.partition_type_id
    and key && new.key
    and id is distinct from new.id;

    if (stryText is not null) then
        raise exception 'There is already a record in the partition table for parent_id % with key %  which overlaps with the inserted/updated key %', 
                         new.parent_id, stryText::text, new.key::text using errcode = 'UT001';
    end if;

return new;
end
$$ language plpgsql security definer;

