/***********************************************************************************************************************
* UTILITY Release Trigger Functions
***********************************************************************************************************************/
create or replace function _utility.release_trigger_insert() returns trigger as $$
begin
    if
    (
        select count(*) > 0
          from _utility.release
    ) then
        raise exception 'A release record already exists so no inserts are allowed';
    end if;
    
    return new;
end
$$ language plpgsql security definer;

create or replace function _utility.release_trigger_update() returns trigger as $$
begin
    if new.name = old.name and 
       (new.patch is null and old.patch is not null or new.patch <= old.patch) then
        raise exception 'The new patch version % does not increase over the old patch version %', new.patch, old.patch;
    end if;
    
    return new;
end
$$ language plpgsql security definer;

create or replace function _utility.release_trigger_delete() returns trigger as $$
begin
    raise exception 'Deletes from the release table are not allowed';
    
    return null;
end
$$ language plpgsql security definer;
