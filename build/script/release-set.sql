/***********************************************************************************************************************************
release-set.sql

Set the new database version.
**********************************************************************************************************************************/;
do $$
declare
    strName text = _utility.release_split('@release@', 'name');
    iPatch int = _utility.release_split('@release@', 'patch');
    tsTimestamp timestamp = current_timestamp;
    strBuildUser text = current_user;
begin
    if
    (
        select count(*) = 0
          from _utility.release
    ) then
        insert into _utility.release (name, patch, build_user, timestamp) 
                              values (strName, iPatch, strBuildUser, tsTimestamp);
    else
        update _utility.release
           set name = strName,
               patch = iPatch,
               build_user = strBuildUser,
               timestamp = tsTimestamp;
    end if;    
end $$;

-- Output the version to the console
select case when '@build.type@' = 'full' and '@build.update@' = 'y' then null else 'Database ' || current_database() || ' has been successfully ' || 
       case when '@build.type@' = 'update' then 'updated to' else 'built at' end || ' release ' || _utility.release_get() end;
