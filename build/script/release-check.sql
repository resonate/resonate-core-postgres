/***********************************************************************************************************************************
build-schema-release.sql

Create version table and check the release before starting the update.
***********************************************************************************************************************************/

/***********************************************************************************************************************************
Tables and functions to support releases (do not rename this table!  It is used in other places.)
***********************************************************************************************************************************/
/*create table _build.release
(
    current text not null,
    update text not null
);

insert into _build.release values ('@release@', '@release.update@');*/

do $$ 
begin
    if '@build.type@' = 'update' and (select count(*) = 1 from pg_namespace where pg_namespace.nspname = '_utility') then
        begin
            if _utility.release_get() <> '@release.update@' then
                raise exception 'Update script requires release @release.update@ but this database is release %', _utility.release_get();
            end if;
        exception -- !!! Exception clause should be dropped once all DBs are on the new release table
            when undefined_function then
                if _utility.version_get() <> '@release.update@' then
                    raise exception 'Update script requires version @release.update@ but this database is version %', _utility.version_get();
                end if;
        end;
    end if;
end $$;
