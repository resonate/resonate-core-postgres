/***********************************************************************************************************************************
drop.sql

Drop a database and all associated roles.
**********************************************************************************************************************************/;
drop database if exists @db.instance_name@;

do $$
declare
    xRole record;
    xTablespace record;
    strRoleName text = '@db.user@';
begin
    if
    (
        select count(*) = 0
          from pg_roles, pg_database
         where pg_roles.rolname = strRoleName
           and pg_roles.oid = pg_database.datdba
    ) then
        for xRole in
            select rolname as name
              from pg_roles
             where rolname like strRoleName || '%'
        loop
            for xTablespace in
                select spcname as name
                  from pg_tablespace
            loop
                execute 'revoke all on tablespace ' || xTablespace.name || ' from ' || xRole.name;
            end loop;

            execute 'drop role ' || xRole.name;
        end loop;
    end if;
end $$;
