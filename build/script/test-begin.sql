/***********************************************************************************************************************************
test-begin.sql

Creates a savepoint so all unit test changes can be rolled back.
**********************************************************************************************************************************/;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
reset role;

do $$
declare
    xSchema record;
    strOwnerName text = '@db.user@';
begin
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        execute 'grant usage on schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';
        execute 'grant select on all tables in schema ' || xSchema.name || ' to ' || strOwnerName || '_reader';

        if xSchema.name in ('_test') then
            execute 'grant execute on all functions in schema ' || xSchema.name || ' to public';
        else
            execute 'revoke all on all functions in schema ' || xSchema.name || ' from public';
        end if;
    end loop;
end $$;

set role @db.user@;

savepoint unit_test;
