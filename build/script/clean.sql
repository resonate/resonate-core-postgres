/***********************************************************************************************************************************
clean.sql

This script drops all the user-owned schemas in a database.  Schemas owned by the postgres user are left untouched.
**********************************************************************************************************************************/;
do $$
declare
    xSchema record;
    xUser record;
begin
    for xSchema in
        select nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
    loop
        execute 'drop schema ' || xSchema.name || ' cascade';
    end loop;

    execute 'revoke all on database ' || current_database() || ' from public';
end $$;
