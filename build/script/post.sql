/***********************************************************************************************************************************
post.sql

Finalize the database update.
**********************************************************************************************************************************/;
-- Reset the role to the original logon role
reset role;

-- Make sure that all objects belong to the db owner
do $$
declare
    xSchema record;
    strOwnerName text = '@db.user@';
    xObject record;
    iCount int = 0;
begin
    -- Temp table to hold the objects with invalid ownership
    create temp table temp_post_owner
    (
        ordering serial,
        type text,
        schema_name text,
        object_name text,
        owner text
    );
     
    for xSchema in
        select pg_namespace.oid, nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspname not in (@db.validation.schema.exclusion@, '_dev')
           and pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
         order by nspname
    loop
        insert into temp_post_owner (type, schema_name, object_name, owner)
        select type,
               schema_name,
               object_name,
               owner
          from
            (
                select 'class' as type,
                       xSchema.name as schema_name,
                       relname as object_name,
                       pg_roles.rolname as owner
                  from pg_class
                       inner join pg_roles
                            on pg_roles.oid = pg_class.relowner
                           and pg_roles.rolname <> strOwnerName
                 where relnamespace = xSchema.oid
                    union
                select 'function' as types,
                       xSchema.name as schema_name,
                       proname as object_name,
                       pg_roles.rolname as owner
                  from pg_proc
                       inner join pg_roles
                            on pg_roles.oid = pg_proc.proowner
                           and pg_roles.rolname <> strOwnerName
                 where pronamespace = xSchema.oid
                    union
                select 'type' as type,
                       xSchema.name as schema_name,
                       typname as object_name,
                       pg_roles.rolname as owner
                  from pg_type
                       inner join pg_roles
                            on pg_roles.oid = pg_type.typowner
                           and pg_roles.rolname <> strOwnerName
                 where typnamespace = xSchema.oid
                   and typname not like E'\\_%'
            ) object
         where not exists
        (
            select 1
              from _build.object_owner_exception
             where object_owner_exception.schema_name = object.schema_name
               and object_owner_exception.object_name = object.object_name
               and object_owner_exception.owner = object.owner
        )
         order by schema_name,
                  object_name,
                  type;
    end loop;

    for xObject in
            select *
              from temp_post_owner
             order by ordering
        loop
            raise warning '% %.% is owned by % instead of %', Initcap(xObject.type), xSchema.name, xObject.object_name,
                                                              xObject.owner, strOwnerName;
            iCount = iCount + 1;
        end loop;

    if iCount <> 0 then
        raise exception 'Some objects do not have correct ownership';
    end if;
end $$;

-- Make sure all tables and views can be read by the schema reader role (including assigning usage)
-- Revoke execute permissions on all functions from public
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

-- Make sure all names follow the standard
do $$
declare
    xSchema record;
    xObject record;
    iCount int = 0;
begin
    for xSchema in
        select pg_namespace.oid,
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and pg_namespace.nspname not in (@db.validation.schema.exclusion@, '_dev')
           and pg_namespace.nspname not like '%_partition'
         order by name
    loop
        for xObject in
            select 'Index' as label,
                   pg_class.relname as name,
                   indrelid as table_oid,
                   pg_table.relname as table_name,
                   case indisunique 
                       when true then 'unq'
                       else 'idx'
                   end as type,
                   indkey::int[] as columns
              from pg_class, pg_index, pg_class pg_table
             where pg_class.relnamespace = xSchema.oid
               and pg_class.oid = pg_index.indexrelid 
               and pg_index.indrelid = pg_table.oid
               and not exists
             (
                 select conrelid
                   from pg_constraint
                  where pg_constraint.connamespace = xSchema.oid
                    and pg_constraint.conname = pg_class.relname
             )
                union
            select 'Constraint' as label,
                   pg_constraint.conname as name, 
                   pg_constraint.conrelid as table_oid,
                   pg_class.relname as table_name,
                   case contype
                       when 'p' then 'pk'
                       when 'u' then 'unq'
                       when 'f' then 'fk'
                       when 'c' then 'ck'
                       else 'err'
                   end as type,
                   pg_constraint.conkey::int[] as columns
              from pg_constraint, pg_class
             where pg_constraint.connamespace = xSchema.oid
               and pg_constraint.conrelid = pg_class.oid
               and pg_constraint.contype <> 't'
            order by table_name, label, name
        loop
           if (xObject.name !~ _build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns)) or
              (_build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns) is null) then
               raise warning '% "%" on table "%.%" (oid %) should be named "%"', xObject.label, xObject.name, xSchema.name, xObject.table_name, xObject.table_oid, _build.object_name(xObject.table_oid, xObject.name, xObject.type, xObject.columns);
               iCount = iCount + 1;
           end if;
        end loop;                
        
        -- for each trigger in the current schema (that does not begin with _) that is not a postgres internal trigger and is not an _scd,_workflow or _partition trigger
        -- check if there is an exception on the name else error if the name is not in the correct format
        for xObject in
            select tgname as name,
                   pg_class.oid as table_oid,
                   pg_class.relname as table_name,
                   lower(replace(pg_class.relname, '_', '')) as table_abbr
              from pg_trigger
                   inner join pg_class
                        on pg_class.oid = pg_trigger.tgrelid
                       and pg_class.relnamespace = xSchema.oid
             where pg_trigger.tgisinternal = false
               and tgname !~ ('^' || lower(replace(pg_class.relname, '_', '')) || '_(scd|workflow|partition)_trigger_.*')
               and xSchema.name !~ E'^_'
               and not exists
                (
                    select trigger_name
                      from _build.trigger_exception
                     where schema_name = xSchema.name
                       and trigger_name = pg_trigger.tgname
                )
            order by table_name, name
        loop
           if xObject.name !~ (xObject.table_abbr || '_[0-9]{2}_trigger.*') then
               raise warning 'Trigger "%" on table "%.%" (oid %) should begin with "%"', xObject.name, xSchema.name, xObject.table_name, xObject.table_oid, xObject.table_abbr || '_[0-9]{2}_trigger_';
               iCount = iCount + 1;
           end if;
        end loop;                
    end loop;

    if iCount > 0 then
        raise exception 'Object naming errors were detected';
    end if;
end $$;

-- Make sure all foreign keys have supporting indexes
do $$
declare
    xSchema record;
    xTable record;
    xForeignKey record;
    xIndex record;
    iCount int = 0;
    bFound boolean;
begin
    for xSchema in
        select pg_namespace.oid,
               nspname as name
          from pg_namespace, pg_roles
         where pg_namespace.nspowner = pg_roles.oid
           and pg_roles.rolname <> 'postgres'
           and pg_namespace.nspname not in (@db.validation.schema.exclusion@)
           and pg_namespace.nspname not like '%_partition'
         order by name
    loop
        for xTable in 
            select oid,
                   relname as name
              from pg_class
             where relnamespace = xSchema.oid
             order by relname
        loop
            for xForeignKey in 
                select _build.object_name(xTable.oid, pg_constraint.conname, '', pg_constraint.conkey::int[], false, false) as name
                  from pg_constraint
                 where pg_constraint.conrelid = xTable.oid
                   and pg_constraint.contype = 'f'
                   and not exists
                (
                    select foreign_key_name
                      from _build.foreign_key_exception
                     where schema_name = xSchema.name
                       and foreign_key_name = _build.object_name(xTable.oid, pg_constraint.conname, 'fk', pg_constraint.conkey::int[], false, true)
                )
                order by name
            loop
                bFound = false;

                for xIndex in
                    select _build.object_name(xTable.oid, pg_class.relname, 'idx', pg_index.indkey::int[], false, false) as name
                  from pg_index, pg_class
                 where pg_index.indrelid = xTable.oid
                   and pg_index.indexrelid = pg_class.oid
                   and pg_index.indpred is null
                loop
                    if strpos(xIndex.name, xForeignKey.name) = 1 then
                        bFound = true;
                    end if;
                end loop;

                if not bFound then
                    raise warning 'Foreign key %.%fk has no supporting index', xSchema.name, xForeignKey.name;
                    iCount = iCount + 1;
                end if;
            end loop;
        end loop;
    end loop;

    if iCount > 0 then
        raise exception '% unsupported foreign keys were found', iCount;
    end if;
end $$;
