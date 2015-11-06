create or replace function _scd.nextval() returns bigint as $$
begin
    return nextval('_scd.object_id_seq');
end;
$$ language plpgsql security definer;

do $$ begin execute 'grant execute on function _scd.nextval() to ' || _utility.role_get('admin'); end $$;

create or replace function _scd.init(iObjectIdMin bigint, iObjectIdMax bigint) returns void as $$
declare
    iCount int;
begin
    select count(*)
      into iCount
      from _scd.config;

    if iCount <> 0 then
        raise exception 'SCD is already initialized';
    end if;

    insert into _scd.config (object_id_min, object_id_max)
                    values ( iObjectIdMin,  iObjectIdMax);

    execute 'alter sequence _scd.object_id_seq start ' || iObjectIdMin;
    execute 'alter sequence _scd.object_id_seq restart ' || iObjectIdMin;
    execute 'alter sequence _scd.object_id_seq minvalue ' || iObjectIdMin;
    execute 'alter sequence _scd.object_id_seq maxvalue ' || iObjectIdMax;
end;
$$ language plpgsql security definer;

create or replace function _scd.object_update_role_get(lObjectId bigint, strField text) returns text as $$
begin
    return
    (
        select account.key
          from _scd.journal
               inner join _scd.transaction
                    on transaction.id = journal.transaction_id
               inner join _scd.account 
                    on account.id = transaction.account_id
         where journal.id = 
        ( 
            select max(journal_id)
              from _scd.journal
                   inner join _scd.journal_detail
                        on journal_detail.journal_id = journal.id
                   inner join _scd.config_table_column
                        on config_table_column.id = journal_detail.config_table_column_id
                       and config_table_column.name = strField
             where object_id = lObjectId
        )
    );
end
$$ language plpgsql security definer;

create or replace function _scd.create_map_table(strSchema text, strTable text) returns void as $$
declare
    strSchemaTable text = strSchema || '.' || strTable;
    strMapTable text = strTable || '_map';
    strMapTableShort text = _utility.string_table_shorten(strMapTable);
begin
    execute 'create table ' || strSchema || '.' || strMapTable || E'\r\n' ||
            '(' || E'\r\n' ||
            '    id bigint' || E'\r\n' ||
            '        constraint ' || strMapTableShort || '_id_nn not null' || E'\r\n' ||
            '        constraint ' || strMapTableShort || '_id_fk references ' || strSchemaTable || E' (id) on delete cascade,\r\n' ||
            '    map_id bigint' || E'\r\n' ||
            '        constraint ' || strMapTableShort || '_mapid_fk references ' || strSchemaTable || E' (id) on delete cascade,\r\n' ||
            '    level int' || E'\r\n' ||
            '        constraint ' || strMapTableShort || '_level_nn not null,' || E'\r\n' ||
            '    depth int' || E'\r\n' ||
            '        constraint ' || strMapTableShort || '_depth_nn not null,' || E'\r\n' ||
            '    constraint ' || strMapTableShort || '_pk primary key (id, level, map_id),' || E'\r\n' ||
            '    constraint ' || strMapTableShort || E'_id_depth_mapid_unq unique (id, depth, map_id)\r\n' ||
            ')';

    execute 'create index ' || strMapTableShort || '_mapid_idx on ' || strSchema || '.' || strMapTable || ' (map_id)';

     execute 'comment on table ' || _utility.string_schema_table_combine(strSchema, strMapTable) || ' is ' ||
             '''Hierarchical map for the ' || strTable || ' table.''';

    execute 'comment on column ' || _utility.string_schema_table_combine(strSchema, strMapTable) || '.id is ' ||
             '''References ' || strTable || '.id.''';
    execute 'comment on column ' || _utility.string_schema_table_combine(strSchema, strMapTable) || '.map_id is ' ||
             '''References ' || strTable || '.id.  If map_id is equal to id then this is a self-referential record used as a placeholder for referencing the source row during joins.''';
    execute 'comment on column ' || _utility.string_schema_table_combine(strSchema, strMapTable) || '.level is ' ||
             '''Level of map_id with the source record being 0, parents running negative, and children positive.''';
    execute 'comment on column ' || _utility.string_schema_table_combine(strSchema, strMapTable) || '.depth is ' ||
             '''Absolute depth from the root node.  The depth of the root node is 0.''';
end;
$$ language plpgsql security definer;

create or replace function _scd.config_insert(strSchema text, strTable text, bMap boolean, strCurrentUser text, bJournal boolean) returns bigint as $$
declare
    lConfigTableId bigint;
begin
    insert into _scd.config_table (schema_name, table_name, map, journal)
                           values (strSchema, strTable, bMap, bJournal)
                        returning id
                             into lConfigTableId;

    if bJournal then
        insert into _scd.config_table_column (config_table_id, name)
        select lConfigTableId,
               pg_attribute.attname
          from pg_namespace
               inner join pg_class
                   on pg_class.relnamespace = pg_namespace.oid
                  and pg_class.relname = strTable
               inner join pg_attribute
                   on pg_attribute.attrelid = pg_class.oid
                  and pg_attribute.attnum >= 1
                  and pg_attribute.attisdropped = false
                  and pg_attribute.attname <> 'id'
         where pg_namespace.nspname = strSchema
         order by pg_attribute.attnum;
    end if;

    return lConfigTableId;
end;
$$ language plpgsql security definer;

create or replace function _scd.config_update(strSchema text, strTable text, bMap boolean, bJournal boolean) returns bigint as $$
declare
    lConfigTableId bigint;
    bCurrentMap boolean;
    bCurrentJournal boolean;
    rColumn record;
    rObject record;
    lJournalId bigint;
begin
    -- Get the current settings from _scd.config_table
    select id,
           map,
           journal
      into lConfigTableId,
           bCurrentMap,
           bCurrentJournal
      from _scd.config_table
     where schema_name = strSchema
       and table_name = strTable;

    -- Check to see if the map flag has changed
    if bMap <> bCurrentMap then
        -- Create the map if it has been turned on
        if bMap = true then
            -- Make sure that all parent_id is null in all rows
            declare
                iParentCount int;
            begin
                execute
                    E'select count(*)' ||
                    E'  from ' || strSchema || '.' || strTable || E'\n' ||
                    E' where parent_id is not null' into iParentCount;

                if iParentCount <> 0 then
                    raise exception 'Cannot add a map table to %.% unless parent_id is null in all rows', strSchema, strTable;
                end if;
            end;

            -- Create the map table
            perform _scd.create_map_table(strSchema, strTable);
            
            -- Insert records into the map
            execute
                E'insert into ' || strSchema || '.' || strTable || E'_map (id, map_id, level, depth)\n' ||
                E'select id, id, 0, 0\n' ||
                E'  from ' || strSchema || '.' || strTable || E'\n' ||
                E' where parent_id is null';
        else
            -- Don't allow maps to be turned off
            raise exception 'Cannot remove map for %.%', strSchema, strTable;
        end if;
    end if;

    -- Journals can only be turned on (never off)
    if bCurrentJournal and not bJournal then
        raise exception 'Cannot disable journaling for %.%', strSchema, strTable;
    end if;

    -- Update _scd.config_table with the new settings
    update _scd.config_table
       set map = bMap,
           journal = bJournal
     where id = lConfigTableId;
     
    -- If the table is journaled then reconcile the fields
    if bJournal then
        lock table _scd.config_table_column in exclusive mode;

        -- Get the new field list from the system catalog
        create temporary table temp_scd_config_table_column_new as
        select pg_attribute.attname as name
          from pg_namespace
               inner join pg_class
                   on pg_class.relnamespace = pg_namespace.oid
                  and pg_class.relname = strTable
               inner join pg_attribute
                   on pg_attribute.attrelid = pg_class.oid
                  and pg_attribute.attnum >= 1
                  and pg_attribute.attisdropped = false
                  and pg_attribute.attname <> 'id'
         where pg_namespace.nspname = strSchema;

        -- Get the current field list from _scd.config_table_column
        create temporary table temp_scd_config_table_column_current as
        select name
          from _scd.config_table_column
         where config_table_column.config_table_id = lConfigTableId;
        
        -- Check if there are new columns
        if
        (
            select count(*) > 0
              from
            (
                select name 
                  from temp_scd_config_table_column_new
                    except
                select name 
                  from temp_scd_config_table_column_current
            ) column_new
        ) then
            -- Insert the new columns into config_table_column
            for rColumn in
                select name
                  from temp_scd_config_table_column_new
                    except
                select name 
                  from temp_scd_config_table_column_current
            loop
                insert into _scd.config_table_column (config_table_id, name) values (lConfigTableId, rColumn.name);
            end loop;
        
            -- Loop through all the objects
            for rObject in
                select id
                  from _scd.object
                 where config_table_id = lConfigTableId
            loop
                -- Create a journal entry
                lJournalId = _scd.journal_create(rObject.id, 'i');

                -- Create an insert record for all the new columns
                for rColumn in
                    select id,
                           name 
                      from _scd.config_table_column
                     where config_table_id = lConfigTableId
                       and name in
                    (                     
                        select name
                          from temp_scd_config_table_column_new
                            except
                        select name 
                          from temp_scd_config_table_column_current
                    )
                loop
                    execute 'insert into _scd.journal_detail (journal_id, config_table_column_id, value) values (' || lJournalId || ', ' || rColumn.Id || ', (select ' || rColumn.name || ' from ' || strSchema || '.' || strTable || ' where id = ' || rObject.id || '))';
                end loop;
            end loop;
        end if;

        -- Check if there are deleted columns
        if
        (
            select count(*) > 0
              from
            (
                select name 
                  from temp_scd_config_table_column_current
                    except
                select name 
                  from temp_scd_config_table_column_new
            ) column_deleted
        ) then
            -- Remove the journal detail for deleted columns
            delete
              from _scd.journal_detail
             where config_table_column_id in
            (
                select id
                  from _scd.config_table_column
                 where config_table_id = lConfigTableId
                   and name in
                (                     
                    select name 
                      from temp_scd_config_table_column_current
                        except
                    select name
                      from temp_scd_config_table_column_new
                )
            );

            -- Remove the journal entry for deleted columns if that was the only change
            delete 
              from _scd.journal
             where type <> 'd'
               and not exists
            (
                select journal_id
                  from _scd.journal_detail
                 where journal_id = journal.id
            );

            -- Remove the transaction for deleted columns if that was the only change
            delete 
              from _scd.transaction
             where not exists
            (
                select transaction_id
                  from _scd.journal
                 where transaction_id = transaction.id
            );

            -- Remove deleted columns from _scd.config_table_column
            delete 
              from _scd.config_table_column
             where config_table_id = lConfigTableId
               and name in
            (
                select name 
                  from temp_scd_config_table_column_current
                    except
                select name
                  from temp_scd_config_table_column_new
            );
            
        end if;
        
        -- Drop the temp tables used for new and current columns
        drop table temp_scd_config_table_column_new;
        drop table temp_scd_config_table_column_current;
    end if;

    return lConfigTableId;
end;
$$ language plpgsql security definer;

create or replace function _scd.rename_table(strSchemaNameOld text, strTableNameOld text, strSchemaNameNew text, strTableNameNew text) returns void as $$
declare
    lConfigTableId bigint;
    bMap boolean;
    bJournal boolean;
begin
    -- Get info about the table
    select id,
           map,
           journal
      into lConfigTableId,
           bMap,
           bJournal
      from _scd.config_table
     where schema_name = strSchemaNameOld
       and table_name = strTableNameOld;

     if lConfigTableId is null then
        raise exception 'Table %.% does not exist in the scd', strSchemaNameOld, strTableNameOld;
     end if;

    -- For now this function does not work with mapped tables
    if bMap then
        raise exception 'Mapped tables (%.%) are not yet supported', strSchemaNameOld, strTableNameOld;
    end if;

    -- Drop the _scd.object.id FK
    execute 'alter table ' || _utility.string_schema_table_combine(strSchemaNameOld, strTableNameOld) || ' drop constraint ' || _utility.string_table_shorten(strTableNameOld) || '_scd_id_fk';
    
    -- Drop the old triggers
    execute _utility.trigger_drop('scd', strSchemaNameOld, strTableNameOld, 'insert', 'before');
    execute _utility.trigger_function_drop('scd', strSchemaNameOld, strTableNameOld, 'insert', 'before');
    execute _utility.trigger_drop('scd', strSchemaNameOld, strTableNameOld, 'update', 'before');
    execute _utility.trigger_function_drop('scd', strSchemaNameOld, strTableNameOld, 'update', 'before');
    execute _utility.trigger_drop('scd', strSchemaNameOld, strTableNameOld, 'delete', 'after');
    execute _utility.trigger_function_drop('scd', strSchemaNameOld, strTableNameOld, 'delete', 'after');

    -- Alter the schema name if required
    if strSchemaNameOld <> strSchemaNameNew then
        execute 'alter table ' || _utility.string_schema_table_combine(strSchemaNameOld, strTableNameOld) || ' set schema ' || strSchemaNameNew;
    end if;

    -- Alter the table name if required
    if strTableNameOld <> strTableNameNew then
        execute 'alter table ' || _utility.string_schema_table_combine(strSchemaNameNew, strTableNameOld) || ' rename to ' || '"' || strTableNameNew || '"';
    end if;

    -- Update _scd.config_table
    update _scd.config_table
       set schema_name = strSchemaNameNew,
           table_name = strTableNameNew
     where id = lConfigTableId;

    -- Add contraints and triggers back to the table
    perform _scd.add_table(strSchemaNameNew, strTableNameNew, bMap, bJournal, false);
end;
$$ language plpgsql security definer;

create or replace function _scd.rename_table_column(strSchemaName text, strTableName text, strColumnNameOld text, strColumnNameNew text) returns void as $$
declare
    lConfigTableId bigint;
    bMap boolean;
    bJournal boolean;
begin
    -- Get info about the table
    select id,
           map,
           journal
      into lConfigTableId,
           bMap,
           bJournal
      from _scd.config_table
     where schema_name = strSchemaName
       and table_name = strTableName;

     if lConfigTableId is null then
        raise exception 'Table %.% does not exist in the scd', strSchemaName, strTableName;
     end if;

    -- Make sure it is a valid column
    if strColumnNameOld = 'id' or strColumnNameOld = 'parent_id' and bMap then
        raise exception 'Cannot rename column "%" on table %.% to "%"', strColumnNameOld, strSchemaName, strTableName, strColumnNameNew;
    end if;

    -- Update _scd.config_table_column
    update _scd.config_table_column
       set name = strColumnNameNew
     where config_table_id = lConfigTableId
       and name = strColumnNameOld;

    -- Rename the column
    execute 'alter table ' || _utility.string_schema_table_combine(strSchemaName, strTableName) || ' rename column ' || strColumnNameOld || ' to ' || strColumnNameNew;

    -- Update the triggers
    perform _scd.update_table(strSchemaName, strTableName, bMap, bJournal);
end;
$$ language plpgsql security definer;

create or replace function _scd.remove_table(strSchemaName text, strTableName text) returns void as $$
declare
    lConfigTableId int;
    bMap boolean;
    rObject record;
begin
    -- Get information about the table
    select id,
           map
      into lConfigTableId,
           bMap
      from _scd.config_table
     where schema_name = strSchemaName
       and table_name = strTableName;

    -- For now this function does not work with mapped tables
    if bMap then
        raise exception 'Mapped tables (%.%) are not yet supported', strSchemaName, strTableName;
    end if;

    -- Drop the _scd.object.id FK
    execute 'alter table ' || _utility.string_schema_table_combine(strSchemaName, strTableName) || ' drop constraint if exists ' || _utility.string_table_shorten(strTableName) || '_scd_id_fk';
     
    -- Drop the old triggers
    execute _utility.trigger_drop('scd', strSchemaName, strTableName, 'insert', 'before');
    execute _utility.trigger_function_drop('scd', strSchemaName, strTableName, 'insert', 'before');
    execute _utility.trigger_drop('scd', strSchemaName, strTableName, 'update', 'before');
    execute _utility.trigger_function_drop('scd', strSchemaName, strTableName, 'update', 'before');
    execute _utility.trigger_drop('scd', strSchemaName, strTableName, 'delete', 'after');
    execute _utility.trigger_function_drop('scd', strSchemaName, strTableName, 'delete', 'after');

    for rObject in
        select id from _scd.object
         where config_table_id = lConfigTableId
    loop
        delete from _scd.journal_detail
         where journal_id in
        (
            select id 
              from _scd.journal 
             where object_id = rObject.id
        );
          
        delete from _scd.journal 
         where object_id = rObject.id;

        delete from _scd.object
         where id = rObject.id;
    end loop;

    delete from _scd.config_table_column
     where config_table_id = lConfigTableId;

    delete from _scd.config_table
     where id = lConfigTableId;
end;
$$ language plpgsql;

create or replace function _scd.add_table(strSchema text, strTable text, bMap boolean, bJournal boolean default false, bCreate boolean default true) returns void as $$
declare
    lConfigTableId bigint;
    rRole record;
begin
     execute 'alter table ' || _utility.string_schema_table_combine(strSchema, strTable) || ' add constraint ' || _utility.string_table_shorten(strTable) || '_scd_id_fk foreign key (id) references _scd.object (id)';

     execute 'comment on column ' || _utility.string_schema_table_combine(strSchema, strTable) || '.id is ' ||
             '''Synthetic primary key generated by the _scd schema.''';
    
    -- Revoke insert and update on the id column (the synthetic primary key on an scd table cannot be inserted or updated except by the owner)
    for rRole in
        select _utility.role_get_all() as name
    loop
        execute 'revoke insert (id), update (id) on ' || _utility.string_schema_table_combine(strSchema, strTable) || ' from ' || rRole.name;
    end loop;
    
    execute _scd.add_table_internal(strSchema, strTable, bMap, bJournal, bCreate);

    if bMap and bCreate then
        execute 'alter table ' || _utility.string_schema_table_combine(strSchema, strTable) || ' add constraint ' || _utility.string_table_shorten(strTable) || '_scd_parentid_ck check (parent_id <> id)';
    end if;
end;
$$ language plpgsql;

create or replace function _scd.add_table_internal(strSchema text, strTable text, bMap boolean, bJournal boolean, bCreate boolean) returns void as $$
declare
    lConfigTableId bigint;
begin
    if bCreate then
        lConfigTableId = _scd.config_insert(strSchema, strTable, bMap, current_user, bJournal);

        if bMap then
            execute _scd.create_map_table(strSchema, strTable);
        end if;
    else
        select id
          into lConfigTableId
          from _scd.config_table
         where schema_name = strSchema
           and table_name = strTable;

        if lConfigTableId is null then
            raise exception 'Unable to find table %.% in _scd.config_table', strSchema, strTable;
        end if;
    end if;

    execute _scd.create_before_insert_trigger(lConfigTableId, strSchema, strTable, bJournal, false);
    execute _scd.create_after_insert_trigger(lConfigTableId, strSchema, strTable, bMap);
    execute _scd.create_before_update_trigger(lConfigTableId, strSchema, strTable, bMap, bJournal, false);
    execute _scd.create_after_delete_trigger(lConfigTableId, strSchema, strTable, bJournal, false);
end;
$$ language plpgsql security definer;

create or replace function _scd.update_table(strSchema text, strTable text, bMap boolean, bJournal boolean) returns void as $$
declare
    lConfigTableId bigint;
    bCurrentMap boolean;
begin
    -- Get information about the table
    select id,
           map
      into lConfigTableId,
           bCurrentMap
      from _scd.config_table
     where schema_name = strSchema
       and table_name = strTable;

    -- Update the configuration
    perform _scd.config_update(strSchema, strTable, bMap, bJournal);

    -- Rebuild the triggers
    perform _scd.create_before_insert_trigger(lConfigTableId, strSchema, strTable, bJournal, true);
    perform _scd.create_before_update_trigger(lConfigTableId, strSchema, strTable, bMap, bJournal, true);
    perform _scd.create_after_delete_trigger(lConfigTableId, strSchema, strTable, bJournal, true);

    -- Create the map trigger if a map has been created
    if not bCurrentMap and bMap then
        perform _scd.create_after_insert_trigger(lConfigTableId, strSchema, strTable, true);
        execute 'alter table ' || _utility.string_schema_table_combine(strSchema, strTable) || ' add constraint ' || _utility.string_table_shorten(strTable) || '_scd_parentid_ck check (parent_id <> id)';
    end if;
end;
$$ language plpgsql security definer;

create or replace function _scd.refresh() returns void as $$
declare
    rTable record;
begin
    for rTable in 
        select schema_name,
               table_name,
               map,
               journal
          from _scd.config_table
         order by schema_name, table_name
    loop
        perform _scd.update_table(rTable.schema_name, rTable.table_name, rTable.map, rTable.journal);
    end loop;
end;
$$ language plpgsql security definer;

create or replace function _scd.transaction_create_internal(strComment text default null) returns bigint as $$
declare
    lTransactionId bigint;
    lAccountId bigint;
    strAccountName text;
    bAccountDeny boolean;
    bAccountComment boolean;
    lApplicationId bigint;
    strApplicationName text;
    bApplicationDeny boolean;
    bApplicationComment boolean;
    bBuild boolean = false;
    strSql text;
begin
    begin
         execute 'create temporary table _scd_temp_transaction (id bigint, comment text) on commit drop';

         insert into _scd_temp_transaction (comment) values (strComment);
    exception
        when duplicate_table then
            select id
              into lTransactionId
              from _scd_temp_transaction;

            if strComment is not null then
                update _scd_temp_transaction
                   set comment = coalesce(comment || E'\n', '') || trim(both E' \t\n' from strComment);

                if lTransactionId is not null then
                    update _scd.transaction
                       set comment = (select comment from _scd_temp_transaction)
                     where id = lTransactionId;
                end if;
            end if;

            if lTransactionId is not null then
                return lTransactionId;
            end if;
    end;

    select count(*) = 1
      into bBuild
      from pg_namespace
     where pg_namespace.nspname = '_build';

    if (strComment is null or bBuild = true) and lTransactionId is null then
        select nextval('_scd.object_id_seq')
         into lTransactionId;

         update _scd_temp_transaction 
            set id = lTransactionId;

         select id,
                key,
                deny,
                comment
           into lAccountId,
                strAccountName,
                bAccountDeny,
                bAccountComment
           from _scd.account
          where account.key = session_user;

        if bAccountDeny then
            raise exception 'Service account/role "%" cannot update journaled scd tables.', strAccountName;
        end if;

        -- This exception is for 9.0-9.2 compatability.
        strSql = 
            'select coalesce(application_name, ''<unknown>'')
              from pg_stat_activity
             where pid = pg_backend_pid()';
        
        execute strSql into strApplicationName;

        select id,
               deny,
               comment
          into lApplicationId,
               bApplicationDeny,
               bApplicationComment
          from _scd.application
         where lower(application.key) = lower(strApplicationName);

        if bApplicationDeny then
            raise exception 'Application "%" cannot update journaled scd tables.', strApplicationName;
        end if;

        if lAccountId is null then
            insert into _scd.account (key)
                              values (session_user)
                           returning id, deny, comment
                                into lAccountId, bAccountDeny, bAccountComment;
        end if;

        if lApplicationId is null then
            insert into _scd.application (key)
                                  values (strApplicationName)
                               returning id, deny, comment
                                    into lApplicationId, bApplicationDeny, bApplicationComment;
        end if;

        strComment = (select comment from _scd_temp_transaction);

        if bAccountComment and bApplicationComment then
            if strComment is null then
                raise exception 'Transaction comment is required';
            end if;

            if strComment !~ (select comment_expression from _scd.config) and not bBuild then
                raise exception 'Transaction comment "%" does not match expression "%"', strComment, (select comment_expression from _scd.config);
            end if;
        end if;

        insert into _scd.transaction (id, build, account_id, application_id, comment)
                              values (lTransactionId, bBuild, lAccountId, lApplicationId, strComment);

        perform _utility.metric_begin('_scd', 'transaction_create_internal', null, lTransactionId);
    end if;

    return lTransactionId;
end
$$ language plpgsql security definer;

create or replace function _scd.transaction_create(strComment text default null) returns bigint as $$
begin
    if strComment is null then
        raise exception 'Transaction must have a comment';
    end if;

    return _scd.transaction_create_internal(strComment);
end
$$ language plpgsql security definer;

do $$ begin
    execute 'grant execute on function _scd.transaction_create(text) to ' || _utility.role_get('user'); 
    execute 'grant execute on function _scd.transaction_create(text) to ' || _utility.role_get('etl'); 
    execute 'grant execute on function _scd.transaction_create(text) to ' || _utility.role_get('admin'); 
end $$;

create or replace function _scd.journal_create(lObjectId bigint, strType text) returns bigint as $$
declare
    lTransactionId bigint;
    lJournalId bigint;
begin
    lTransactionId = _scd.transaction_create_internal();

    insert into _scd.journal (object_id, transaction_id, timestamp, type)
                      values (lObjectId, lTransactionId, clock_timestamp(), strType)
                   returning id
                        into lJournalId;

    return lJournalId;
end;
$$ language plpgsql security definer;

create or replace function _scd.create_before_insert_trigger(lConfigTableId bigint, strSchema text, strTable text, bJournal boolean, bUpdate boolean) returns void as $$
declare
    strBody text;
    lObjectIdMin bigint;
    lObjectIdMax bigint;
    rColumn record;
    strDeclare text = null;
begin
    select object_id_min,
           object_id_max
      into lObjectIdMin,
           lObjectIdMax
      from _scd.config;

    strBody =  
         '    if new.id is null then' || E'\n' ||
         '        select nextval(''_scd.object_id_seq'')' || E'\n' ||
         '          into new.id;' || E'\n' ||
        E'    else\n' ||
         '        if new.id >= ' || lObjectIdMin || ' and new.id <= ' || lObjectIdMax || ' then' || E'\n' ||
        E'            declare\n' ||
        E'                lCurrentId bigint;\n' ||
        E'                strHint text = ''use _scd.nextval() to generate valid SCD sequences'';\n' ||
        E'            begin\n' ||
        E'                lCurrentId = currval(''_scd.object_id_seq'');\n' ||
        E'\n' ||
         '                if new.id > lCurrentId and not pg_has_role(session_user, ''' || _utility.role_get() || E''', ''usage'') then\n' ||
        E'                    raise exception ''_scd.object_id_seq has current value of % so new.id = % is not a valid (%)'', lCurrentId, new.id, strHint;\n' ||
        E'                end if;\n' ||
        E'            exception\n' ||
        E'                when object_not_in_prerequisite_state then\n' ||
         '                    if not pg_has_role(session_user, ''' || _utility.role_get() || E''', ''usage'') then\n' ||
        E'                        raise exception ''_scd.object_id_seq has no current value so new.id = % could not have come from it (%)'', new.id, strHint;\n' ||
        E'                    end if;\n' ||
        E'            end;\n' ||
         '        end if;' || E'\n' ||
        E'\n' ||
        E'        if new.id < 100000000000000000 then\n' ||
         '            raise exception ''IDs from foreign keyspaces must be >= 100000000000000000'';' || E'\n' ||
         '        end if;' || E'\n' ||
         '    end if;' || E'\n' ||
        E'\n' ||
        E'    begin\n' ||
         '        insert into _scd.object (    id, config_table_id)' || E'\n' ||
         '                         values (new.id, ' || lpad(lConfigTableId::text, 18, ' ') || E');\n' ||
        E'    exception\n' ||
        E'        when unique_violation then\n' ||
        E'            if\n' ||
        E'            (\n' ||
         '                select config_table.id <> ' || lConfigTableId || E'\n' ||
        E'                  from _scd.object\n' ||
        E'                       inner join _scd.config_table\n' ||
        E'                            on config_table.id = object.config_table_id\n' ||
        E'                 where object.id = new.id\n' ||
        E'            ) then\n' ||
        E'                raise exception ''Object % cannot be (re)inserted into another table'', new.id;\n' ||
        E'            end if;\n' ||
        E'\n' ||
        E'            update _scd.object\n' ||
        E'               set datetime_delete = null\n' ||
        E'             where id = new.id;\n' ||
         '    end;';

    if bJournal then
        strDeclare = '    lJournalId bigint;';
    
        strBody = strBody || E'\r\n\r\n    -- Insert the journal' ||
                             E'\r\n    lJournalId = _scd.journal_create(new.id, ''i'');\r\n';

        for rColumn in
            select id,
                   name
              from _scd.config_table_column
             where config_table_id = lConfigTableId
             order by id
        loop
            strBody = strBody || E'\r\n    insert into _scd.journal_detail (journal_id, config_table_column_id, value) values (lJournalId, ' || rColumn.id || ', new.' || rColumn.name || ');';
        end loop;
    end if;        

    if bUpdate then
        execute _utility.trigger_function_create('scd', strSchema, strTable, 'insert', 'before', 'definer', strDeclare, strBody);
    else
        execute _utility.trigger_create('scd', strSchema, strTable, 'insert', 'before', 'definer', strDeclare, strBody);
    end if;
end
$$ language plpgsql security definer;

create or replace function _scd.create_after_insert_trigger(lConfigTableId bigint, strSchema text, strTable text, bMap boolean) returns void as $$
declare
    strDeclare text;
    strBody text;
    strMapTableName text = strTable || '_map';
    strSchemaMapTableName text = _utility.string_schema_table_combine(strSchema, strTable) || '_map';
begin
    if not bMap then
        return;
    end if;

    strDeclare = 
        '    iDepth int = 0;';

    strBody = 
        '    -- Select parent depth' || E'\r\n' ||
        '    if new.parent_id is not null then' || E'\r\n' ||
        '        select depth + 1' || E'\r\n' ||
        '          into iDepth' || E'\r\n' ||
        '          from ' || strSchemaMapTableName || E'\r\n' ||
        '         where id = new.parent_id' || E'\r\n' ||
        '           and level = 0;' || E'\r\n' ||
        '    end if;' || E'\r\n' ||
        E'\r\n' ||
        '    -- Insert object mapping' || E'\r\n' ||
        '    insert into ' || strSchemaMapTableName ||                              ' (    id, map_id, level,  depth)' || E'\r\n' ||
        '                ' || lpad('values', length(strSchemaMapTableName), ' ') || ' (new.id, new.id,     0, iDepth);' || E'\r\n' ||
        E'\r\n' ||
        '    -- Insert parent mapping' || E'\r\n' ||
        '    if new.parent_id is not null then' || E'\r\n' ||
        '        -- Insert the parents for this object and its children' || E'\r\n' ||
        '        insert into ' || strSchemaMapTableName || ' (id, map_id, level, depth)' || E'\r\n' ||
        '        select new.id, map_id, level - 1, depth' || E'\r\n' ||
        '          from ' || strSchemaMapTableName || E'\r\n' ||
        '         where id = new.parent_id' || E'\r\n' ||
        '           and level <= 0;' || E'\r\n' ||
        E'\r\n' ||
        '        -- Insert this object and its children into all parents' || E'\r\n' ||
        '        insert into ' || strSchemaMapTableName || ' (id, map_id, level, depth)' || E'\r\n' ||
        '        select map_id, id, level * -1, iDepth' || E'\r\n' ||
        '          from ' || strSchemaMapTableName || E'\r\n' ||
        '         where id = new.id' || E'\r\n' ||
        '           and level < 0;' || E'\r\n' ||
        '    end if;';
        
    execute _utility.trigger_create('scd', strSchema, strTable, 'insert', 'after', 'definer', strDeclare, strBody);
end
$$ language plpgsql security definer;

create or replace function _scd.create_before_update_trigger(lConfigTableId bigint, strSchema text, strTable text, bMap boolean, bJournal boolean, bUpdate boolean) returns void as $$
declare
    strDeclare text = null;
    strBody text;
    strSchemaMapTableName text = _utility.string_schema_table_combine(strSchema, strTable) || '_map';
    rColumn record;
begin
    if bMap then
        strDeclare = 
            '    iLoopId bigint;' || E'\r\n' ||
            '    iDepth int;' || E'\r\n' ||
            '    xResult record;' || E'\r\n' ||
            '    xDepthResult record;';
    end if;

    strBody = 
        '    -- Object ID cannot be altered' || E'\r\n' ||
        '    if new.id <> old.id then' || E'\r\n' ||
        '        raise exception ''Cannot alter ID on ' || upper(_utility.string_schema_table_combine(strSchema, strTable)) || ''';' || E'\r\n' ||
        '    end if;';

    if bMap then
        strBody = strBody ||
            E'\r\n\r\n' ||
            '    -- If PARENT_ID is altered update the map table' || E'\r\n' ||
            '    if new.parent_id is distinct from old.parent_id then' || E'\r\n' ||
            '        lock table ' || strSchemaMapTableName || ' in exclusive mode;' || E'\r\n' ||
            E'\r\n' ||
            '        -- Delete old mapping' || E'\r\n' ||
            '        if old.parent_id is not null then' || E'\r\n' ||
            '            -- Delete this object and its children from all parents' || E'\r\n' ||
            '            delete from ' || strSchemaMapTableName || E'\r\n' ||
            '             where id in' || E'\r\n' ||
            '            (' || E'\r\n' ||
            '                select map_id' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.id' || E'\r\n' ||
            '                   and level < 0' || E'\r\n' ||
            '            )' || E'\r\n' ||
            '               and map_id in' || E'\r\n' ||
            '            (' || E'\r\n' ||
            '                select map_id' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.id' || E'\r\n' ||
            '                   and level >= 0' || E'\r\n' ||
            '            );' || E'\r\n' ||
            E'\r\n' ||
            '            -- Delete the old parents for this object and its children' || E'\r\n' ||
            '            delete from ' || strSchemaMapTableName || E'\r\n' ||
            '             where id in' || E'\r\n' ||
            '            (' || E'\r\n' ||
            '                select map_id' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.id' || E'\r\n' ||
            '                   and level >= 0' || E'\r\n' ||
            '            )' || E'\r\n' ||
            '               and map_id in' || E'\r\n' ||
            '            (' || E'\r\n' ||
            '                select map_id' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.id' || E'\r\n' ||
            '                   and level < 0' || E'\r\n' ||
            '            );' || E'\r\n' ||
            E'\r\n' ||
            '            -- Update depths to reflect the new tree position' || E'\r\n' ||
            '            for xDepthResult in' || E'\r\n' ||
            '                select id, depth old_depth, depth - ' || E'\r\n' ||
            '                (' || E'\r\n' ||
            '                    select depth' || E'\r\n' ||
            '                      from ' || strSchemaMapTableName || E'\r\n' ||
            '                     where id = new.id' || E'\r\n' ||
            '                       and level = 0' || E'\r\n' ||
            '                ) as depth' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id in' || E'\r\n' ||
            '                (' || E'\r\n' ||
            '                    select map_id' || E'\r\n' ||
            '                      from ' || strSchemaMapTableName || E'\r\n' ||
            '                     where id = new.id' || E'\r\n' ||
            '                       and level >= 0' || E'\r\n' ||
            '                )' || E'\r\n' ||
            '                 order by id, old_depth' || E'\r\n' ||
            '            loop' || E'\r\n' ||
            '                update ' || strSchemaMapTableName || E'\r\n' ||
            '                   set depth = xDepthResult.depth' || E'\r\n' ||
            '                 where id = xDepthResult.id' || E'\r\n' ||
            '                   and depth = xDepthResult.old_depth;' || E'\r\n' ||
            '            end loop;' || E'\r\n' ||
            '        end if;' || E'\r\n' ||
            E'\r\n' ||
            '        -- Insert new mapping' || E'\r\n' ||
            '        if new.parent_id is not null then' || E'\r\n' ||
            '            -- Look for the new parent in the child list of the current object' || E'\r\n' ||
            '            select map_id' || E'\r\n' ||
            '              into iLoopId' || E'\r\n' ||
            '              from ' || strSchemaMapTableName || E'\r\n' ||
            '             where id = new.id' || E'\r\n' ||
            '               and level > 0' || E'\r\n' ||
            '               and map_id = new.parent_id;' || E'\r\n' ||
            E'\r\n' ||
            '            -- If the parent is found a loop will be created, return error' || E'\r\n' ||
            '            if iLoopId is not null then' || E'\r\n' ||
            '                raise exception ''PARENT_ID=% creates a loop on ' || upper(strSchemaMapTableName) || ' (ID=%)'', new.parent_id, new.id;' || E'\r\n' ||
            '            end if;' || E'\r\n' ||
            E'\r\n' ||
            '            -- Select the depth of the new parent' || E'\r\n' ||
            '            select case depth when null then 0 else depth + 1 end' || E'\r\n' ||
            '              into iDepth' || E'\r\n' ||
            '              from ' || strSchemaMapTableName || E'\r\n' ||
            '             where id = new.parent_id' || E'\r\n' ||
            '               and level = 0;' || E'\r\n' ||
            E'\r\n' ||
            '            -- Insert new mapping' || E'\r\n' ||
            '            for xResult in' || E'\r\n' ||
            '                select map_id, depth' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.id' || E'\r\n' ||
            '                 order by level' || E'\r\n' ||
            '            loop' || E'\r\n' ||
            '                -- Update depths for object and its children' || E'\r\n' ||
            '                for xDepthResult in' || E'\r\n' ||
            '                    select depth' || E'\r\n' ||
            '                      from ' || strSchemaMapTableName || E'\r\n' ||
            '                     where id = xResult.map_id' || E'\r\n' ||
            '                     order by depth desc' || E'\r\n' ||
            '                loop' || E'\r\n' ||
            '                    update ' || strSchemaMapTableName || E'\r\n' ||
            '                       set depth = depth + iDepth' || E'\r\n' ||
            '                     where id = xResult.map_id' || E'\r\n' ||
            '                       and depth = xDepthResult.depth;' || E'\r\n' ||
            '                end loop;' || E'\r\n' ||
            E'\r\n' ||
            '                -- Insert the parents for this object and its children' || E'\r\n' ||
            '                insert into ' || strSchemaMapTableName || ' (id, map_id, level, depth)' || E'\r\n' ||
            '                select xResult.map_id, map_id, level - 1 - xResult.depth, depth' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = new.parent_id' || E'\r\n' ||
            '                   and level <= 0;' || E'\r\n' ||
            E'\r\n' ||
            '                -- Insert this object and its children into all parents' || E'\r\n' ||
            '                insert into ' || strSchemaMapTableName || ' (id, map_id, level, depth)' || E'\r\n' ||
            '                select map_id, id, level * -1, iDepth + xResult.depth' || E'\r\n' ||
            '                  from ' || strSchemaMapTableName || E'\r\n' ||
            '                 where id = xResult.map_id' || E'\r\n' ||
            '                   and level < xResult.depth * -1;' || E'\r\n' ||
            '            end loop;' || E'\r\n' ||
            '        end if;' || E'\r\n' ||
            '    end if;';
    end if;

    if bJournal then
        if strDeclare is null then
            strDeclare = '';
        else
            strDeclare = strDeclare || E'\r\n';
        end if;

        strDeclare = strDeclare || '    lJournalId bigint;';

        strBody = strBody || E'\r\n\r\n    -- Update the journal' ||
                             E'\r\n    lJournalId = _scd.journal_create(new.id, ''u'');';

        for rColumn in
            select id,
                   name
              from _scd.config_table_column
             where config_table_id = lConfigTableId
             order by id
        loop
            strBody = strBody || E'\r\n\r\n    if new.' || rColumn.name || ' is distinct from old.' || rColumn.name || E' then\r\n' ||
                                 E'        insert into _scd.journal_detail (journal_id, config_table_column_id, value) values (lJournalId, ' || rColumn.id || ', new.' || rColumn.name || E');\r\n' ||
                                 E'    end if;';
        end loop;
    end if;

    strBody = strBody || E'\r\n\r\n' ||
                         E'    -- Update the modified date\r\n' ||
                         E'    update _scd.object\r\n' ||
                         E'       set timestamp_update = clock_timestamp()\r\n' ||
                         E'     where id = new.id;';

    if bUpdate then
        execute _utility.trigger_function_create('scd', strSchema, strTable, 'update', 'before', 'definer', strDeclare, strBody);
    else
        execute _utility.trigger_create('scd', strSchema, strTable, 'update', 'before', 'definer', strDeclare, strBody);
    end if;
end
$$ language plpgsql security definer;

create or replace function _scd.create_after_delete_trigger(lConfigTableId bigint, strSchema text, strTable text, bJournal boolean, bUpdate boolean) returns void as $$
declare
    strBody text;
begin
    if bJournal then
        strBody = '    -- Delete the journal (journal is not actually deleted, just marked as such)' ||
                  E'\r\n    perform _scd.journal_create(old.id, ''d'');';

        strBody = strBody || E'\r\n\r\n' ||
                         E'    -- Update the deleted date\r\n' ||
                         E'    update _scd.object\r\n' ||
                         E'       set timestamp_delete = clock_timestamp()\r\n' ||
                         E'     where id = old.id;';
    else
        strBody = 
            '    -- Remove the scd object' || E'\r\n' ||
            '    delete from _scd.object' || E'\r\n' ||
            '     where id = old.id;';
    end if;        

    if bUpdate then
        execute _utility.trigger_function_create('scd', strSchema, strTable, 'delete', 'after', 'definer', null, strBody);
    else
        execute _utility.trigger_create('scd', strSchema, strTable, 'delete', 'after', 'definer', null, strBody);
    end if;
end
$$ language plpgsql security definer;

create or replace function _scd.transaction_max_get(strSchema text, strTable text) returns bigint as $$
declare
    strBody text;
begin

    if (select count(*)
      from _scd.config_table
     where schema_name = strSchema
       and table_name = strTable
       and journal = true) = 0 then 
       
            raise exception 'The table %.% is not journaled', strSchema, strTable;
    end if;
    
    return
    (
            select max(transaction_id) as transaction_max_id
              from _scd.config_table
               inner join _scd.object
                on object.config_table_id = config_table.id
               inner join _scd.journal
                on journal.object_id = object.id
             where schema_name = strSchema
               and table_name = strTable
               and journal = true
    );

end
$$ language plpgsql security definer;

do $$ begin
    execute 'grant execute on function _scd.transaction_max_get(text,text) to ' || _utility.role_get('etl');
    execute 'grant usage on schema _scd to ' || _utility.role_get('etl');
end $$;
