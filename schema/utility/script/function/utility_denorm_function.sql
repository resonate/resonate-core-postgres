/***********************************************************************************************************************
* UTILITY Schema Denormalize Functions
*
* These functions help to maintain structured denormalized tables.
***********************************************************************************************************************/
create or replace function _utility.denorm_maintain_structure(strSchemaName text,
                                                              strTableName text,
															  strColumnName text,
															  strDenormSchemaName text,
                                                              strDenormTableName text,
															  strDenormColumnPrefix text,
															  iDenormFixedColumnCount int,
															  strDenormColumnDataType text,
															  bNullable boolean,
															  strDenormColumnDefault text,
															  bRebuild boolean) returns setof text as $$
declare
    rColumnName record;
    strTableSql text;
    strDenormSql text;
    strAlterSql text;
begin
    strTableSql = 
        E'select ' || strColumnName || E'::text as column_name\n' ||
        E'  from ' || strSchemaName || '.' || strTableName;

    strDenormSql = 
        E'select substr(pg_attribute.attname, length(''' || strDenormColumnPrefix || E''') + 1) as column_name\n' ||
        E'  from pg_namespace, pg_class, pg_attribute\n' ||
        E' where pg_namespace.nspname = ''' || strDenormSchemaName || E'''\n' ||
        E'   and pg_namespace.oid = pg_class.relnamespace\n' ||
        E'   and pg_class.relname = ''' || strDenormTableName || E'''\n' ||
        E'   and pg_class.oid = pg_attribute.attrelid\n' ||
        E'   and attnum > ' || iDenormFixedColumnCount || E'\n' ||
        E'   and not attisdropped';

   for rColumnName in execute
        strDenormSql || E'\n' ||
        E'    except\n' ||
        strTableSql || E'\n' ||
        'order by column_name'
   loop
       execute 'alter table ' || strDenormSchemaName || '.' || strDenormTableName || ' drop ' || strDenormColumnPrefix || rColumnName.column_name;
   end loop;

   for rColumnName in execute
        strTableSql || E'\n' ||
        E'    except\n' ||
        strDenormSql || E'\n' ||
        'order by column_name'
   loop 
       strAlterSql = 'alter table ' || strDenormSchemaName || '.' || strDenormTableName || ' add ' || strDenormColumnPrefix || rColumnName.column_name || ' ' || strDenormColumnDataType;

       if not bNullable then
           strAlterSql = strAlterSql || ' not null';
       end if;

       if strDenormColumnDefault is not null then
           strAlterSql = strAlterSql || ' default ' || strDenormColumnDefault;
       end if;
       
       execute strAlterSql;
	   
	   return next rColumnName.column_name;
   end loop;
end;
$$ language plpgsql security definer;

do $$
begin
    execute 'grant execute on function _utility.denorm_maintain_structure(text, text, text, text, text, text, int, text, boolean, text, boolean) to ' || 
            _utility.role_get('user') || ', ' || _utility.role_get('etl');
end $$;