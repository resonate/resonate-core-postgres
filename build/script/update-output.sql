/***********************************************************************************************************************************
Output catalog information from the full build for the update script to use for validation.
**********************************************************************************************************************************/;
do $$
begin
    perform _build.build_info('full');
end $$;

select 'insert into _build.build_info values (' ||
       id || ', ' ||
       'null, ' ||
       coalesce(parent_id::text, 'null') || ', ' ||
       quote_literal(type) || ', ' ||
       coalesce(quote_literal(name), 'null') || ', ' ||
       coalesce(quote_literal(owner), 'null') || ', ' ||
       coalesce(quote_literal(acl), 'null') || ', ' ||
       coalesce(quote_literal(meta), 'null') || ', ' ||
       coalesce(quote_literal(meta_hash), 'null') || ', ' ||
       coalesce(comment_build_id::text, 'null') || ', ' ||
       coalesce('convert_from(' || quote_literal(comment::bytea) || ', ''UTF8'')', 'null') || ');'
  from _build.build_info
 where type <> 'db'
 order by id;

select 'insert into _build.object_name_exception values (' ||
       coalesce(quote_literal(schema_name), 'null') || ', ' ||
       coalesce(quote_literal(object_name), 'null') || ');'
  from _build.object_name_exception;
  
select 'insert into _build.foreign_key_exception values (' ||
       coalesce(quote_literal(schema_name), 'null') || ', ' ||
       coalesce(quote_literal(foreign_key_name), 'null') || ');'
  from _build.foreign_key_exception;

select 'insert into _build.object_owner_exception values (' ||
       coalesce(quote_literal(schema_name), 'null') || ', ' ||
       coalesce(quote_literal(object_name), 'null') || ', ' ||
       coalesce(quote_literal(owner), 'null') || ');'
  from _build.object_owner_exception;
  
select 'insert into _build.trigger_exception values (' ||
       coalesce(quote_literal(schema_name), 'null') || ', ' ||
       coalesce(quote_literal(trigger_name), 'null') || ');'
  from _build.trigger_exception;
