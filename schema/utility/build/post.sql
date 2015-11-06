/***********************************************************************************************************************************
UTILITY Post
***********************************************************************************************************************************/
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('etl'); end $$;
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('reader'); end $$;
do $$ begin execute 'grant usage on schema _utility to ' || _utility.role_get('user'); end $$;
