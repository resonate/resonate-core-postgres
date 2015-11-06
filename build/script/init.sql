/***********************************************************************************************************************************
init.sql

Make sure the db cannot accept new connections while update is going on.
**********************************************************************************************************************************/;
-- Allow connections to the db
update pg_database set datallowconn = true where datname = '@db.instance_name@';
commit;

-- Connection to the db
\connect @db.instance_name@;

-- Only show warnings and errors
set client_min_messages='warning';

-- Make sure that no connections are made while script is running
update pg_database set datallowconn = false where datname = '@db.instance_name@';
commit;

update pg_language set lanpltrusted = true where lanname = 'c';
