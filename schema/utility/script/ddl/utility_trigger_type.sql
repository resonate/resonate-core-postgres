/***********************************************************************************************************************
* Trigger Types
***********************************************************************************************************************/
create type _utility.trigger_type as enum ('insert', 'update', 'delete');
create type _utility.trigger_when as enum ('before', 'after');
create type _utility.trigger_security as enum ('definer', 'invoker');
