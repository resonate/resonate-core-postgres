/***********************************************************************************************************************
* Catalog Types
***********************************************************************************************************************/
create type _utility.catalog_index as
(
    schema_name text,
    table_name text,
    index_name text,
    is_unique boolean,
    field_list text[],
    tablespace_name text
);

create type _utility.catalog_constraint as
(
    schema_name text,
    table_name text,
    constraint_name text,
    type text,
    field_list text[],
    field_list_fk text[],
    schema_name_fk text,
    table_name_fk text,
    source text,
    defer text,
    on_update text,
    on_delete text,
    tablespace_name text
);

create type _utility.catalog_table_acl as
(
    schema_name text,
    table_name text,
    grantor text,
    grantee text,
    privilege_type text,
    is_grantable boolean
);

create type _utility.catalog_field_acl as
(
    schema_name text,
    table_name text,
    column_name text,
    grantor text,
    grantee text,
    privilege_type text,
    is_grantable boolean
);

