/***********************************************************************************************************************************
UTILITY Release Tables
***********************************************************************************************************************************/
create table _utility.release
(
    name text not null,
    patch int
        constraint release_patch_ck check (patch >= 1 and patch < 100),
    vcs_commit_key text,
    build_user text not null,
    timestamp timestamp with time zone not null
);
