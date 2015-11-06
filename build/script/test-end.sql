/***********************************************************************************************************************************
test-end.sql

Rollback all changes made by the unit tests.
**********************************************************************************************************************************/;
reset session authorization;
reset role;
set role @db.user@;

do $$
declare
    strUnitCurrent text = (select unit from _test.unit_test);
begin
    if strUnitCurrent is not null then
        raise exception 'Unit "%" was not ended by calling _test.unit_end()', strUnitCurrent;
    end if;
end $$;

rollback to unit_test;
