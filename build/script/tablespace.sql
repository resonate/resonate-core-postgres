/***********************************************************************************************************************************
Assign tablespaces to new tables, partitions, and indexes
**********************************************************************************************************************************/;
do $$
begin
    perform _utility.tablespace_move();
end $$;
