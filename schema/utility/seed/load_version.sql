/***********************************************************************************************************************************
Load Version
***********************************************************************************************************************************/
do $$
begin
    perform _utility.version_set((select current from _build.version));
end $$;