/***********************************************************************************************************************************
Insert the transaction comment for an update build.
**********************************************************************************************************************************/;
do $$
begin
    perform _scd.transaction_create('@release.update@ -> @release@ update build');
end $$;