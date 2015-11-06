do $$
begin
    perform _build.build_info('update');
    perform _build.build_info_validate();
end $$;
