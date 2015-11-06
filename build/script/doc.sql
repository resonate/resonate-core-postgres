-- Create wiki documentation
do $$
begin
    perform _build.build_info('full');
end $$;

select * from _build.build_info_document();