/***********************************************************************************************************************************
SCD Post
***********************************************************************************************************************************/
do $$ 
begin
    if _utility.role_get() = 'rn_cookie' then
        -- Create the temp table for the shard config
        create temp table shard_config
        (
            key text not null,
            value text
        );

        reset role;
        copy shard_config from 'shard.conf' csv header;
        set role rn_cookie;

        -- Init the SCD table
        declare
            iShardKey int;
        begin
            select value::int
              into iShardKey
              from shard_config
             where key = 'shard';

            -- Init the scd before trying to insert the shard
            perform _scd.init(('4' || lpad(iShardKey::text, 2, '0') || '000000000000000')::bigint, ('4' || lpad(iShardKey::text, 2, '0') || '999999999999999')::bigint);
        end;
    else
        perform _scd.init(@schema.scd.sequence.min@, @schema.scd.sequence.max@);
    end if;
end $$;
