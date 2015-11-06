/***********************************************************************************************************************************
Metric Unit Test
**********************************************************************************************************************************/;
--rollback; reset session authorization; begin transaction; select _scd.transaction_create('ADHOC: Test'); SET client_min_messages = 'warning'; set role rn_rtp; savepoint unit_test_init;
rollback to unit_test_init;

/***********************************************************************************************************************************
* Metric test functions
**********************************************************************************************************************************/;
create or replace function _test.metric_test
(
    strName text,
    iDepth int
)
    returns void as $$
declare
    lMetricId bigint = _utility.metric_begin('_test', 'metric_test', array[
                                             array['name', strName],
                                             array['depth', idepth::text]]);

    iIndex int;
    stryResult text[][] = case when iDepth <> 0 then array[array['recurse', iDepth::text]] else null end;
begin
    for iIndex in 1..iDepth loop
        perform _test.metric_test(strName || ' -> child ' || iIndex, iDepth - 1);
    end loop;

    perform _utility.metric_end(lMetricId, stryResult);
end
$$ language plpgsql security definer;

/***********************************************************************************************************************************
* Create a journaled test table to test linking between metrics a journal
**********************************************************************************************************************************/;
create table _test.metric_test
(
    id bigint not null
);

do $$ begin perform _scd.add_table('_test', 'metric_test', false, true); end $$;

/***********************************************************************************************************************************
* Unit Test
**********************************************************************************************************************************/;
do $$
declare
    lMetricId bigint;
    iIndex int;
    iTestTotal int = 100;
    iExpectedTotal int;
    iActualTotal int;
    tsBegin timestamp;
    vDelta interval;
    lMetricMaxId bigint = (select coalesce(max(id), 0) from _utility.vw_metric);
begin
    /*******************************************************************************************************************************
    * Unit begin
    *******************************************************************************************************************************/
    perform _test.unit_begin('Metric');

    /*******************************************************************************************************************************
    * Test correctness
    *******************************************************************************************************************************/
    perform _scd.transaction_create('ADHOC: Test');
    insert into _test.metric_test values (_scd.nextval());

    perform _test.metric_test('root 0', 0);
    perform _test.metric_test('root 1', 1);

    drop table _utility_temp_metric_transaction;
    drop table _scd_temp_transaction;

    perform _test.metric_test('root 2', 2);

    drop table _utility_temp_metric_process;
    drop table _utility_temp_metric_transaction;

    perform _test.metric_test('root 3', 3);
    perform _scd.transaction_create('ADHOC: Test');
    insert into _test.metric_test values (_scd.nextval());

    perform _utility.metric_process();
    perform _utility.metric_process();

    --Commenting out all the performance tests because pgdev1 is not working
    --for any kind performance testing right now. Uncomment them once the
    --issue with pgdev1 has been fixed.
    /*******************************************************************************************************************************
    * Test process performance
    *******************************************************************************************************************************/
   /* perform _test.test_begin('single function per process performance');
    drop table _utility_temp_metric_process;
    drop table _utility_temp_metric_transaction;

    tsBegin = clock_timestamp();

    for iIndex in 1..iTestTotal loop
        perform _test.metric_test('root per process performance', 0);
        drop table _utility_temp_metric_process;
        drop table _utility_temp_metric_transaction;
    end loop;

    vDelta = (clock_timestamp() - tsBegin) / iTestTotal;

    if vDelta > interval '10ms' then
        perform _test.test_fail('performance not acceptable: ' || vDelta || ' per execution');
    else
        perform _test.test_pass();    
    end if;*/

    /*******************************************************************************************************************************
    * Test transaction performance
    *******************************************************************************************************************************/
    /*perform _test.test_begin('single function per transaction performance');

    tsBegin = clock_timestamp();

    for iIndex in 1..iTestTotal loop
        perform _test.metric_test('root per transaction performance', 0);
        drop table _utility_temp_metric_transaction;
    end loop;

    vDelta = (clock_timestamp() - tsBegin) / iTestTotal;

    if vDelta > interval '5ms' then
        perform _test.test_fail('performance not acceptable: ' || vDelta || ' per execution');
    else
        perform _test.test_pass();    
    end if;*/

    /*******************************************************************************************************************************
    * Test root performance
    *******************************************************************************************************************************/
    /*perform _test.test_begin('single function per root performance');

    tsBegin = clock_timestamp();

    for iIndex in 1..iTestTotal loop
        perform _test.metric_test('root per root performance', 0);
    end loop;

    vDelta = (clock_timestamp() - tsBegin) / iTestTotal;

    if vDelta > interval '3ms' then
        perform _test.test_fail('performance not acceptable: ' || vDelta || ' per execution');
    else
        perform _test.test_pass();    
    end if;*/

    /*******************************************************************************************************************************
    * Test deep function performance
    *******************************************************************************************************************************/
    /*perform _test.test_begin('deep function performance');

    iTestTotal = 100;
    tsBegin = clock_timestamp();

    for iIndex in 1..iTestTotal loop
        perform _test.metric_test('root deep function performance', 3);
    end loop;

    vDelta = (clock_timestamp() - tsBegin) / (iTestTotal * 16);

    if vDelta > interval '2ms' then
        perform _test.test_fail('performance not acceptable: ' || vDelta || ' per execution');
    else
        perform _test.test_pass();    
    end if;*/

    /*******************************************************************************************************************************
    * Check the final row count in vw_metric
    *******************************************************************************************************************************/
    /*perform _test.test_begin('vw_metric final row total');

    iExpectedTotal = 1926;

    select count(*)
      into iActualTotal
      from _utility.vw_metric
     where id > lMetricMaxId;

    if iExpectedTotal <> iActualTotal then
        perform _test.test_fail('expected = ' || iExpectedTotal || ', actual =  ' || iActualTotal);
    else
        perform _test.test_pass();    
    end if;*/

    /*******************************************************************************************************************************
    * Unit end
    *******************************************************************************************************************************/
    perform _test.unit_end();
end $$;

/*
rollback

select * from pg_stat_activity where pid = pg_backend_pid();

select * from _utility.metric_raw_process
select * from _utility.metric_raw_transaction
select * from _utility.metric_raw_transaction_journal_map
select * from _utility.metric_raw_query
select * from _utility.metric_raw_begin
select * from _utility.metric_raw_end

select _utility.metric_process()

select * from _utility.metric
select * from _utility.vw_metric
select * from _utility.metric_process
select * from _utility.vw_metric_process
select * from _utility.vw_metric_transaction order by id
select * from _utility.vw_metric_query

select * from temp_metric_process

set role rn_rtp;
select txid_current();


*/
