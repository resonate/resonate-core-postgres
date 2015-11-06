/***********************************************************************************************************************************
* UTILITY Metric Views
***********************************************************************************************************************************/

/***********************************************************************************************************************************
* VW_METRIC_PROCESS View
***********************************************************************************************************************************/
create or replace view _utility.vw_metric_process as
select id,
       user_name,
       application_name,
       client_address,
       client_hostname,
       timestamp_begin
  from _utility.metric_raw_process
    union
select *
  from _utility.metric_process
order by id;
  
comment on view _utility.vw_metric_process is
'Combines {{_utility.metric_process}} and {{_utility.metric_raw_process}} so that unprocessed metrics can be queried with
processed metrics.';

/***********************************************************************************************************************************
* VW_METRIC_TRANSACTION View
***********************************************************************************************************************************/
create or replace view _utility.vw_metric_transaction as
select metric_raw_transaction.id,
       metric_raw_transaction.metric_process_id,
       metric_raw_transaction_journal_map.journal_transaction_id,
       metric_raw_transaction.timestamp_begin
  from _utility.metric_raw_transaction
       left outer join _utility.metric_raw_transaction_journal_map
            on metric_raw_transaction_journal_map.metric_transaction_id = metric_raw_transaction.id
    union
select *
  from _utility.metric_transaction
order by id;
  
comment on view _utility.vw_metric_transaction is
'Combines {{_utility.metric_transaction}} and {{_utility.metric_raw_transaction}} so that unprocessed metrics can be queried with
processed metrics.';

/***********************************************************************************************************************************
* VW_METRIC_QUERY View
***********************************************************************************************************************************/
create or replace view _utility.vw_metric_query as
select metric_raw_query.id,
       vw_metric_transaction.metric_process_id,
       metric_raw_query.metric_transaction_id,
       metric_raw_query.timestamp_begin,
       metric_raw_query.sql
  from _utility.metric_raw_query
       left outer join _utility.vw_metric_transaction
            on vw_metric_transaction.id = metric_raw_query.metric_transaction_id
    union
select *
  from _utility.metric_query
order by id;
  
comment on view _utility.vw_metric_query is
'Combines {{_utility.metric_query}} and {{_utility.metric_raw_query}} so that unprocessed metrics can be queried with
processed metrics.';

/***********************************************************************************************************************************
* VW_METRIC View
***********************************************************************************************************************************/
create or replace view _utility.vw_metric as
select metric_raw_begin.id,
       vw_metric_process.id as metric_process_id,
       vw_metric_transaction.id as metric_transaction_id,
       vw_metric_query.id as metric_query_id,
       metric_raw_begin.parent_id,
       metric_raw_begin.depth,
       metric_raw_begin.current_user_name,
       metric_raw_begin.schema_name,
       metric_raw_begin.function_name,
       metric_raw_end.cached,
       metric_raw_begin.parameter,
       metric_raw_end.result,
       metric_raw_begin.timestamp as timestamp_begin,
       metric_raw_end.timestamp as timestamp_end
  from _utility.metric_raw_begin
       left outer join _utility.vw_metric_query
            on vw_metric_query.id = metric_raw_begin.metric_query_id
       left outer join _utility.vw_metric_transaction
            on vw_metric_transaction.id = vw_metric_query.metric_transaction_id
       left outer join _utility.vw_metric_process
            on vw_metric_process.id = vw_metric_transaction.metric_process_id
       left outer join _utility.metric_raw_end
            on metric_raw_end.id = metric_raw_begin.id
    union
select *
  from _utility.metric
order by id;
  
comment on view _utility.vw_metric is
'Combines {{_utility.metric}}, {{_utility.metric_raw_begin}} and {{_utility.metric_raw_end}} so that unprocessed metrics
can be queried with processed metrics.';
