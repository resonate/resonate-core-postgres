/***********************************************************************************************************************************
UTILITY Partition Views
***********************************************************************************************************************************/

/***********************************************************************************************************************************
VW_PARTITION_FLAT_MAP Table
***********************************************************************************************************************************/
create or replace view _utility.vw_partition_flat_map as
select partition_table.schema_name,
       partition_table.name as table_name,
       partition_type.key as column_name,
       partition_map.depth,
       partition.id,
       partition_map_parent.map_id as parent_id,
       partition_type.type,
       partition.name,
       partition.key
  from _utility.partition_table
       inner join _utility.partition_type
            on partition_type.partition_table_id = partition_table.id
       inner join _utility.partition
            on partition.partition_type_id = partition_type.id
       inner join _utility.partition_map
            on partition_map.id = partition.id
           and partition_map.level = 0
       left outer join _utility.partition_map as partition_map_parent
            on partition_map_parent.id = partition.id
           and partition_map_parent.level = -1
 order by partition_table.schema_name,
          partition_table.name,
          coalesce(partition_map_parent.map_id, partition.id),
          partition.id;
