/***********************************************************************************************************************
* UTILITY Array Functions
***********************************************************************************************************************/

--User-defined aggregate function that concatenates arrays together
--(similar to array_cat, except it can be run as an aggregate function)
CREATE AGGREGATE _utility.array_cat_agg (anyarray)
(
    sfunc = array_cat,
    stype = anyarray,
    initcond = '{}'
);

