
  create view "bcb"."analytics"."stg_macro_monthly__dbt_tmp"
    
    
  as (
    -- Standardize the interface between the Spark-generated Gold layer
-- and the dbt transformation layer.

select

    *

from "bcb"."gold"."macro_monthly"
  );