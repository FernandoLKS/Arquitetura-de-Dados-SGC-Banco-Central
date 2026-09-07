-- Standardize the interface between the Spark-generated Gold layer
-- and the dbt transformation layer.

select

    *

from "bcb"."gold"."macro_monthly"