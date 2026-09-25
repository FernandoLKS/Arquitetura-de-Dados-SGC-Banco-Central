-- Standardize the interface between the Spark-generated Gold layer
-- and the dbt transformation layer.

select

    *

from {{ source('gold', 'macro_monthly') }}