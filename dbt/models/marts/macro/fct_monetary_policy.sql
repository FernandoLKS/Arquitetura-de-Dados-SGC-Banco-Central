-- Create analytical indicators related to monetary policy.
-- The model derives changes in the Selic rate and the spread
-- between the effective Selic rate and its target.

select

    reference_month,

    selic,

    selic_meta,

    -- Difference between the effective Selic rate and its target
    selic - selic_meta as selic_spread,

    -- Selic rate from the previous month
    lag(selic) over (
        order by reference_month
    ) as selic_previous_month,

    -- Absolute monthly change in the Selic rate
    selic - lag(selic) over (
        order by reference_month
    ) as selic_change,

    -- Selic target from the previous month
    lag(selic_meta) over (
        order by reference_month
    ) as selic_meta_previous_month,

    -- Absolute monthly change in the Selic target
    selic_meta - lag(selic_meta) over (
        order by reference_month
    ) as selic_meta_change

from {{ ref('stg_macro_monthly') }}