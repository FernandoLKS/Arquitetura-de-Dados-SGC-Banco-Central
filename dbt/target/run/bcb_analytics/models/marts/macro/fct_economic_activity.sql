
  
    

  create  table "bcb"."analytics"."fct_economic_activity__dbt_tmp"
  
  
    as
  
  (
    -- Create analytical indicators for economic activity.
-- Monthly-over-month (MoM) and year-over-year (YoY)
-- growth rates are calculated from the monthly series.

with base as (

    select

        reference_month,

        atividade_economica,
        atividade_agropecuaria,
        atividade_industria,
        atividade_servicos,
        atividade_impostos

    from "bcb"."analytics"."stg_macro_monthly"

),

features as (

    select

        *,

        -- Monthly-over-month economic activity growth
        (
            atividade_economica /
            lag(atividade_economica) over (
                order by reference_month
            ) - 1
        ) * 100 as atividade_economica_mom,

        -- Year-over-year economic activity growth
        (
            atividade_economica /
            lag(atividade_economica, 12) over (
                order by reference_month
            ) - 1
        ) * 100 as atividade_economica_yoy,

        -- Year-over-year industrial activity growth
        (
            atividade_industria /
            lag(atividade_industria, 12) over (
                order by reference_month
            ) - 1
        ) * 100 as industria_yoy,

        -- Year-over-year services activity growth
        (
            atividade_servicos /
            lag(atividade_servicos, 12) over (
                order by reference_month
            ) - 1
        ) * 100 as servicos_yoy

    from base

)

select *

from features
  );
  