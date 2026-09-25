-- Create analytical indicators for the BRL/USD exchange rate.
-- Daily exchange rate series have already been aggregated
-- to monthly averages by Spark.

select

    reference_month,

    dolar_compra,

    dolar_venda,

    -- Average between the purchase and selling exchange rates
    (dolar_compra + dolar_venda) / 2 as dolar_medio,

    -- Difference between selling and purchase rates
    dolar_venda - dolar_compra as spread_cambio,

    -- Absolute monthly change in the selling exchange rate
    dolar_venda
        - lag(dolar_venda) over (
            order by reference_month
        ) as cambio_change,

    -- Monthly-over-month exchange rate growth
    (
        dolar_venda /
        lag(dolar_venda) over (
            order by reference_month
        ) - 1
    ) * 100 as cambio_mom,

    -- Year-over-year exchange rate growth
    (
        dolar_venda /
        lag(dolar_venda, 12) over (
            order by reference_month
        ) - 1
    ) * 100 as cambio_yoy

from {{ ref('stg_macro_monthly') }}