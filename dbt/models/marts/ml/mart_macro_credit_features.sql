-- Create a feature dataset for statistical and machine learning models.
-- Lagged macroeconomic variables are included to capture potential
-- delayed effects of economic conditions on credit risk.

select

    reference_month,

    selic,
    selic_meta,

    -- Lagged Selic features
    lag(selic, 1) over (
        order by reference_month
    ) as selic_lag_1,

    lag(selic, 3) over (
        order by reference_month
    ) as selic_lag_3,

    lag(selic, 6) over (
        order by reference_month
    ) as selic_lag_6,

    lag(selic, 12) over (
        order by reference_month
    ) as selic_lag_12,

    ipca,

    atividade_economica,

    dolar_venda,

    credito_concedido_pf,

    taxa_credito_pf,

    inadimplencia_pf,

    -- Lagged default rate features
    lag(inadimplencia_pf, 1) over (
        order by reference_month
    ) as inadimplencia_lag_1,

    lag(inadimplencia_pf, 3) over (
        order by reference_month
    ) as inadimplencia_lag_3

from {{ ref('stg_macro_monthly') }}