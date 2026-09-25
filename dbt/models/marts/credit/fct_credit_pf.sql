-- Create analytical indicators for individual credit.
-- The model combines credit concessions, outstanding balances,
-- interest rates, and default indicators with their variations.

select

    reference_month,

    credito_concedido_pf,

    credito_livre_pf,

    saldo_credito_pessoal,

    taxa_credito_pf,

    inadimplencia_pf,

    inadimplencia_credito_pessoal_pf,

    -- Absolute monthly change in credit concessions
    credito_concedido_pf
        - lag(credito_concedido_pf) over (
            order by reference_month
        ) as credito_concedido_change,

    -- Monthly-over-month credit concession growth
    (
        credito_concedido_pf /
        lag(credito_concedido_pf) over (
            order by reference_month
        ) - 1
    ) * 100 as credito_concedido_mom,

    -- Year-over-year credit concession growth
    (
        credito_concedido_pf /
        lag(credito_concedido_pf, 12) over (
            order by reference_month
        ) - 1
    ) * 100 as credito_concedido_yoy,

    -- Absolute monthly change in the individual default rate
    inadimplencia_pf
        - lag(inadimplencia_pf) over (
            order by reference_month
        ) as inadimplencia_change

from {{ ref('stg_macro_monthly') }}