CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.macro_monthly (
    reference_month TIMESTAMP PRIMARY KEY,

    atividade_agropecuaria DOUBLE PRECISION,
    atividade_economica DOUBLE PRECISION,
    atividade_impostos DOUBLE PRECISION,
    atividade_industria DOUBLE PRECISION,
    atividade_servicos DOUBLE PRECISION,
    comprometimento_renda DOUBLE PRECISION,

    credito_administracao_publica DOUBLE PRECISION,
    credito_agropecuaria DOUBLE PRECISION,
    credito_concedido_pf DOUBLE PRECISION,
    credito_governo_federal DOUBLE PRECISION,
    credito_governos_estaduais_municipais DOUBLE PRECISION,
    credito_industria DOUBLE PRECISION,
    credito_livre_pf DOUBLE PRECISION,
    credito_servicos_financeiros DOUBLE PRECISION,

    divida_indexada_cambio DOUBLE PRECISION,
    divida_indexada_outros DOUBLE PRECISION,
    divida_indexada_selic DOUBLE PRECISION,
    divida_mobiliaria_tesouro DOUBLE PRECISION,

    dolar_compra DOUBLE PRECISION,
    dolar_venda DOUBLE PRECISION,

    endividamento_familias DOUBLE PRECISION,
    -- endividamento_familias_sem_habitacional DOUBLE PRECISION,

    inadimplencia_credito_pessoal_pf DOUBLE PRECISION,
    inadimplencia_pf DOUBLE PRECISION,
    inadimplencia_pj DOUBLE PRECISION,

    ipca DOUBLE PRECISION,

    saldo_credito_pessoal DOUBLE PRECISION,
    saldo_credito_pj DOUBLE PRECISION,

    selic DOUBLE PRECISION,
    selic_meta DOUBLE PRECISION,

    taxa_credito_nao_rotativo_pj DOUBLE PRECISION,
    taxa_credito_pf DOUBLE PRECISION,
    taxa_credito_pj DOUBLE PRECISION
);