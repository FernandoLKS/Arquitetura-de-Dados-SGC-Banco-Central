BCB_SERIES = {

    # Monetary policy

    "selic": {
        "code": 1178,
        "frequency": "daily",
        "available_from": "1986-06-04",
        "unit": "PERCENT_ANNUAL",
        "description": "Taxa Selic anualizada."
    },

    "selic_meta": {
        "code": 432,
        "frequency": "daily",
        "available_from": "1999-03-05",
        "unit": "PERCENT_ANNUAL",
        "description": "Meta da taxa Selic definida pelo Copom."
    },


    # Inflation

    "ipca": {
        "code": 433,
        "frequency": "monthly",
        "available_from": "1980-01-01",
        "unit": "PERCENT",
        "description": "Indice Nacional de Precos ao Consumidor Amplo."
    },


    # Economic activity

    "atividade_economica": {
        "code": 24363,
        "frequency": "monthly",
        "available_from": "2003-01-01",
        "unit": "INDEX",
        "description": "Indice de Atividade Economica do Banco Central."
    },

    "atividade_agropecuaria": {
        "code": 29601,
        "frequency": "monthly",
        "available_from": "2003-01-01",
        "unit": "INDEX",
        "description": "IBC-Br - Agropecuaria."
    },

    "atividade_industria": {
        "code": 29602,
        "frequency": "monthly",
        "available_from": "2003-01-01",
        "unit": "INDEX",
        "description": "IBC-Br - Industria."
    },

    "atividade_servicos": {
        "code": 29605,
        "frequency": "monthly",
        "available_from": "2003-01-01",
        "unit": "INDEX",
        "description": "IBC-Br - Servicos."
    },

    "atividade_impostos": {
        "code": 29608,
        "frequency": "monthly",
        "available_from": "2003-01-01",
        "unit": "INDEX",
        "description": "IBC-Br - Impostos."
    },

    # Exchange rate

    "dolar_venda": {
        "code": 1,
        "frequency": "daily",
        "available_from": "1984-11-28",
        "unit": "BRL_PER_USD",
        "description": "Taxa de cambio do dolar americano - Venda."
    },

    "dolar_compra": {
        "code": 10813,
        "frequency": "daily",
        "available_from": "1984-11-28",
        "unit": "BRL_PER_USD",
        "description": "Taxa de cambio do dolar americano - Compra."
    },


    # Credit - individuals

    "credito_concedido_pf": {
        "code": 20633,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "BRL_MILLIONS",
        "description": "Concessoes de credito para pessoas fisicas."
    },

    "credito_livre_pf": {
        "code": 20662,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "BRL_MILLIONS",
        "description": "Concessoes de credito livre para pessoas fisicas."
    },

    "saldo_credito_pessoal": {
        "code": 20580,
        "frequency": "monthly",
        "available_from": "2007-03-01",
        "unit": "BRL_MILLIONS",
        "description": "Saldo de credito pessoal."
    },

    "taxa_credito_pf": {
        "code": 27628,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT_ANNUAL",
        "description": "Taxa media de juros do credito nao rotativo para pessoas fisicas."
    },

    "inadimplencia_pf": {
        "code": 21084,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT",
        "description": "Inadimplencia da carteira de credito de pessoas fisicas."
    },

    "inadimplencia_credito_pessoal_pf": {
        "code": 21120,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT",
        "description": "Inadimplencia do credito pessoal para pessoas fisicas."
    },


    # Credit - companies

    "saldo_credito_pj": {
        "code": 22047,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLIONS",
        "description": "Saldo das operacoes de credito para pessoas juridicas."
    },

    "taxa_credito_pj": {
        "code": 20715,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT_ANNUAL",
        "description": "Taxa media de juros das operacoes de credito para pessoas juridicas."
    },

    "taxa_credito_nao_rotativo_pj": {
        "code": 27624,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT_ANNUAL",
        "description": "Taxa media de juros do credito nao rotativo para pessoas juridicas."
    },

    "inadimplencia_pj": {
        "code": 21083,
        "frequency": "monthly",
        "available_from": "2011-03-01",
        "unit": "PERCENT",
        "description": "Inadimplencia da carteira de credito de pessoas juridicas."
    },


    # Households

    "comprometimento_renda": {
        "code": 29266,
        "frequency": "monthly",
        "available_from": "2005-03-01",
        "unit": "PERCENT",
        "description": "Comprometimento de renda das familias."
    },

    "endividamento_familias": {
        "code": 29037,
        "frequency": "monthly",
        "available_from": "2005-01-01",
        "unit": "PERCENT",
        "description": "Endividamento das familias."
    },

    # "endividamento_familias_sem_habitacional": {
    #     "code": 29038,
    #     "frequency": "monthly",
    #     "available_from": "2005-01-01",
    #     "unit": "PERCENT",
    #     "description": "Endividamento das familias, exceto credito habitacional."
    # },


    # Public sector

    "credito_governo_federal": {
        "code": 22025,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLIONS",
        "description": "Saldo das operacoes de credito do Governo Federal."
    },

    "credito_governos_estaduais_municipais": {
        "code": 22026,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLIONS",
        "description": "Saldo das operacoes de credito dos governos estaduais e municipais."
    },


    # Credit by activity

    "credito_agropecuaria": {
        "code": 22027,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLION",
        "description": "Saldo das operacoes de credito ao setor agropecuario."
    },

    "credito_industria": {
        "code": 22043,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLION",
        "description": "Saldo das operacoes de credito ao setor industrial."
    },

    "credito_administracao_publica": {
        "code": 22039,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLION",
        "description": "Saldo das operacoes de credito ao setor de governo."
    },

    "credito_servicos_financeiros": {
        "code": 27742,
        "frequency": "monthly",
        "available_from": "2012-01-01",
        "unit": "BRL_MILLION",
        "description": "Saldo das operacoes de credito ao setor de servicos financeiros e seguros."
    },

    # Public debt

    "divida_mobiliaria_tesouro": {
        "code": 4154,
        "frequency": "monthly",
        "available_from": "2000-01-31",
        "unit": "BRL_MILLIONS",
        "description": "Divida mobiliaria federal em titulos do Tesouro Nacional."
    },

    "divida_indexada_selic": {
        "code": 4177,
        "frequency": "monthly",
        "available_from": "2000-01-31",
        "unit": "PERCENT",
        "description": "Participacao da divida mobiliaria indexada a Selic."
    },

    "divida_indexada_cambio": {
        "code": 4173,
        "frequency": "monthly",
        "available_from": "2000-01-31",
        "unit": "PERCENT",
        "description": "Participacao da divida mobiliaria indexada ao cambio."
    },

    "divida_indexada_outros": {
        "code": 4180,
        "frequency": "monthly",
        "available_from": "2000-01-31",
        "unit": "PERCENT",
        "description": "Participacao da divida mobiliaria indexada a outros indexadores."
    }
}


# BCB_SERIES = {

#     # Monetary policy

#     "selic": {
#         "code": 1178,
#         "frequency": "daily",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT_ANNUAL",
#         "description": "Taxa Selic anualizada."
#     },

#     "selic_meta": {
#         "code": 432,
#         "frequency": "daily",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT_ANNUAL",
#         "description": "Meta da taxa Selic definida pelo Copom."
#     },


#     # Inflation

#     "ipca": {
#         "code": 433,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Indice Nacional de Precos ao Consumidor Amplo."
#     },


#     # Economic activity

#     "atividade_economica": {
#         "code": 24363,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "INDEX",
#         "description": "Indice de Atividade Economica do Banco Central."
#     },

#     "atividade_agropecuaria": {
#         "code": 29601,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "INDEX",
#         "description": "IBC-Br - Agropecuaria."
#     },

#     "atividade_industria": {
#         "code": 29602,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "INDEX",
#         "description": "IBC-Br - Industria."
#     },

#     "atividade_servicos": {
#         "code": 29605,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "INDEX",
#         "description": "IBC-Br - Servicos."
#     },

#     "atividade_impostos": {
#         "code": 29608,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "INDEX",
#         "description": "IBC-Br - Impostos."
#     },


#     # Exchange rate

#     "dolar_venda": {
#         "code": 1,
#         "frequency": "daily",
#         "available_from": "2020-01-01",
#         "unit": "BRL_PER_USD",
#         "description": "Taxa de cambio do dolar americano - Venda."
#     },

#     "dolar_compra": {
#         "code": 10813,
#         "frequency": "daily",
#         "available_from": "2020-01-01",
#         "unit": "BRL_PER_USD",
#         "description": "Taxa de cambio do dolar americano - Compra."
#     },


#     # Credit - individuals

#     "credito_concedido_pf": {
#         "code": 20633,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Concessoes de credito para pessoas fisicas."
#     },

#     "credito_livre_pf": {
#         "code": 20662,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Concessoes de credito livre para pessoas fisicas."
#     },

#     "saldo_credito_pessoal": {
#         "code": 20580,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Saldo de credito pessoal."
#     },

#     "taxa_credito_pf": {
#         "code": 27628,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT_ANNUAL",
#         "description": "Taxa media de juros do credito nao rotativo para pessoas fisicas."
#     },

#     "inadimplencia_pf": {
#         "code": 21084,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Inadimplencia da carteira de credito de pessoas fisicas."
#     },

#     "inadimplencia_credito_pessoal_pf": {
#         "code": 21120,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Inadimplencia do credito pessoal para pessoas fisicas."
#     },


#     # Credit - companies

#     "saldo_credito_pj": {
#         "code": 22047,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Saldo das operacoes de credito para pessoas juridicas."
#     },

#     "taxa_credito_pj": {
#         "code": 20715,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT_ANNUAL",
#         "description": "Taxa media de juros das operacoes de credito para pessoas juridicas."
#     },

#     "taxa_credito_nao_rotativo_pj": {
#         "code": 27624,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT_ANNUAL",
#         "description": "Taxa media de juros do credito nao rotativo para pessoas juridicas."
#     },

#     "inadimplencia_pj": {
#         "code": 21083,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Inadimplencia da carteira de credito de pessoas juridicas."
#     },


#     # Households

#     "comprometimento_renda": {
#         "code": 29266,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Comprometimento de renda das familias."
#     },

#     "endividamento_familias": {
#         "code": 29037,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Endividamento das familias."
#     },

#     "endividamento_familias_sem_habitacional": {
#         "code": 29038,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Endividamento das familias, exceto credito habitacional."
#     },


#     # Public sector

#     "credito_governo_federal": {
#         "code": 22025,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Saldo das operacoes de credito do Governo Federal."
#     },

#     "credito_governos_estaduais_municipais": {
#         "code": 22026,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Saldo das operacoes de credito dos governos estaduais e municipais."
#     },


#     # Credit by activity

#     "credito_agropecuaria": {
#         "code": 22027,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLION",
#         "description": "Saldo das operacoes de credito ao setor agropecuario."
#     },

#     "credito_industria": {
#         "code": 22043,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLION",
#         "description": "Saldo das operacoes de credito ao setor industrial."
#     },

#     "credito_administracao_publica": {
#         "code": 22039,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLION",
#         "description": "Saldo das operacoes de credito ao setor de governo."
#     },

#     "credito_servicos_financeiros": {
#         "code": 27742,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLION",
#         "description": "Saldo das operacoes de credito ao setor de servicos financeiros e seguros."
#     },


#     # Public debt

#     "divida_mobiliaria_tesouro": {
#         "code": 4154,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "BRL_MILLIONS",
#         "description": "Divida mobiliaria federal em titulos do Tesouro Nacional."
#     },

#     "divida_indexada_selic": {
#         "code": 4177,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Participacao da divida mobiliaria indexada a Selic."
#     },

#     "divida_indexada_cambio": {
#         "code": 4173,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Participacao da divida mobiliaria indexada ao cambio."
#     },

#     "divida_indexada_outros": {
#         "code": 4180,
#         "frequency": "monthly",
#         "available_from": "2020-01-01",
#         "unit": "PERCENT",
#         "description": "Participacao da divida mobiliaria indexada a outros indexadores."
#     }
# }
