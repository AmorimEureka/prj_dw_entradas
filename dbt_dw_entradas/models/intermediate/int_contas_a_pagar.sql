{{
    config(
        tags = ['entradas']
    )
}}

WITH source_item_previsao
    AS (
        SELECT
            "CD_ITCON_PAG",
            "CD_CON_PAG",
            "NR_PARCELA",
            "DT_VENCIMENTO",
            "DT_PREVISTA_PAG",
            "TP_QUITACAO",
            "VL_DUPLICATA"
        FROM {{ ref('stg_itcon_pag') }}
),
source_previsao
    AS (
        SELECT
            "CD_CON_PAG",
            "CD_PROCESSO",
            "CD_REDUZIDO",
            "CD_TIP_DOC",
            "CD_FORNECEDOR",
            "DS_CON_PAG",
            "NR_DOCUMENTO",
            "NR_SERIE",
            "DT_LANCAMENTO",
            "TP_VENCIMENTO",
            "VL_BRUTO_CONTA"
        FROM {{ ref('stg_con_pag') }}
),
source_pagamentos
    AS (
        SELECT
            "CD_PAGCON_PAG",
            "CD_ITCON_PAG",
            "DS_PAGCON_PAG",
            "DT_PAGAMENTO",
            "DT_BAIXA",
            "TP_PAGAMENTO",
            "VL_ACRESCIMO",
            "VL_DESCONTO",
            "VL_PAGO"
        FROM {{ ref('stg_pagcon_pag') }}
),
source_setor
    AS (
        SELECT
            "CD_CON_PAG",
            "CD_SETOR"
        FROM {{ ref('stg_ratcon_pag') }}
),
treats
    AS (
        SELECT
            sip."CD_ITCON_PAG",
            sp."CD_CON_PAG",
            spg."CD_PAGCON_PAG",
            s."CD_SETOR",
            sp."CD_FORNECEDOR",
            sp."NR_DOCUMENTO",
            sp."NR_SERIE",
            sp."CD_PROCESSO",
            sp."CD_REDUZIDO",
            sp."CD_TIP_DOC",
            sp."DS_CON_PAG",
            spg."DS_PAGCON_PAG",
            sp."DT_LANCAMENTO",
            sip."DT_VENCIMENTO",
            sip."DT_PREVISTA_PAG",
            spg."DT_PAGAMENTO",
            sip."TP_QUITACAO",
            sp."TP_VENCIMENTO",
            spg."TP_PAGAMENTO",
            sip."NR_PARCELA",
            sip."VL_DUPLICATA",
            sp."VL_BRUTO_CONTA",
            spg."VL_ACRESCIMO",
            spg."VL_DESCONTO",
            spg."VL_PAGO"
        FROM source_item_previsao sip
        LEFT JOIN source_previsao sp    ON sip."CD_CON_PAG" = sp."CD_CON_PAG"
        LEFT JOIN source_pagamentos spg ON sip."CD_ITCON_PAG" = spg."CD_ITCON_PAG"
        LEFT JOIN source_setor s        ON sp."CD_CON_PAG" = s."CD_CON_PAG"
),
chave_substituta
    AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['"CD_FORNECEDOR"', '"NR_DOCUMENTO"']) }} AS "SK_FORNECEDOR_DOCUMENTO",
            *
        FROM treats
)
SELECT * FROM chave_substituta
