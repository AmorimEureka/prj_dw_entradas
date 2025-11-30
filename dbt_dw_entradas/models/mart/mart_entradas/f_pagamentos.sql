{{
    config(
        materialized = 'incremental',
        unique_key = "CD_ITCON_PAG",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

SELECT
    "CD_ITCON_PAG",
    "SK_FORNECEDOR_DOCUMENTO",
    "CD_CON_PAG",
    "CD_PAGCON_PAG",
    "CD_SETOR",
    "CD_FORNECEDOR",
    "NR_DOCUMENTO",
    "NR_SERIE",
    "CD_PROCESSO",
    "CD_REDUZIDO",
    "CD_TIP_DOC",
    "DS_CON_PAG",
    "DS_PAGCON_PAG",
    "DT_LANCAMENTO",
    "DT_VENCIMENTO",
    "DT_PREVISTA_PAG",
    "DT_PAGAMENTO",
    "TP_QUITACAO",
    "TP_VENCIMENTO",
    "TP_PAGAMENTO",
    "NR_PARCELA",
    "VL_DUPLICATA",
    "VL_BRUTO_CONTA",
    "VL_ACRESCIMO",
    "VL_DESCONTO",
    "VL_PAGO"
FROM {{ ref('int_contas_a_pagar') }}
