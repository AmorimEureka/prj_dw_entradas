{{
    config(
        materialized = 'incremental',
        unique_key = "CD_ENT_SERV",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

SELECT
    "CD_ENT_SERV",
    "SK_FORNECEDOR_DOCUMENTO",
    "CD_CON_PAG",
    "CD_FORNECEDOR",
    "NR_DOCUMENTO",
    "NR_SERIE",
    "CD_OFICINA",
    "CD_TIP_DOC",
    "CD_USUARIO",
    "DT_ENTRADA",
    "DT_EMISSAO",
    "DS_OBSERVACAO",
    "VL_TOTAL"
FROM {{ ref('int_notas_servicos') }}

