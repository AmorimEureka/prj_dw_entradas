{{
    config(
        materialized = 'incremental',
        unique_key = "CD_FORNECEDOR",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_fornecedores
    AS (
        SELECT
          "CD_FORNECEDOR",
          "CD_CIDADE",
          "CD_SETOR",
          "NM_FORNECEDOR",
          "NM_FANTASIA",
          "NR_CGC_CPF" AS "CNPJ"
        FROM {{ ref( 'stg_fornecedores' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_fornecedores

)
SELECT * FROM treats
