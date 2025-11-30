{{
    config(
        materialized = 'incremental',
        unique_key = "CD_FORNECEDOR",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_fornecedor
    AS (
        SELECT
          cd_fornecedor,
          cd_cidade,
          cd_setor,
          nm_fornecedor,
          nm_fantasia,
          nr_cgc_cpf
        FROM {{ source('raw_entradas_mv', 'fornecedor') }}
),
treats
    AS (
        SELECT
          cd_fornecedor::BIGINT AS "CD_FORNECEDOR",
          cd_cidade::BIGINT AS "CD_CIDADE",
          cd_setor::BIGINT AS "CD_SETOR",
          nm_fornecedor::VARCHAR(250) AS "NM_FORNECEDOR",
          nm_fantasia::VARCHAR(100) AS "NM_FANTASIA",
          nr_cgc_cpf::VARCHAR(60) AS "NR_CGC_CPF"
        FROM source_fornecedor
)
SELECT * FROM treats