{{
    config(
        materialized = 'incremental',
        unique_key = "CD_SETOR",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_setor
    AS (
        SELECT
            cd_setor,
            nm_setor,
            cd_cen_cus
        FROM {{ source('raw_entradas_mv', 'setor') }}
),
treats
    AS (
        SELECT
            cd_setor::BIGINT AS "CD_SETOR",
            nm_setor::VARCHAR(70) AS "NM_SETOR",
            cd_cen_cus::BIGINT AS "CD_CEN_CUS"
        FROM source_setor
)
SELECT * FROM treats