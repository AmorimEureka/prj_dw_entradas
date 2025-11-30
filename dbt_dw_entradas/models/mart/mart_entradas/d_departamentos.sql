{{
    config(
        materialized = 'incremental',
        unique_key = "CD_SETOR",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_departamentos
    AS (
        SELECT
            "CD_SETOR",
            "NM_SETOR"
        FROM {{ ref( 'stg_departamentos' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_departamentos

)
SELECT * FROM treats
