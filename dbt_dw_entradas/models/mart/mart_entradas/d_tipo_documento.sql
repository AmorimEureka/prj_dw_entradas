{{
    config(
        materialized = 'incremental',
        unique_key = "CD_TIP_DOC",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_tipo_documento
    AS (
        SELECT
            "CD_TIP_DOC",
            "DS_TIP_DOC"
        FROM {{ ref( 'stg_tipo_documento' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_tipo_documento

)
SELECT * FROM treats
