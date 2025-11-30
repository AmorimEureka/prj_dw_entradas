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
            cd_tip_doc,
            ds_tip_doc
        FROM {{ source('raw_entradas_mv', 'tip_doc') }}
),
treats
    AS (
        SELECT
          cd_tip_doc::BIGINT AS "CD_TIP_DOC",
          ds_tip_doc::VARCHAR(33) AS "DS_TIP_DOC"
        FROM source_tipo_documento
)
SELECT * FROM treats