{{
    config(
        materialized = 'incremental',
        unique_key = "CD_RATCON_PAG",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_ratcon_pag
    AS (
        SELECT
            cd_ratcon_pag,
            cd_con_pag,
            cd_setor,
            cd_reduzido,
            cd_item_res,
            dt_competencia,
            vl_rateio
        FROM {{ source('raw_entradas_mv', 'ratcon_pag') }}
),
treats
    AS (
        SELECT
            cd_ratcon_pag::BIGINT AS "CD_RATCON_PAG",
            cd_con_pag::BIGINT AS "CD_CON_PAG",
            cd_setor::BIGINT AS "CD_SETOR",
            cd_reduzido::BIGINT AS "CD_REDUZIDO",
            cd_item_res::BIGINT AS "CD_ITEM_RES",
            dt_competencia::DATE AS "DT_COMPETENCIA",
            vl_rateio::NUMERIC(12,2) AS "VL_RATEIO"
        FROM source_ratcon_pag
)
SELECT * FROM treats
