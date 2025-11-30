{{
    config(
        materialized = 'incremental',
        unique_key = "CD_ITCON_PAG",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_itcon_pag
    AS (
        SELECT
            cd_itcon_pag,
            cd_con_pag,
            nr_parcela,
            dt_vencimento,
            dt_prevista_pag,
            tp_quitacao,
            vl_duplicata
        FROM {{ source('raw_entradas_mv', 'itcon_pag') }}
),
treats
    AS (
        SELECT
            cd_itcon_pag::BIGINT AS "CD_ITCON_PAG",
            cd_con_pag::BIGINT AS "CD_CON_PAG",
            nr_parcela::VARCHAR(2) AS "NR_PARCELA",
            dt_vencimento::DATE AS "DT_VENCIMENTO",
            dt_prevista_pag::DATE AS "DT_PREVISTA_PAG",
            tp_quitacao::VARCHAR(1) AS "TP_QUITACAO",
            vl_duplicata::NUMERIC(12,2) AS "VL_DUPLICATA"
        FROM source_itcon_pag
)
SELECT * FROM treats

