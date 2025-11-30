{{
    config(
        materialized = 'incremental',
        unique_key = "CD_CON_PAG",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_con_pag
    AS (
        SELECT
            cd_con_pag,
            cd_processo,
            cd_reduzido,
            cd_tip_doc,
            cd_fornecedor,
            ds_con_pag,
            nr_documento,
            nr_serie,
            dt_lancamento,
            tp_vencimento,
            vl_bruto_conta
        FROM {{ source('raw_entradas_mv', 'con_pag') }}
),
treats
    AS (
        SELECT
            cd_con_pag::BIGINT AS "CD_CON_PAG",
            cd_processo::BIGINT AS "CD_PROCESSO",
            cd_reduzido::BIGINT AS "CD_REDUZIDO",
            cd_tip_doc::BIGINT AS "CD_TIP_DOC",
            cd_fornecedor::BIGINT AS "CD_FORNECEDOR",
            ds_con_pag::VARCHAR(180) AS "DS_CON_PAG",
            nr_documento::VARCHAR(17) AS "NR_DOCUMENTO",
            nr_serie::VARCHAR(10) AS "NR_SERIE",
            dt_lancamento::DATE AS "DT_LANCAMENTO",
            tp_vencimento::VARCHAR(1) AS "TP_VENCIMENTO",
            vl_bruto_conta::NUMERIC(12,2) AS "VL_BRUTO_CONTA"
        FROM source_con_pag
)
SELECT * FROM treats