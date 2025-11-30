{{
    config(
        materialized = 'incremental',
        unique_key = "CD_ENT_SERV",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_ent_serv
    AS (
        SELECT
           cd_ent_serv,
           cd_con_pag,
           cd_fornecedor,
           cd_oficina,
           cd_tip_doc,
           cd_usuario,
           nr_documento,
           nr_serie,
           dt_entrada,
           dt_emissao,
           ds_observacao,
           vl_total
        FROM {{ source('raw_entradas_mv', 'ent_serv') }}
),
treats
    AS (
        SELECT
           cd_ent_serv::BIGINT AS "CD_ENT_SERV",
           cd_con_pag::BIGINT AS "CD_CON_PAG",
           cd_fornecedor::BIGINT AS "CD_FORNECEDOR",
           cd_oficina::BIGINT AS "CD_OFICINA",
           cd_tip_doc::BIGINT AS "CD_TIP_DOC",
           cd_usuario::VARCHAR(20) AS "CD_USUARIO",
           nr_documento::VARCHAR(15) AS "NR_DOCUMENTO",
           nr_serie::VARCHAR(10) AS "NR_SERIE",
           dt_entrada::DATE AS "DT_ENTRADA",
           dt_emissao::DATE AS "DT_EMISSAO",
           ds_observacao::VARCHAR(2000) AS "DS_OBSERVACAO",
           vl_total::NUMERIC(12,2) AS "VL_TOTAL"
        FROM source_ent_serv
)
SELECT * FROM treats
