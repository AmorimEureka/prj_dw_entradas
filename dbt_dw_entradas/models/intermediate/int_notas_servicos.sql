{{
    config(
        tags = ['entradas']
    )
}}

WITH source_ent_serv
    AS (
        SELECT
            "CD_ENT_SERV",
            "CD_CON_PAG",
            "CD_FORNECEDOR",
            "NR_DOCUMENTO",
            "NR_SERIE",
            "CD_OFICINA",
            "CD_TIP_DOC",
            "CD_USUARIO",
            "DT_ENTRADA",
            "DT_EMISSAO",
            "DS_OBSERVACAO",
            "VL_TOTAL"
        FROM {{ ref('stg_ent_serv') }}
),
chave_substituta
     AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['"CD_FORNECEDOR"', '"NR_DOCUMENTO"']) }} AS "SK_FORNECEDOR_DOCUMENTO",
            *
        FROM source_ent_serv
)
SELECT * FROM chave_substituta