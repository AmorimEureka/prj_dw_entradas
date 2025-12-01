{{
    config(
        tags = ['entradas']
    )
}}

WITH source_ent_serv
    AS (
        SELECT
            cd_ent_serv,
            cd_con_pag,
            cd_fornecedor,
            nr_documento,
            nr_serie,
            cd_oficina,
            cd_tip_doc,
            cd_usuario,
            dt_entrada,
            dt_emissao,
            ds_observacao,
            vl_total
        FROM {{ ref('stg_ent_serv') }}
),
chave_substituta
     AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['cd_fornecedor', 'nr_documento']) }} AS sk_fornecedor_documento,
            *
        FROM source_ent_serv
)
SELECT * FROM chave_substituta