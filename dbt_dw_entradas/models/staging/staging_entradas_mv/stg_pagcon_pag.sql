{{
    config(
        materialized = 'incremental',
        unique_key = "CD_PAGCON_PAG",
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_pagcon_pag
    AS  (
        SELECT
            cd_pagcon_pag,
            cd_itcon_pag,
            ds_pagcon_pag,
            dt_pagamento,
            dt_baixa,
            tp_pagamento,
            vl_acrescimo,
            vl_desconto,
            vl_pago
        FROM {{ source('raw_entradas_mv', 'pagcon_pag') }}
),
treats
    AS (
        SELECT
            cd_pagcon_pag::BIGINT AS "CD_PAGCON_PAG",
            cd_itcon_pag::BIGINT AS "CD_ITCON_PAG",
            ds_pagcon_pag::VARCHAR AS "DS_PAGCON_PAG",
            dt_pagamento::DATE AS "DT_PAGAMENTO",
            dt_baixa::DATE AS "DT_BAIXA",
            tp_pagamento::VARCHAR(1) AS "TP_PAGAMENTO",
            vl_acrescimo::NUMERIC(12,2) AS "VL_ACRESCIMO",
            vl_desconto::NUMERIC(12,2) AS "VL_DESCONTO",
            vl_pago::NUMERIC(12,2) AS "VL_PAGO"
        FROM source_pagcon_pag
)
SELECT * FROM treats
