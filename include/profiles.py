

from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

perfil_postgres = ProfileConfig(
    profile_name="dbt_dw_entradas",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_prontocardio",
        profile_args={"schema": "raw_entradas_mv"},
    ),
)
