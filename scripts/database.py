from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_engine():
    """
    Usa la Airflow Connection 'postgres_dw' (creada automáticamente por
    airflow-init en docker-compose) en lugar de leer variables de entorno
    sueltas. Esto cumple el requisito de la prueba de usar Airflow Connections
    para la base de datos destino.
    """
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    return hook.get_sqlalchemy_engine()