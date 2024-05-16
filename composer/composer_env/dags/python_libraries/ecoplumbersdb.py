from airflow.hooks.postgres_hook import PostgresHook

def create_engine(postgres_conn_id: str):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    return engine

def create_connection(postgres_conn_id: str):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    return engine.connect()