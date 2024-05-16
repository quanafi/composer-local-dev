from python_libraries.ecoplumbersdb import create_connection
from airflow.decorators import dag, task
import pendulum

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 3, tz="UTC"),
    catchup=False,
    tags=["tests"],
)
def test_postgres():
    @task()
    def test_dag_sql_engine(con):
        from pandas import read_sql_query
        result = read_sql_query("select * from edw2.jobs limit 1;",con)
        print(result)
        return result

    con = create_connection('ecoplumbersdb')
    test_dag_sql_engine(con)

test_postgres()