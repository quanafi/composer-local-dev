from python_libraries.secrets import get_json_secret
from airflow.decorators import dag, task
import pendulum

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 3, tz="UTC"),
    catchup=False,
    tags=["tests"],
)
def test_secret():
    @task()
    def test_secret_retrieval(secret_name):
        # Secret: airflow-variables-test-value gives variable value 'test-value'
        test_dict = get_json_secret(secret_name)
        print(test_dict['TEST_KEY'])
        print(test_dict['TEST_NAME'])
        return test_dict
    test_secret_retrieval('test-value')
test_secret()