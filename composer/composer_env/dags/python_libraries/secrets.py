def get_json_secret(secret_name):
    """Retrieves a secret from Airflow's Variable store and parses it as JSON.

    Args:
        secret_name: The name of the secret to retrieve.

    Returns:
        Json Object
    """
    from json import loads
    from airflow.models.variable import Variable
    # Secret: airflow-variables-test-value gives variable value 'test-value'
    secret_json = Variable.get(secret_name)
    return loads(secret_json)

def get_secret(secret_name):
    """Retrieves a secret from Airflow's Variable store.

    Args:
        secret_name: The name of the secret to retrieve.

    Returns:
        The secret value.
    """
    from airflow.models.variable import Variable
    return Variable.get(secret_name)