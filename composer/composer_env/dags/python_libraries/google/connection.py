from google.oauth2 import service_account
def connect(config_path=None, info=None, scopes = None, subject = None):
  """Creates a service account connection object that can be used to access Google Cloud APIs.

  The connect function creates a service account connection object that can be used 
  to access Google Cloud APIs. It takes three arguments: config_path, which is the 
  path to the service account key file, scopes, which is a list of the scopes that 
  the connection object should have, and subject, which is the email address of the 
  user who will be impersonated by the connection object. The function returns a 
  service account connection object.

  Inside the function, the service_account.Credentials.from_service_account_file 
  function is used to create a service account credentials object. The credentials 
  object is then used to create a service account connection object. The connection 
  object is returned to the caller.

  Args:
    config_path: Path to the service account key file. (Optional)
    info: JSON secret containing service account credentials. (Optional)
    scopes: List of the scopes that the connection object should have. (Optional)
    subject: Email address of the user who will be impersonated by the connection object. (Optional)

  Returns:
  """

  if config_path is not None:
    creds = service_account.Credentials.from_service_account_file(filename=config_path, scopes=scopes, subject=subject)
  elif info is not None:
    creds = service_account.Credentials.from_service_account_info(info=info, scopes=scopes, subject=subject)
  else:
    raise("MISSING CREDENTIALS ENTER VALUE FOR CONNECT CONFIG PATH OR INFO")

  return creds