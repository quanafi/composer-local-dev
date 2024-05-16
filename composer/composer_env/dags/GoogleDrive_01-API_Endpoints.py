# https://developers.google.com/admin-sdk/directory/reference/rest/v1/users.photos/update
# 


import pandas as pd

from datetime import datetime, timedelta

from google.cloud import storage

from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset

from python_libraries.ecoplumbersdb import create_engine
from python_libraries.secrets import get_json_secret
from python_libraries.google.connection import connect
from python_libraries.google.drive_storage import download_media_file, view_image, list_files, create_folder_in_folder, move_file_to_folder

# Google Scripts -------------------------------


def drive_connection():
  """Creates a service account connection object that can be used to access Google Drive.

  The drive_connection function creates a service account connection object that can be 
  used to access the full Eco Google Drive. It takes no arguments and returns a service 
  account connection object.

  Inside the function, the connect function is used to create a service account connection 
  object. The connection object is then returned to the caller.

  Args:
    None
  Returns: Service account connection object.
  """

  creds = connect(info = get_json_secret('gsheets-creds'), scopes=["https://www.googleapis.com/auth/drive"], subject='elliot@geteco.com')

  return creds

def storage_client():
  """Creates a storage client object that can be used to access Google Cloud Storage.

  The `storage_client` function creates a storage client object that can be used to 
  access Google Cloud Storage. It takes no arguments and returns a storage client object.

  Inside the function, the connect function is used to create a service account 
  connection object. The connection object is then used to create a storage client 
  object. The client object is returned to the caller.

  Args:
    None
  Returns: Storage client object.
  """

  credentials = connect(info = get_json_secret('ebq-service-account-creds'))

  client = storage.Client(project = "data-warehouse-ecobigquery", credentials = credentials)

  return client.bucket("eco-public-bucket-01")

def folder_generator(id, folder_name, file_list_df):
  """Creates a folder in the Eco Google Drive for a specific user if one does not already exist.

  The `folder_generator` function takes three arguments: id, which is the ID of the 
  user for whom the folder will be created, folder_name, which is the name of the 
  folder to be created, and file_list_df, which is a Pandas DataFrame containing a 
  list of all the files in the Eco Google Drive. The function first checks if a 
  folder with the specified name already exists for the specified user. If it does, 
  the function returns None. If it does not, the function creates the folder and 
  returns the ID of the newly created folder.

  Inside the function, the drive_connection function is used to create a service 
  account connection object. The connection object is then used to create a Drive 
  API client. The client is then used to make a request to the Drive API to create 
  the new folder. The request body includes the name of the new folder, the ID of 
  the parent folder, and the ID of the user who will own the new folder. If the 
  request is successful, the function returns the ID of the newly created folder. 
  If an error occurs during the request, the function prints the error message 
  and returns None.

  Args:
      id: ID of the user for whom the folder will be created.
      folder_name: Name of the folder to be created.
      file_list_df: Pandas DataFrame containing a list of all the files in the Eco Google Drive.
  Returns: ID of the newly created folder, or None if the folder already exists.
  """
  
  connection = drive_connection()
  df = file_list_df
  if "properties.user_id" not in df.columns:
    df["properties.user_id"] = None
  if str(id) not in df["properties.user_id"].values:
    return create_folder_in_folder(connection, "1HDqzujR7raoiI-go4PsmW_yAY5ae0Eat", folder_name, id)
  else:
    # print(f"{folder_name} with id: {id} already exists.")
    return None

def find_pp(inova_id):
  """Finds the Google Drive ID of the profile picture for a specific user.

  The find_pp function takes one argument: inova_id, which is the ID of the user 
  whose profile picture is being searched for. The function first uses the 
  `list_files` function to search for a folder in the Eco Google Drive that has 
  the specified user ID as a property. If a folder is found, the function then 
  uses the `list_files` function again to search for a file in the folder that has 
  the mime type "image/jpeg". If a file is found, the function returns the ID of 
  the file. If no file is found, the function returns None.

  Inside the function, the drive_connection function is used to create a service 
  account connection object. The connection object is then used to create a 
  Drive API client. The client is then used to make requests to the Drive API 
  to search for the folder and file.

  Also has funny name function name! :D

  Args:
      inova_id: ID of the user whose profile picture is being searched for.
  Returns: ID of the profile picture file, or None if no file is found.
  """

  connection = drive_connection()
  search_string = "properties has {{ key='user_id' and value='{}' }}".format(inova_id)
  try:
    df = list_files(connection, search_string)
    df2 = list_files(connection, f"parents = '{df.iloc[0]['id']}'")
    return df2.iloc[0]['id']
  except:
    return None

def find_and_view_pp(inova_id):
  """Finds the Google Drive ID of the profile picture for a specific user and displays the image.

  The `find_and_view_pp` function takes one argument: inova_id, which is the ID of 
  the user whose profile picture is being searched for. The function first uses 
  the `list_files` function to search for a folder in the Eco Google Drive that 
  has the specified user ID as a property. If a folder is found, the function then 
  uses the `list_files` function again to search for a file in the folder that has 
  the mime type "image/jpeg". If a file is found, the function uses the `view_image`
  function to display the image. If no file is found, the function prints a message 
  indicating that no profile picture was found.

  Inside the function, the drive_connection function is used to create a service 
  account connection object. The connection object is then used to create a Drive 
  API client. The client is then used to make requests to the Drive API to search 
  for the folder and file.

  See `find_pp` function if you want the file id returned.

  Args:
      inova_id: ID of the user whose profile picture is being searched for.
  Returns: None
  """

  connection = drive_connection()
  search_string = "properties has {{ key='user_id' and value='{}' }}".format(inova_id)
  df = list_files(connection, search_string)
  df2 = list_files(connection, f"parents = '{df.iloc[0]['id']}'")
  view_image(df2.iloc[0]['id']) 

def upload_employee_image(io_object,bucket_object,aid):
  """Uploads an employee's profile picture to Google Cloud Storage.

  The upload_employee_image function takes three arguments: io_object, which is 
  a file-like object containing the image data, bucket_object, which is a Google 
  Cloud Storage bucket object, and aid, which is the ID of the employee. The 
  function first creates a blob object in the specified bucket with the specified 
  name and content type. It then uploads the image data to the blob. Finally, 
  the function returns the URL of the uploaded image.

  Inside the function, the blob object is created using the blob method of the 
  bucket object. The content type of the blob is set to "image/jpeg". The image 
  data is then uploaded to the blob using the open method of the blob object. 
  The open method takes a mode argument, which specifies the mode in which the 
  blob should be opened. In this case, the mode is set to "wb", which indicates 
  that the blob should be opened for writing in binary mode. The image data is 
  then written to the blob using the write method of the file-like object. Finally, 
  the URL of the uploaded image is returned using the format method of the f-string.

  Args:
    io_object: A file-like object containing the image data.
    bucket_object: A Google Cloud Storage bucket object.
    aid: The ID of the employee.
  Returns:
    The URL of the uploaded image.
  """


  # Create a blob object
  blob = bucket_object.blob(f"employee_inova_images/{aid}.jpg")
  blob.content_type = "image/jpeg"
  # Upload the image data
  with blob.open("wb") as f:
      f.write(io_object.getvalue())
  if blob: print("Blob was uploaded!")
  photo_url = f"https://storage.googleapis.com/eco-public-bucket-01/employee_inova_images/{aid}.jpg"
  # Note: Add ?ignoreCache=1 to end of url if you want to see current uncached image.
  print(photo_url)
  return photo_url

def search_files(user_id, file_list_df = list_files(drive_connection())):
  """Searches for files in the Eco Google Drive that belong to a specific user.

  The search_files function takes two arguments: user_id, which is the HR ID of the 
  user whose files are being searched for, and file_list_df, which is a Pandas 
  DataFrame containing a list of all the files in the Eco Google Drive. The 
  function first filters the DataFrame to only include the files that belong to 
  the specified user. It then returns the filtered DataFrame.

  Inside the function, the file_list_df DataFrame is filtered using the 
  df[df["parents"]==df[df["properties.user_id"]==user_id]['id'].values[0]]
  expression. This expression first filters the DataFrame to only include the 
  files that have the specified user ID as a property. It then filters the DataFrame 
  again to only include the files that are children of the folder that has the 
  specified user ID as a property. The resulting DataFrame is returned to the caller.

  Args:
      user_id: ID of the user whose files are being searched for.
      file_list_df: Pandas DataFrame containing a list of all the files in the Eco Google Drive. (Optional)
  Returns:
      Pandas DataFrame containing the files that belong to the specified user.
  """
  df = file_list_df
  try:
    return df[df["parents"]==df[df["properties.user_id"]==user_id]['id'].values[0]]
  except:
      return pd.DataFrame()
  
def search_folders(user_id, file_list_df = list_files(drive_connection())):
  """Searches for folders in the Eco Google Drive that belong to a specific user.

  The `search_folders` function takes two arguments: user_id, which is the HR ID of the 
  user whose folders are being searched for, and file_list_df, which is a Pandas 
  DataFrame containing a list of all the files and folders in the Eco Google Drive. 
  The function first filters the DataFrame to only include the folders that belong to 
  the specified user. It then returns the filtered DataFrame.

  Inside the function, the file_list_df DataFrame is filtered using the 
  df[df["properties.user_id"]==user_id] expression. This expression filters the 
  DataFrame to only include the folders that have the specified user ID as a property. 
  The resulting DataFrame is returned to the caller.

  Args:
      user_id: ID of the user whose folders are being searched for.
      file_list_df: Pandas DataFrame containing a list of all the files and folders in the Eco Google Drive. (Optional)
  Returns:
      Pandas DataFrame containing the folders that belong to the specified user.
  """

  df = file_list_df
  try:
    return df[df["properties.user_id"]==user_id]["id"]
  except:
      return pd.DataFrame()

def db_connector(engine):
    """Connects to the Eco Google Drive and uploads any new profile pictures to Google Cloud Storage.

    The `db_connector` function takes one argument: engine, which is a SQLAlchemy 
    engine object. The function first creates a service account connection object 
    to the Eco Google Drive. It then creates a Google Cloud Storage bucket object. 
    The function then queries the Inova edw2.employees table in the Eco data 
    warehouse to get a list of all active employees. For each employee, the 
    function checks if a folder exists in the Eco Google Drive for that employee. 
    If a folder does not exist, the function creates one. The function then 
    searches the Eco Google Drive for any files that belong to the employee. 
    If any files are found, the function checks if the most recently modified 
    file has been updated since the last time the employee's profile picture was 
    updated in the ecoplumbers schema dataset. If the file has been updated, the 
    function downloads the file, uploads it to Google Cloud Storage, and updates 
    the employee's profile picture in the ecoplumbers schema dataset.

    Inside the function, the `drive_connection` function is used to create a service 
    account connection object to the Eco Google Drive. The `storage_client` function 
    is used to create a Google Cloud Storage bucket object. The `list_files` function 
    is used to search the Eco Google Drive for files that belong to a specific user. 
    The `folder_generator` function is used to create a folder in the Eco Google Drive 
    for a specific user if one does not already exist. The `download_media_file` function 
    is used to download a file from the Eco Google Drive. The `upload_employee_image` function 
    is used to upload a file to Google Cloud Storage. The `pd.read_sql` function is used 
    to query the `edw2.employees` table in the Eco BigQuery data warehouse. The 
    `pd.DataFrame.to_sql` function is used to write a DataFrame to the Eco data warehouse.

    Args:
        engine: A SQLAlchemy engine object.
    Returns:
        None
    """

    print("Initializing...", end=" ")
    connection = drive_connection()
    storage_bucket_object = storage_client()

    current_file_list_df = list_files(connection)
    df = pd.read_sql("select * from edw2.employees where status = 'Active'",engine)
    id_df = pd.DataFrame(columns=['inova_id', 'drive_id', 'avatar_url', 'updated_on'])

    table_name = "ecoplumbers.profile_avatars"

    create_table_sql = f"""
    CREATE TABLE if not exists {table_name} (
    inova_id BIGINT PRIMARY KEY,
    drive_id TEXT,
    avatar_url TEXT,
    updated_on TIMESTAMPTZ
    )
    """

    profile_avatars_df = pd.read_sql(f"select * from {table_name}",engine)

    engine.execute(create_table_sql)
    print("Completed!\n")
    print(f"-------- Generating {len(df)} Employee Folders. --------\n")
    for employee, value in df.iterrows():
        
        if folder_generator(value["id"], value["full_name"], current_file_list_df) is not None:
          current_file_list_df = list_files(connection)

        user_drive_image_df = search_files(str(value["id"]), current_file_list_df)
        if not user_drive_image_df.empty:
          drive_modified_on = datetime.fromisoformat(user_drive_image_df["modifiedTime"].values[0][:-1]) - timedelta(hours=4)
          try:
            last_update_on = pd.to_datetime(profile_avatars_df[profile_avatars_df["inova_id"]==value["id"]]["updated_on"].values[0])
            # print(f"Last_updated_on: {last_update_on} Drive_modified_on: {drive_modified_on}")
            if last_update_on < drive_modified_on:
              print(f"New image found for {value['full_name']} - {value['id']}.")
              drive_id = user_drive_image_df["id"].values[0]
              io_object = download_media_file(connection, drive_id)
              photo_url = upload_employee_image(io_object, storage_bucket_object, value["id"])
              temp_df = pd.DataFrame({'inova_id': [value["id"]], 'drive_id': [drive_id], 'avatar_url': [photo_url], 'updated_on': str(datetime.now())})
              id_df = pd.concat([id_df, temp_df], axis=0)
          except:
            drive_id = user_drive_image_df["id"].values[0]
            io_object = download_media_file(connection, drive_id)
            photo_url = upload_employee_image(io_object, storage_bucket_object, value["id"])
            temp_df = pd.DataFrame({'inova_id': [value["id"]], 'drive_id': [drive_id], 'avatar_url': [photo_url], 'updated_on': str(datetime.now())})
            id_df = pd.concat([id_df, temp_df], axis=0)  

    if not id_df.empty:
      id_df.to_sql('profile_avatars_stage',engine,'ecoplumbers', 'replace', index=False)
      engine.execute(f'DELETE FROM {table_name} USING {table_name}_stage WHERE {table_name}."inova_id" = {table_name}_stage."inova_id"')
      engine.execute(f'INSERT INTO {table_name} ("inova_id", "drive_id", "avatar_url", "updated_on") SELECT "inova_id"::bigint, "drive_id", "avatar_url", "updated_on"::timestamp FROM {table_name}_stage')
      print("\nUploading following dataframe to ecoplumbers.profile_avatars.")
      print(id_df)
      print("--------------------------------------------------\n")
      print("Finished folder creation and avatar upload!\n")

    else:
      print("--------------------------------------------------\n")
      print("No new profile pictures have been uploaded.\n")

def db_connector2(engine):
    """Moves folders of terminated employees to a "_Terminated" folder in the Eco Google Drive.

    The `db_connector2` function takes one argument: engine, which is a SQLAlchemy 
    engine object. The function first queries the Inova edw2.employees table in the 
    Eco data warehouse to get a list of all terminated employees. For each terminated 
    employee, the function searches the Eco Google Drive for a folder that belongs to 
    that employee. If a folder is found, the function moves the folder to the "_Terminated" 
    folder. The "_Terminated" folder is located at https://drive.google.com/drive/u/0/folders/1dcY4jOuoRWulTA4gYIAKo2Ek3FnZki4h.

    Inside the function, the `drive_connection` function is used to create a service 
    account connection object to the Eco Google Drive. The `list_files` function is 
    used to search the Eco Google Drive for folders that belong to a specific user. 
    The `search_folders` function is used to search for folders in the Eco Google Drive 
    that belong to a specific user. The `move_file_to_folder` function is used to move 
    a folder to a new folder. The `pd.read_sql` function is used to query the 
    `edw2.employees` table in the Eco BigQuery data warehouse.

    Args:
        engine: A SQLAlchemy engine object.
    Returns:
        None
    """

    df = pd.read_sql("select * from edw2.employees where status = 'Terminated'",engine)
    # print(df)
    connection = drive_connection()
    current_file_list_df = list_files(connection, "'1HDqzujR7raoiI-go4PsmW_yAY5ae0Eat' in parents")
    # print(current_file_list_df)
    print("- Removing the following Terminated Team Members -\n")
    for employee, value in df.iterrows():
        id = search_folders(str(value["id"]), current_file_list_df)
        if not id.empty:
            # print(id.values[0])
            print(f"Moving {id.values[0]} to _Terminated folder @ https://drive.google.com/drive/u/0/folders/1HDqzujR7raoiI-go4PsmW_yAY5ae0Eat")
            move_file_to_folder(connection, id.values[0],"1dcY4jOuoRWulTA4gYIAKo2Ek3FnZki4h")
    print("--------------------------------------------------\n")

with DAG(
    dag_id="GoogleDrive_01-API_Endpoints",
    schedule_interval="@hourly",
    start_date=datetime(2024, 5, 13),
    catchup=False,
    tags=["Google Drive","Pipeline","Schema: ecoplumbers"],
) as dag:
    @task(outlets=Dataset("ecoplumbers.profile_avatars"))
    def move_photos_from_drive_to_storage():
        db_connector(create_engine('ecoplumbersdb'))
    @task()
    def move_photos_from_drive_to_terminated_folder():
        db_connector2(create_engine('ecoplumbersdb'))

    [move_photos_from_drive_to_storage(), move_photos_from_drive_to_terminated_folder()]