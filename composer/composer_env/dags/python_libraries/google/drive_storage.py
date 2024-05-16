import pandas as pd
import json, random, re, io

from datetime import datetime, timedelta
from PIL import Image, ImageChops

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.cloud import storage

def download_media_file(credentials, real_file_id, verbose=False):
  """Downloads a media file from a drive that the connection object can access.

  The download_media_file function downloads a media file from a Google Drive 
  that the provided credentials have access to. It takes two arguments: credentials,
  which is a service account connection object, and real_file_id, which is the ID of
  the file to be downloaded. The function returns an IO object with the location of 
  the downloaded file. 
  
  Inside the function, the credentials are used to create a Drive API client. 
  The file ID is then used to make a request to the Drive API to get the media content 
  of the file. The media content is downloaded in chunks and stored in an IO object. 
  If the verbose parameter is set to True, the function will print the progress of the 
  download. If an error occurs during the download, the function will print the error 
  message and return None.

  Args:
    credentials: Service Account connection. (See connection.connect())
    real_file_id: ID of the file to download
  Returns : IO object with location with media based download object.
  """

  try:
    # create drive api client
    service = build("drive", "v3", credentials=credentials)

    file_id = real_file_id

    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
      status, done = downloader.next_chunk()
      if verbose:
        print(f"Download {int(status.progress() * 100)}.")
    return io.BytesIO(file.getvalue())

  except HttpError as error:
    print(f"An error occurred: {error}")
    return None
  
def view_image(credentials, id):
  """Downloads and displays an image from a drive that the connection object can access.

  The view_image function downloads an image file from a Google Drive that the provided 
  credentials have access to and displays it using the Python Imaging Library (PIL). 
  It takes two arguments: credentials, which is a service account connection object, and 
  id, which is the ID of the image file to be downloaded. The function returns the 
  downloaded image as a PIL Image object.

  Inside the function, the download_media_file function is used to download the image file 
  from the Drive. The downloaded image is then opened using the PIL Image.open function. 
  Finally, the PIL Image.show function is used to display the image in a window.

  Args:
    credentials: Service Account connection. (See connection.connect())
    id: ID of the image file to download
  Returns: PIL Image object of the downloaded image.
  """
  
  image = download_media_file(credentials, id, True)
  image = Image.open(image)
  return image.show()

def list_files(credentials, query=None):
  """Lists files in a Google Drive that the connection object can access.

  The list_files function lists the files in a Google Drive that the provided connection 
  object has access to. It takes two arguments: connection, which is a service account 
  connection object, and query, which is a string that can be used to filter the files 
  that are returned. The function returns a Pandas DataFrame with the following columns: 
  id, name, createdTime, modifiedTime, mimeType, properties, and parents.

  Inside the function, the connection object is used to create a Drive API client. The 
  client is then used to make a request to the Drive API to list the files in the drive. 
  The files are returned in a list of dictionaries, and the function converts the list of 
  dictionaries to a Pandas DataFrame. The DataFrame is then returned to the caller.

  Args:
    connection: Service Account connection. (See connection.connect())
    query: String used to filter the files that are returned. (Optional)
  Returns: Pandas DataFrame with the following columns: id, name, createdTime, modifiedTime, 
  mimeType, properties, and parents.
  """

  try:
    # create drive api client
    service = build("drive", "v3", credentials=credentials)
    files = []
    page_token = None
    while True:
      response = (
          service.files()
          .list(
              # q="'1mcE-2DsJITVxDcM_4nA3zeoZVHywdHbO' in parents and mimeType contains 'image/'",
              # q="mimeType = 'application/vnd.google-apps.folder'",
              q=query,
              corpora="drive",
              driveId="0AFU_e8ZR2Bv2Uk9PVA",
              fields="nextPageToken, files(id, name, createdTime, modifiedTime, mimeType, properties, parents)",
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              pageToken=page_token,
          )
          .execute()
      )
      files.extend(response.get("files", []))
      page_token = response.get("nextPageToken", None)
      if page_token is None:
        break

  except HttpError as error:
    print(f"An error occurred: {error}")
    files = None
  df = pd.json_normalize(files)
  df['parents'] = df['parents'].str[0]
  return df

def create_folder_in_folder(credentials, target_folder_id, folder_name, user_id):
  """Creates a new folder in a specified folder in a Google Drive that the connection object can access.

  The create_folder_in_folder function creates a new folder in a specified folder 
  in a Google Drive that the provided connection object has access to. It takes 
  four arguments: connection, which is a service account connection object, 
  target_folder_id, which is the ID of the folder in which the new folder will be 
  created, folder_name, which is the name of the new folder, and user_id, which is 
  the HR ID of a user who the folder is meant to represent. The function returns the 
  Drive ID of the newly created folder.

  Inside the function, the connection object is used to create a Drive API client. 
  The client is then used to make a request to the Drive API to create the new folder. 
  The request body includes the name of the new folder, the ID of the parent folder, 
  and the ID of the user who will own the new folder. If the request is successful, 
  the function returns the ID of the newly created folder. If an error occurs during 
  the request, the function prints the error message and returns None.

  Args:
    connection: Service Account connection. (See connection.connect())
    target_folder_id: ID of the folder in which the new folder will be created.
    folder_name: Name of the new folder.
    user_id: ID of the user who will own the new folder.
  Returns: ID of the newly created folder.
  """

  try:
    # create drive api client
    service = build("drive", "v3", credentials=credentials)

    # pylint: disable=maybe-no-member

    file_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [target_folder_id],
        "properties":
            {
              "user_id": user_id
            }
    }

    # Create File
    file = service.files().create(
      body=file_metadata, 
      fields="id",
      supportsAllDrives=True,
      ).execute()

    print(f'File ID: "{file.get("id")}".')
    return file.get("id")

  except HttpError as error:
    print(f"An error occurred: {error}")
    return None

def move_file_to_folder(credentials, file_id, folder_id):
  """Move specified file to the specified folder.
  Args:
      file_id: Id of the file to move.
      folder_id: Id of the folder
  Print: An object containing the new parent folder and other meta data
  Returns : Parent Ids for the file

  Load pre-authorized user credentials from the environment.
  """

  try:
    # call drive api client
    service = build("drive", "v3", credentials=credentials)

    # pylint: disable=maybe-no-member
    # Retrieve the existing parents to remove
    file = (
        service.files()
        .get(
            fileId=file_id, 
            fields="parents",
            supportsAllDrives=True,
            )
            .execute()
    )
    previous_parents = ",".join(file.get("parents"))
    # Move the file to the new folder
    file = (
        service.files()
        .update(
            fileId=file_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents",
            supportsAllDrives=True,
        )
        .execute()
    )
    return file.get("parents")

  except HttpError as error:
    print(f"An error occurred: {error}")
    return None