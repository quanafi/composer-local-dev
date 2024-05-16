import requests, io, calendar
import pandas as pd

from datetime import date, datetime
from google.cloud import storage
from google.oauth2 import service_account

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset

from python_libraries.inova.inova import Inova
from python_libraries.utilities import upload_dataframe, generate_select_columns_with_datatypes, progressbar
from python_libraries.ecoplumbersdb import create_engine as get_engine
from python_libraries.secrets import get_json_secret

engine = get_engine('ecoplumbersdb')
inova_credentials = get_json_secret('inova-creds')
inova = Inova(inova_credentials)

def connect():
  """Uses service account to get proper credentials.

  Takes a service account with Domain Wide Deligation and the proper scopes and allows it to proxy
  as a user and accomplish admin actions. Using Elliots account as it has super
  admin permissions.
  """

  creds = service_account.Credentials.from_service_account_info(get_json_secret('ebq-service-account-creds'))

  return creds


def export_to_database(engine, table_name, data, id_column_1, id_column_2="", date_column=None):   
    start = datetime.now().strftime('%I:%M:%S %p')
    print(f"\r{start} - Importing {table_name}.", end="")
    table_name = f"{table_name}"
    # Uploads current data as a stage table
    upload_dataframe(data, f"{table_name}_stage", engine, if_exists="replace")
    print(f"\r{start} - Importing {table_name} Data..", end="")
    engine.execute(f"CREATE TABLE IF NOT EXISTS {table_name} as (SELECT * FROM {table_name}_stage)")
    # Make sure to have atleast id_column_1 in order for script to work with a max of 2 id_columns for now
    select_string_with_dtypes, select_string_without_dtypes = generate_select_columns_with_datatypes(table_name, engine)
    print(f"\r{start} - Importing {table_name} Data...", end="")
    if date_column is not None:
        print(" Running by Date Column...", end="")
        engine.execute(f'DELETE FROM {table_name} USING {table_name}_stage WHERE {table_name}."{date_column}"::date between (select min("{date_column}"::date) from {table_name}_stage) and (select max("{date_column}"::date) from {table_name}_stage)')
        engine.execute(f'INSERT INTO {table_name} ({select_string_without_dtypes}) SELECT {select_string_with_dtypes} FROM {table_name}_stage')
    elif id_column_2 == "":
        engine.execute(f'DELETE FROM {table_name} USING {table_name}_stage WHERE {table_name}."{id_column_1}" = {table_name}_stage."{id_column_1}"')
        engine.execute(f'INSERT INTO {table_name} ({select_string_without_dtypes}) SELECT {select_string_with_dtypes} FROM {table_name}_stage')
    elif id_column_1 == "":
        print("\nError: Needs at least 1 id column")
    else:
        engine.execute(f'DELETE FROM {table_name} USING {table_name}_stage WHERE {table_name}."{id_column_1}" = {table_name}_stage."{id_column_1}" and {table_name}."{id_column_2}" = {table_name}_stage."{id_column_2}"')
        engine.execute(f'INSERT INTO {table_name} ({select_string_without_dtypes}) SELECT {select_string_with_dtypes} FROM {table_name}_stage')
    print(f"✅ - Imported to {table_name}")


def upload_employee_image(photo_href,bucket_object,aid):
        # save image to bucket
        # response = requests.get(photo_href)
        # Create a blob object
        # blob = bucket_object.blob(f"employee_inova_images/{aid}.jpg")
        # blob.content_type = "image/jpeg"
        # Upload the image data
        # with blob.open("wb") as f:
            # f.write(response.content)
        photo_url = f"https://storage.googleapis.com/eco-public-bucket-01/employee_inova_images/{aid}.jpg"
        return photo_url

def create_employees_full(engine):

    df4=pd.DataFrame()
    index = 0
    user_ids = pd.read_sql('select id from inova.employees_data',con=engine)
    # Google Objects
    credentials = connect()
    client = storage.Client(project = "data-warehouse-ecobigquery", credentials = credentials)
    bucket = client.bucket("eco-public-bucket-01")
    start = datetime.now().strftime('%I:%M:%S %p')
    print("")
    for aid in user_ids['id']:
        df4 = pd.concat([df4, pd.json_normalize(inova.endpoint(f'employees/{aid}'))], ignore_index = True)
        df4.reset_index()
        try:
            # save image to bucket and generate url
            url = str(df4['photo_href'][index])
            new_url = upload_employee_image(url,bucket,aid)
            df4.loc[index,'photo_href'] = new_url
        except Exception as err:
            pass
        try:
            job_id = inova.endpoint(f'employees/{aid}/pay-info')['default_job']['id'] # Re-queries user for job_id for job title join in dbt
            df4.loc[index,'job_id'] = job_id
        except Exception as err:
            df4.loc[index,'job_id'] = None
        index +=1
        print(f'\rStart Time: {start} Estimated Time: {round(user_ids.count()[0]*1.10/60,1)} Minutes || Added {index}/{user_ids.count()[0]} employees. {progressbar(index,user_ids.count()[0])} ', end="")
    print(f"\nEmployees Added! Completion Time: {datetime.now().strftime('%I:%M:%S %p')}\n")
    print("Building Employee DF...")
    df4.drop(columns=['social_security','national_id_numbers'], inplace=True)

    # cost centers
    df4 = df4.rename(columns={'cost_centers_info.defaults': 'cost_centers_info'})
    length = len(pd.DataFrame(df4.cost_centers_info.tolist()).columns) + 1
    cost_centers = [f"cost_center_{i}" for i in range(1,length)]
    df4[cost_centers] = pd.DataFrame(df4.cost_centers_info.tolist(), index=df4.index)
    df4.pop('cost_centers_info')
    for cost_center in cost_centers:
        normalized_df = pd.json_normalize(df4[cost_center]).rename(columns= {'value.id': cost_center})
        normalized_df.pop('index')
        df4.pop(cost_center)
        df4 = pd.concat([df4,normalized_df], axis=1)
    df4.reset_index()
    print("Completed Cost Centers.")
    
    # managers
    df4['managers'].fillna('',inplace=True)
    df4[['manager_1','manager_2']] = pd.DataFrame(df4.managers.tolist(), index=df4.index)
    df4.pop('managers')
    A = pd.json_normalize(df4.manager_1).rename(columns= {'empl_ref.account_id': 'manager_1_id'}) 
    B = pd.json_normalize(df4.manager_2).rename(columns= {'empl_ref.account_id': 'manager_2_id'}) 
    A.pop('index')
    B.pop('index')
    df4.pop('manager_1')
    df4.pop('manager_2')
    df4 = pd.concat([df4,A,B], axis=1)
    print("Completed Managers.")

    export_to_database(engine,"inova.employees_full_data", df4, "id")

# Variables for query parameters

today = date.today()
bom = today.replace(day = 1)
eom = today.replace(day=calendar.monthrange(today.year, today.month)[1])

with DAG(
    dag_id="Inova_01_API_ENDPOINTS",
    schedule_interval="5 5 * * *",
    start_date=datetime(2024, 4, 28),
    catchup=False,
    tags=["Inova","API Import","Schema: inova"],
) as dag:

    # Database Connection
    @task()
    def build_statement():
        print(f"\n-----Building Full Inova Schema-----\n")

    @task(outlets=Dataset("inova.time_summary_data"))
    def import_time_summary_data():
        # Imports Time Summary Data
        df = pd.read_csv(io.StringIO(inova.endpoint("REPORT_TIME_ENTRY_SUMMARY", method='report', version=1)), skipinitialspace=True)
        df.columns = df.columns.str.strip()
        export_to_database(engine, "inova.time_summary_data", df, "Employee Id", "Date", date_column="Date")

    @task(outlets=Dataset("inova.time_entry_data"))
    def import_time_entry_data():
        # Imports Time Entry Data

        df = pd.json_normalize(inova.endpoint("time-entries", start_date=f"{bom}", end_date=f"{eom}")['time_entry_sets'], record_path=['time_entries'], meta=['start_date', 'end_date', ['employee','account_id']])
        export_to_database(engine, "inova.time_entry_data", df, "id")
        # -- # There is a random cost_center index-id column that doubles the rows so here is how you access it but we found it unnessessary to add it to the database right now.
        # -- # This is a known column now, you can access it through other methods but I thought I should keep this here for reference anyways.
        # -- # data2 = pd.json_normalize(get_inova_data_v2("https://secure.saashr.com/ta/rest/v2/companies/!ecoplumbers/time-entries", {"start_date":"2023-03-01"}, {"end_date":"2023-03-31"})['time_entry_sets'], record_path=['time_entries','cost_centers'], meta=[['time_entries','id']])

    @task(outlets=Dataset("inova.employees_data"))
    def import_employee_data():
        # Imports Employee Data

        df = pd.json_normalize(inova.endpoint("employees")['employees'])
        export_to_database(engine, "inova.employees_data", df, "id")

    @task(outlets=Dataset("inova.employees_roster"))
    def import_employee_roster_data():
        df = pd.read_csv(io.StringIO(inova.endpoint("REPORT_EMPLOYEE_ROSTER", method='report', version=1)), skipinitialspace=True)
        export_to_database(engine, "inova.employees_roster", df, "Employee Id")

    @task(outlets=Dataset("inova.termination_details"))
    def import_termination_data():
        # Imports Termination Data
        df = pd.read_csv(io.StringIO(inova.endpoint("REPORT_TERMINATION_DETAILS", method='report', version=1)))
        df.columns = df.columns.str.strip()
        export_to_database(engine, "inova.termination_details", df, "Employee Id")

    @task(outlets=Dataset("inova.config_cost_centers_department"))
    def import_cost_center_data():
        # imports Department Data

        df = pd.json_normalize(inova.endpoint("config/cost-centers",tree_index=1)['cost_centers'])
        export_to_database(engine, "inova.config_cost_centers_department", df, "id")

    @task(outlets=Dataset("inova.config_cost_centers_location"))
    def import_location_data():
        # imports Location Data

        df = pd.json_normalize(inova.endpoint("config/cost-centers",tree_index=0)['cost_centers'])
        export_to_database(engine, "inova.config_cost_centers_location", df, "id")

    @task(outlets=Dataset("inova.config_cost_centers_position"))
    def import_job_titles_cc_data():
        # imports Job Titles Data

        df = pd.json_normalize(inova.endpoint("config/cost-centers",tree_index=8)['cost_centers'])
        export_to_database(engine, "inova.config_cost_centers_position", df, "id")

    @task(outlets=Dataset("inova.job_lookups"))
    def import_job_titles_lookups_data():
        # import Job Titles Data (This one is what we are actually using, cost center position is secondary)

        df = pd.json_normalize(inova.endpoint("lookup/jobs")['items'])
        export_to_database(engine, "inova.job_lookups", df, "id")

    @task(outlets=Dataset("inova.employees_full_data"))
    def import_create_employees_full():
        # Uses inova.employees_data to build a full employees dataset
        create_employees_full(engine)

    build_statement() >> [
        import_time_summary_data(),
        import_employee_roster_data(),
        import_termination_data(),
        import_time_entry_data(),
        import_employee_data(),
        import_cost_center_data(),
        import_location_data(),
        import_job_titles_cc_data(),
        import_job_titles_lookups_data()
    ] >> import_create_employees_full()