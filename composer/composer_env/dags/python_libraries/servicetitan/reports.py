import pandas as pd
from pathlib import Path
import datetime, uuid, yaml

from dateutil.relativedelta import relativedelta
from servicepytan import Report, auth as st_auth
from python_libraries.utilities import generate_select_columns_with_datatypes, \
    get_date_string, get_start_of_month, get_start_of_quarter, get_start_of_week, get_start_of_year, \
    clean_whitespace_characters_in_df, column_exists, divide_date_range, fix_dataset_jobs_columns
from python_libraries.ecoplumbersdb import create_engine as get_engine

def split_table_name(table_and_schema):
    """
    Split a 'schema.table_name' into parts to use with pd.to_sql
    """
    table_list = table_and_schema.split(".")
    if len(table_list) == 1:
        schema = None
        table = table_list[0]
    else:
        schema = table_list[0]
        table = table_list[1]
    return([schema, table])

def upload_dataframe(df, name, engine, if_exists='replace', index=False):
    """
    Uploads a dataframe to a PostgreSQL database
    """

    # Import Libraries
    import io

    # Table name needs to be split into schema
    table_list = split_table_name(name)
    schema = table_list[0]
    table  = table_list[1]

    # Truncates the table
    if df.empty:
        print("Dataframe is empty, skipping upload of {name}")
    else: # Only if the dataframe is not empty
        df.head(0).to_sql(table, engine, schema=schema, if_exists=if_exists, index=index)

        # Because sometimes people put newlines in text cells and postgre throws a FIT :()
        df.replace("\\n", value="", regex = True, inplace = True)
        # Upload Data
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        print(df.dtypes)
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # cur.copy_from(output, name, null="", sep="\t") # null values become ''
        cur.copy_expert(f"COPY {name} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')", output)
        conn.commit()

def extract_time_from_rate_limit_message(data):
    """Extracts the time from the rate limit message"""
    time = data['title'].split(" ")[-2]
    return time

def process_report_data(data, table_name=None):
    """Processes the data returned from the report"""
    
    columns = [field['label'] for field in data['fields']]
    columns = [c.lower().replace(" ","_") for c in columns]
    columns = [c.lower().replace("-","_") for c in columns]
    columns = [c.lower().replace("/","_by_") for c in columns]
    columns = [c.lower().replace("__","_") for c in columns]
    columns = [c.lower().replace("%","pct") for c in columns]
    df = pd.DataFrame(data['data'], columns=columns)
    df = clean_whitespace_characters_in_df(df)
    print(f"{len(df)} rows returned")
    
    #!!!TODO: This is a hacky patch to fix a specific issue
    if table_name:
        if table_name == "servicetitan.api_v2_rpt_dataset_jobs":
            df = fix_dataset_jobs_columns(df)
        
    return df

# TODO: add to servicepytan
def try_rate_limited_request(report,params):
    """Attempts to make a request to the API"""
    try:
        data = report.get_data(params)
        print(data)
        if 'status' in data:
            from datetime import time
            sleep_time = extract_time_from_rate_limit_message(data)
            time.sleep(int(sleep_time))
            data = try_rate_limited_request(report,params)
    except Exception as e:
        error = e
        return error
    return data

def read_yaml(file_name):
    with open(file_name, 'r') as f:
        data = yaml.safe_load(f)
    return data

def parse_report_config_yaml(report_file, report_date=None):
  # set the reporting date
  if report_date is None:
    today = datetime.datetime.today()
    date = today - datetime.timedelta(days=1)
  else:
    date = report_date
  # Parse the yaml file
  reports = read_yaml(report_file)
  if reports: # protect against empty yaml files
    for report in reports:
        for report_name, report_config in report.items():
            # evalueate params
            if 'params' in report_config:
                params = report_config['params']
                new_params = []
                if params:
                    for param in params:
                            for param_name, param_value in param.items():
                                new_params.append({param_name: eval(param_value)})
                report_config['params'] = new_params
                
            # evaluate add_cols
            if 'add_cols' in report_config:
                add_cols = report_config['add_cols']
                new_add_cols = []
                if add_cols:
                    for add_col in add_cols:
                        for add_col_name, add_col_value in add_col.items():
                            new_add_cols.append({add_col_name: str(eval(add_col_value))})
                report_config['add_cols'] = new_add_cols

  return reports


def get_all_yml_configs_from_folder(folder_path="./reports", report_date=None, file_search="*.yml"):
  report_configs = {}
  for p in Path(folder_path).rglob(f'{file_search}'):
    for config in parse_report_config_yaml(p, report_date):
      for report_name, report_config in config.items():
        report_config['file_name'] = str(p)
        report_configs[report_name] = report_config

  return report_configs

def get_servicetitan_report_data(report_config, config_json, page_size=25000, engine=get_engine('ecoplumbersdb')):
    st_conn = config_json
    
    report_category = report_config["report_category"]
    report_id = report_config["report_id"]
    params = report_config["params"]
    allow_date_range_division = report_config.get("allow_date_range_division", "Yes")    

    print(params)

    report = Report(report_category, report_id, conn=st_conn)
    start_date = datetime.datetime.strptime(params[0]["From"], "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(params[1]["To"], "%Y-%m-%d").date()
    difference = datetime.datetime.today().date() - start_date
    data = {}

    """
    If the date range is greater than 90 days, divide the date range into 10 equal parts.
    This works for datasets where the date range changes the number of rows returned. However, datasets
    like "Technician Performance" will return the same number of rows regardless of the date range.
    """
    if difference.days > 90 and allow_date_range_division == "Yes":
        for i in divide_date_range(start_date, end_date, num_divisions=10):
            params[0]["From"] = i[0]
            params[1]["To"] = i[1]
            # add the parameters to the report
            print(params)
            for param in params:
                for key, value in param.items():
                    report.add_params(key, value)
        
            """
            Since we're updating on each data pull, we dont need to extend the data set on each loop.
            Making this change to prevent the issue where we pull a large dataset and it fails the insert
            due to duplicate ids. This way we can be fastter between data pull and data insert.
            -- Elliot 2024-05-12 --
            """
            # get the data
            # if data:
            #     data["data"].extend(report.get_all_data(page_size=page_size, timeout_min=10000)["data"])
            # else:
            #     data = report.get_all_data(page_size=page_size, timeout_min=10000)
            
            data = report.get_all_data(page_size=page_size, timeout_min=10000)
            print(f"Total Row Count: {len(data['data'])}")

            # Check to see if data is returned, if not, print the error and continue
            if data.get('data') is None:
                print(f"Error: {data}")
                continue

            # if data is pulled update the database with the data
            print(f"\t - Updating Database...")
            update_database_with_report_data(
                data, report_config['table_name'], report_config["id_cols"], report_config["add_cols"], engine, reset=report_config["reset"], config_json=config_json
            )
    else:
            for param in params:
                    for key, value in param.items():
                        report.add_params(key, value)
            
            """
            If we're not dividing the date range, we can just pull the data and update the database.
            -- Elliot 2024-05-12 --
            """
            
            # get the data
            # if data:
            #     data["data"].extend(report.get_all_data(page_size=page_size, timeout_min=10000)["data"])
            # else:
            #     data = report.get_all_data(page_size=page_size, timeout_min=10000)
            
            data = report.get_all_data(page_size=page_size, timeout_min=10000)
            print(f"Total Row Count: {len(data['data'])}")

            print(f"\t - Updating Database...")
            update_database_with_report_data(
                data, report_config['table_name'], report_config["id_cols"], report_config["add_cols"], engine, reset=report_config["reset"], config_json=config_json
            )
    
    return data

def update_database_with_report_data(data, table_name, id_cols, add_cols, engine, reset=False, config_json=None):
    
    from sqlalchemy import text
    st_conn = config_json
    TENANT_ID = st_conn.get('SERVICETITAN_TENANT_ID')    

    df = process_report_data(data, table_name)
    
    if df.empty:
        return df

    # add updated datetime to dataframe
    for col in add_cols:
        for key, value in col.items():
            df[key] = value

    df["_etl_update_datetime_utc"] = datetime.datetime.utcnow()
    df["tenant_id"] = TENANT_ID

    stage_name_short = f"{table_name}_{TENANT_ID}"
    UUID = uuid.uuid4()
    stage_suffix = f"stage_{TENANT_ID}_{str(UUID).replace('-', '')}"
    stage_name = f"{table_name}_{stage_suffix}"
    # truncate to 63 characters, max table name length
    stage_name = stage_name[:63]
    stage_suffix = stage_name.split(f"{table_name}_",1)[1]


    try: # This assumes that table_name exists (which it always will)

        print(f"\t > Uploading Data to {stage_name}")

        # If table_stage doesn't exist, create it with the same schema as base table
        engine.execute(f"CREATE TABLE IF NOT EXISTS {stage_name} AS SELECT * FROM {table_name} LIMIT 0")
        # Delete all records from table_stage to ensure no duplicates
        engine.execute(f"DELETE FROM {stage_name}")
        # Upload data to table_stage
        #TODO Not sure if this create table does anything. Because of column standardization this step is irrelevant. (generate_select_columns_with_datatypes)
        upload_dataframe(df, f"{stage_name}", engine, if_exists="append")  

    except Exception as e: #But if table_name doesn't exist and errors the upload will try to create the stage
        # This protects against tables not existing from other running scripts
        print(e)
        print("Error creating table. Check data types.")
        upload_dataframe(df, f"{stage_name}", engine, if_exists="replace")      
    # upload to staging table

    # Generally just used during development
    if reset:
        engine.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Update the database
    engine.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {stage_name}")
    
    # if not column_exists(table_name, "tenant_id", engine):
    #     engine.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS tenant_id TEXT")
    
    def delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True, id_col_override = False):
        """Loops through delete statements for a given table based on staged data.
        
        Takes data from a staging table, uses that data to form an inputed
        where statement, then loops through the final table 5000 at a time and
        deletes based on the staging table constrictions. Where clauses must be
        formatted correctly, see delete_sql for syntax options.
        """
        if id_col_override == False:
            print(f"-- Deleting records from {table_name} --")
            total_deleted = 0
            where_clause = where_clause
            if include_tenant_id:
                where_clause += f" AND (perm.tenant_id = '{TENANT_ID}' OR perm.tenant_id IS NULL)"
            delete_sql = f"""
                with stage as (
                select * from {stage_name}
                ),
                perm as (
                select * from {table_name} perm
                WHERE 1=1
                AND ({where_clause})
                order by 1
                limit 5000
                )
                DELETE FROM {table_name} real
                USING perm
                WHERE 1=1
            """
            if id_cols.get('id_col') is not None:
                delete_sql += f" AND real.{id_cols.get('id_col')} = perm.{id_cols.get('id_col')} AND (perm.tenant_id = '{TENANT_ID}' OR perm.tenant_id IS NULL)"
            else:
                delete_sql += f" AND (real.{where_clause[5:]}) AND (perm.tenant_id = '{TENANT_ID}' OR perm.tenant_id IS NULL)"
            while True:
                with engine.begin() as conn:
                    deleted_records = conn.execute(text(delete_sql)).rowcount
                    total_deleted += deleted_records
                print(f"Deleted Rows: {deleted_records} Total deleted rows: {total_deleted}")
                if deleted_records == 0:
                    break
            print(f"Final total deleted rows: {total_deleted}")
        else:
            print(f"-- Deleting records from {table_name} --")
            delete_sql = f"""
                DELETE FROM {table_name} as perm
                USING {stage_name} as stage
                WHERE 1=1
                AND ({where_clause})
            """

            if include_tenant_id:
                delete_sql += f"AND (perm.tenant_id = stage.tenant_id OR perm.tenant_id IS NULL)"

            total_deleted = engine.execute(delete_sql).rowcount
            print(f"Final total deleted rows: {total_deleted}")


    print(id_cols)
    # Delete where clause
    # Where only a dates_between_col is provided, delete all records between the min and max dates in the perm table
    if type(id_cols) != list and id_cols.get('dates_between_col') and not id_cols.get('id_col'):
        print("-- Running dates_between_col delete --")
        dates_between_col = id_cols.get('dates_between_col')
        start_date = f"SELECT MIN({dates_between_col})::timestamp FROM {stage_name}"
        end_date = f"SELECT MAX({dates_between_col})::timestamp FROM {stage_name}"
        start_date = engine.execute(start_date).first()[0]
        end_date = engine.execute(end_date).first()[0]
        
        where_clause = f"perm.{dates_between_col} BETWEEN '{start_date}' AND '{end_date}'"

        try:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True)
        except:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True)
            
    # Where an id_col and dates_between_col are provided, 
    # delete all records between the min and max dates in the perm table and any id_col matches
    # EXPLANATION: This is used to prevent duplicate records from being added to the perm table
    elif type(id_cols) != list and id_cols.get('dates_between_col') and id_cols.get('id_col'):
        print("-- Running id_col and dates_between_col delete --")
        dates_between_col = id_cols.get('dates_between_col')
        start_date = f"SELECT MIN({dates_between_col})::timestamp FROM {stage_name}"
        end_date = f"SELECT MAX({dates_between_col})::timestamp FROM {stage_name}"
        start_date = engine.execute(start_date).first()[0]
        end_date = engine.execute(end_date).first()[0]
        
        print(">> Deleting by Date Range --")
        where_clause = f"perm.{dates_between_col} BETWEEN '{start_date}' AND '{end_date}'"
        try:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True)
        except:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True)

        print(">> Deleting by id_col --")
        where_clause = f" perm.{id_cols.get('id_col')} in ( select {id_cols.get('id_col')} from stage)"
        try:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=False)  
        except:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=False)       
     
    # Where an id_col is provided, delete all records where the id_col matches 
    else:
        print("-- Running id_col only delete --")
        
        where_clause = " AND\n ".join([f"perm.{col} in (select stage.{col} from {stage_name} stage)" for col in id_cols])
        try:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True, id_col_override = True)
        except:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True, id_col_override = True)


    print(f"-- Inserting records into {table_name} --")
    select_string_with_dtypes, select_string_without_dtypes = generate_select_columns_with_datatypes(table_name, engine, type="stage", options={"stage_suffix": stage_suffix})
    try:
        with engine.begin() as conn:
            rows_inserted = conn.execute(text(f"""
                INSERT INTO {table_name} ({select_string_without_dtypes}) 
                SELECT {select_string_with_dtypes} FROM {stage_name}
            """)).rowcount
            print(f"Total rows inserted into {table_name}: {rows_inserted}")
    except:
        try:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=False)     
        except:
            delete_reports_records(table_name, stage_name, where_clause, include_tenant_id=True, id_col_override = True)

        with engine.begin() as conn:
            print("Trying to upload again.")
            rows_inserted = conn.execute(text(f"""
                INSERT INTO {table_name} ({select_string_without_dtypes}) 
                SELECT {select_string_with_dtypes} FROM {stage_name}
            """)).rowcount
            print(f"Total rows inserted into {table_name}: {rows_inserted}")

    # Drop the stage table
    try:
        print(f"** Dropping {stage_name} and saving to {stage_name_short}**")
        engine.execute(
            f"""
            DROP TABLE IF EXISTS {stage_name_short};
            CREATE TABLE {stage_name_short} as SELECT * FROM {stage_name};
            DROP TABLE IF EXISTS {stage_name};
            """
        )
    except Exception as e:
        print(f"Error dropping table {stage_name}: {e}")

def run_all_reports(report_configs, engine, api_config, debug=False, page_size=25000):
    for report_name, report_config in report_configs.items():
        
        # Update to allow for multiple tenant configs
        try:
            if 'tenant_id' in report_config:
                print(f"** Using config for {report_config['tenant_id']} **")
                config_file = api_config[report_config["tenant_id"]]
            else:
                print(f"** Using Default Config **")
                config_file = api_config["default"]
        except Exception as e:
            print(f"** Error: Using Default config **")
            config_file = api_config["default"]

        def update_report_data(report_name, report_config, engine, config_file):
            print(f"Running report: {report_name}")
            print(f"\t - Getting Data...")
            data = get_servicetitan_report_data(report_config, config_file, page_size, engine=engine)
            if debug:
                print(data)
            # print(f"\t - Updating Database...")
            # update_database_with_report_data(
            #     data, report_config['table_name'], report_config["id_cols"], report_config["add_cols"], engine, reset=report_config["reset"], config_file=config_file
            # )
            print(f"\t - Done!")
        
        update_report_data(report_name, report_config, engine, config_file)

    print("*** All Reports Complete! ***")

def clean_up_uuid_staging_tables(engine, schema='servicetitan', ):
    """Cleans up staging tables with UUIDs in the name"""

    #Get all table names with UUIDs
    sql = f"""
        SELECT tablename
        FROM pg_catalog.pg_tables 
        WHERE schemaname = '{schema}'
        AND tablename ~* 'stage_'
        AND NOT tablename ~* 'stage_119741958|stage_1019530021'
        ORDER BY 1
    """

    tables = pd.read_sql(sql, engine)

    for table in tables["tablename"]:
        print(f"Deleting table {table}")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    print("All UUID staging tables deleted")

    return True