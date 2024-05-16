import time
import pandas as pd
import datetime
import calendar
import socket

def add_skill_level_to_technician_api(df):
    print(df['customFields'])
    print(df.head())
    try:
        df['technician_level'] = (
            df['customFields'].apply(lambda x: pd.json_normalize(x).query('name == "Technician Level"')['value'].iloc[0])
        )
    except Exception as e:
        print(e)
        df['technician_level'] = None
        
    return df

def fix_dataset_jobs_columns(df):
    field_list = [
        "subtotal",
        "total",
        "payments",
        "po_by_bill_costs",
        "estimate_sales_subtotal",
        "estimate_sales_installed",
        "total_costs",
        "total_revenue",
        "gross_margin",
        "gross_margin_pct"
    ]

    # For each column in the data frame iterate through the list of fields and replace the column name
    for column in df.columns:
        for field in field_list:
            if field == column:
                print(f"Renaming {column} to jobs_{field}")
                df.rename(columns={column: "jobs_" + field}, inplace=True)

    return df

def fix_materials_primary_vendor_id(df):
    if 'primaryVendor.id' in df.columns:
        # convert the column to an integer
        print("Converting primaryVendor.id to an integer")
        df.loc[df['primaryVendor.id'].isnull(), 'primaryVendor.id'] = 0
        df['primaryVendor.id'] = df['primaryVendor.id'].astype(int)
    
    if 'primaryVendor.vendorId' in df.columns:
        # convert the column to an integer
        print("Converting primaryVendor.vendorId to an integer")
        df.loc[df['primaryVendor.vendorId'].isnull(), 'primaryVendor.vendorId'] = 0
        df['primaryVendor.vendorId'] = df['primaryVendor.vendorId'].astype(int)

    return df

def get_hostname():
    h_name = socket.gethostname()
    IP_addres = socket.gethostbyname(h_name)
    print("Host Name is:" + h_name)
    print("Computer IP Address is:" + IP_addres)
    return h_name

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

def table_exists(table_name, engine):
    schema_table_list = split_table_name(table_name)
    sql = f"""
        SELECT EXISTS (
        SELECT FROM 
            information_schema.tables 
        WHERE 
            table_schema LIKE '{schema_table_list[0]}' AND 
            table_type LIKE 'BASE TABLE' AND
            table_name = '{schema_table_list[1]}'
    )
    """
    return pd.read_sql(sql, engine)["exists"][0]


def get_table_column_datatypes(table_name, engine):
    schema_table_list = split_table_name(table_name)
    sql = f"""
        SELECT
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            table_schema LIKE '{schema_table_list[0]}'
            AND 
            table_name = '{schema_table_list[1]}'
    """
    return pd.read_sql(sql, engine)


def get_table_column_datatypes_in_stage(table_name, engine, options={"stage_suffix": "stage"}):
    schema_table_list = split_table_name(table_name)
    sql = f"""
        with 
        stage as (
            select column_name, data_type
            from information_schema.columns
            WHERE
                table_schema = '{schema_table_list[0]}' AND 
                table_name = '{schema_table_list[1]}_{options["stage_suffix"]}'
        ),
        perm as (
            select column_name, data_type
            from information_schema.columns
            WHERE
                table_schema = '{schema_table_list[0]}' AND 
                table_name = '{schema_table_list[1]}'
        )
        select perm.column_name, perm.data_type
        from perm
        inner join stage on perm.column_name = stage.column_name
    """
    return pd.read_sql(sql, engine)


def generate_select_columns_with_datatypes(table_name, engine, type="perm", options={"stage_suffix": "stage"}):
    if type == "perm":
        df_data_types = get_table_column_datatypes(table_name, engine)
    elif type == "stage":
        df_data_types = get_table_column_datatypes_in_stage(table_name, engine, options)

    column_string_with_data_types = ",".join(
        ("\"" + df_data_types["column_name"] + "\"" + "::" + df_data_types["data_type"]).to_list())
    column_string_without_data_types = ",".join(
        ("\"" + df_data_types["column_name"] + "\"").to_list())
    return column_string_with_data_types, column_string_without_data_types


def insert_with_data_types(table_name, engine, type="stage", options={"stage_suffix": "stage"}):
    select_string_with_dtypes, select_string_without_dtypes = generate_select_columns_with_datatypes(
        table_name, engine, type)
    engine.execute(f"""
        INSERT INTO {table_name} ({select_string_without_dtypes}) 
        SELECT {select_string_with_dtypes} FROM {table_name}_{options["stage_suffix"]}
    """)
    pass


def generate_update_query_for_all_columns(table_name, engine, type="perm"):
    if type == "perm":
        df_data_types = get_table_column_datatypes(table_name, engine)
    elif type == "stage":
        df_data_types = get_table_column_datatypes_in_stage(table_name, engine)

    update_string = ",".join(("\"" + df_data_types["column_name"] + "\"" + " = " +
                             "EXCLUDED." + "\"" + df_data_types["column_name"] + "\"").to_list())
    return update_string


def save_snapshot(table, engine, filter=[], is_daily=True, reset=False):

    today = datetime.date.today()  # Machine Local Time
    today_str = today.strftime("%Y-%m-%d")
    today_str_est = pd.to_datetime(
        "now", format="%Y-%m-%d", utc=True).tz_convert("US/Eastern").date()

    snapshot_table = f"{table}_snapshot"

    try:
        if reset:
            engine.execute(f"DROP TABLE IF EXISTS {snapshot_table}")
    except Exception as e:
        print(f"Error on {table}: {e}")

    # create snapshot table filter from filter list
    filer_string = "WHERE " + (" AND ".join(filter) if filter else '1=1')

    # Get base table
    base_table = pd.read_sql(f"SELECT * FROM {table} {filer_string}", engine)

    # Add snapshot date in utc
    base_table["_snapshot_timestamp"] = pd.to_datetime("now", utc=True)
    base_table["_snapshot_report_date"] = pd.to_datetime(
        "now", format="%Y-%m-%d", utc=True).tz_convert("US/Eastern").date()
    # add hostname column
    base_table["_snapshot_updated_by"] = get_hostname()

    # upload to staging table
    upload_dataframe(
        base_table, f"{snapshot_table}_stage", engine, if_exists="replace")
    engine.execute(
        f"CREATE TABLE IF NOT EXISTS {snapshot_table} as select * from {snapshot_table}_stage LIMIT 1")

    if is_daily:
        update_sql = f"""
            DELETE FROM {snapshot_table} as perm
            WHERE perm."_snapshot_report_date"::date = '{today_str_est}'
        """
        engine.execute(update_sql)

    insert_with_data_types(snapshot_table, engine)
    print(pd.read_sql(
        f"SELECT * from {snapshot_table} ORDER BY _snapshot_timestamp DESC LIMIT 5", engine))

def column_exists(table_name, column_name, engine):
    schema_table_list = split_table_name(table_name)
    sql = f"""
        SELECT EXISTS (
        SELECT FROM 
            information_schema.columns 
        WHERE 
            table_schema LIKE '{schema_table_list[0]}' AND 
            table_name LIKE '{schema_table_list[1]}' AND
            column_name LIKE '{column_name}'
    )
    """
    return pd.read_sql(sql, engine)["exists"][0]


def convert_data_types(value):
    if value == "boolean":
        return "boolean"
    elif value == "bigint":
        return "int"
    elif value == "double precision":
        return "float"
    else:
        return value


def sort_df_cols(df):
    return df.reindex(sorted(df.columns), axis=1)

def columns_clean_full_stops(df, replace_full_stop = '_'):
    new_columns = [col.strip().replace('.', '_') for col in df.columns]
    df.columns = new_columns
    return(df)

def clean_whitespace_characters_in_df(df):
    for column in df:
        try:
            if df[column].dtype == "object":
                try:
                    df[column] = df[column].replace('\x00', '', regex=True)
                except:
                    pass
                try:
                    df[column] = df[column].replace("\t", "", regex=True)
                except:
                    pass
                try:
                    df[column] = df[column].replace("\n", "", regex=True)
                except:
                    pass
                try:
                    df[column] = df[column].replace("\r", "", regex=True)
                except:
                    pass
                try:
                    df[column] = df[column].replace("\x0a", "", regex=True)
                except:
                    pass
        except Exception as e:
            print(f"Error on {column}: {e}")    
    return df


def sleep_with_countdown(sleep_time):
    """Sleeps for a given amount of time with a countdown"""
    for i in range(sleep_time, 0, -1):
        print("Sleeping for {} seconds".format(i), end='\r')
        time.sleep(1)
    print("")
    pass


def convert_columns_to_lowercase(df):
    df.columns = df.columns.str.lower()
    return df


def clean_df_column_names(df, to_lower=True):
    df.columns = (df.columns.str.strip().str.lower().str.replace(
        ' ', '_', regex=False).str.replace('(', '', regex=False,).str.replace(')', '', regex=False))
    if to_lower:
        df.columns = df.columns.str.lower()
    return df

# date utilities


def get_date_string(date, offset_days, format="%Y-%m-%d", tz="Local"):
    date_object = (date + datetime.timedelta(days=offset_days))
    # convert date_object to eastern time
    if tz != "Local":
        date_object = pd.to_datetime(
            date_object, utc=True).tz_convert(tz).date()

    return date_object.strftime(format)
    # return (date + datetime.timedelta(days=offset_days)).strftime(format)


def format_dates_mmddyyyy(date):
    return date.strftime("%m/%d/%Y")


def get_start_of_month(date):
    return date.replace(day=1)


def get_start_of_week(date):
    return date - datetime.timedelta(days=date.weekday())


def get_start_of_quarter(date):
    return date.replace(month=((date.month - 1) // 3) * 3 + 1, day=1)


def get_start_of_year(date):
    return date.replace(month=1, day=1)


def get_end_of_month(date):
    return date.replace(day=calendar.monthrange(date.year, date.month)[1])

def is_string_date(date_string):
    try:
        datetime.datetime.strptime(date_string[0:10], "%Y-%m-%d")
        return True
    except ValueError:
        return False

def parse_datetime_columns_from_df(df):
    for col in df:
        if df[col].dtype == 'object':
            is_date = is_string_date(str(df[col][0]))
            if not is_date:
                # try a sample of 10 rows
                is_date = any([is_string_date(str(row)) for row in df[col][0:10]])
            
            if is_date:
                df[col] = pd.to_datetime(df[col].str.slice(stop=19)+'Z', format='%Y-%m-%dT%H:%M:%SZ')
                
    return df

def is_valid_datetime(datetime_string):
    """
    Check if the given string is a valid date and time in any of the common formats.

    Parameters:
    datetime_string (str): The string to be checked.

    Returns:
    bool: True if the string is a valid date and time in any common format, False otherwise.
    """
    common_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO 8601 datetime: 2023-11-24T14:30:59.123Z
        "%Y-%m-%dT%H:%M:%SZ",     # ISO 8601 datetime: 2023-11-24T14:30:59Z
        "%Y-%m-%dT%H:%M:%S",      # ISO 8601 datetime: 2023-11-24T14:30:59
        "%Y-%m-%d",               # ISO 8601 date: 2023-11-24
        "%Y-%m-%d %H:%M:%S",      # 2023-11-24 14:30:59 (24-hour format)
        "%Y-%m-%d %I:%M:%S %p",   # 2023-11-24 02:30:59 PM (12-hour format)
        "%d/%m/%Y %H:%M",         # 24/11/2023 14:30
        "%m/%d/%Y %I:%M %p",      # 11/24/2023 02:30 PM
        "%d-%b-%Y %H:%M:%S",      # 24-Nov-2023 14:30:59
        "%Y/%m/%d %H:%M",         # 2023/11/24 14:30
        "%d %b %Y %I:%M:%S %p",   # 24 Nov 2023 02:30:59 PM
        "%b %d, %Y %H:%M:%S",     # Nov 24, 2023 14:30:59
        "%d-%m-%Y %I:%M %p",      # 24-11-2023 02:30 PM
        "%m-%d-%Y %H:%M",         # 11-24-2023 14:30
        "%Y.%m.%d %H:%M:%S",      # 2023.11.24 14:30:59
        "%d.%m.%Y %I:%M:%S %p",   # 24.11.2023 02:30:59 PM
        # Add more formats as needed
    ]

    for datetime_format in common_formats:
        try:
            datetime.datetime.strptime(datetime_string, datetime_format)
            return True
        except ValueError:
            continue

    return False

def progressbar(progress, total):
    bar_len = 20
    filled_len = int(progress / total * bar_len)
    remaining_len = bar_len - filled_len
    if progress != total:
        string = f'[{"".join("🟦" * filled_len)}{"".join("  " * remaining_len)}]'
    else:
        string = '✅✅✅✅✅✅✅✅✅                                                              '
    return string

def divide_date_range(start_date, end_date, num_divisions=10):
    """Divides a date range into a specified number of equal sub-ranges.

    Args:
        start_date (datetime.date): The start date of the range.
        end_date (datetime.date): The end date of the range.
        num_divisions (int): The number of desired divisions.

    Returns:
        list: A list of tuples, where each tuple represents a sub-range 
              in the format (start_date, end_date).
    """

    # Calculate the duration of each sub-range
    total_duration = end_date - start_date
    subrange_duration = total_duration / num_divisions

    # Create a list to store the sub-ranges
    sub_ranges = []

    # Generate the start and end dates for each sub-range
    current_start = start_date
    for _ in range(num_divisions):
        current_end = current_start + subrange_duration
        # Handle the last sub-range to ensure it reaches the end_date
        if current_end > end_date:
            current_end = end_date
        sub_ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
        current_start = current_end + datetime.timedelta(days=1)  # Start of the next sub-range

    return sub_ranges

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
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # cur.copy_from(output, name, null="", sep="\t") # null values become ''
        cur.copy_expert(f"COPY {name} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')", output)
        conn.commit()