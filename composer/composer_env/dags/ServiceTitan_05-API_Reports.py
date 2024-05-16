from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset

with DAG(
    dag_id="ServiceTitan_05_API_Reports_15minute",
    schedule=timedelta(minutes=15),
    start_date=datetime(2024, 5, 15),
    catchup=False,
    tags=["ServiceTitan","API Import","Schema: servicetitan"],
    max_active_runs=1,
) as dag:

    from python_libraries.ecoplumbersdb import create_engine
    from python_libraries.servicetitan.reports import get_all_yml_configs_from_folder, run_all_reports
    from python_libraries.secrets import get_json_secret

    engine = create_engine('ecoplumbersdb')
    config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",file_search="*.yml")
    api_config = {
        "default": get_json_secret('servicetitan-hq-creds'),
        "119741958": get_json_secret('servicetitan-hq-creds'),
        "1019530021": get_json_secret('servicetitan-adv-creds'),
        }
    
    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_jobs"))
    def import_rpt_dataset_jobs_data(engine,config,api_config):
        config = {
            "dataset_jobs_scheduled_date": config['dataset_jobs_scheduled_date'],
            "dataset_jobs_created_date": config['dataset_jobs_created_date'],
            "advanced_dataset_jobs_scheduled_date": config['advanced_dataset_jobs_scheduled_date'],
            "advanced_dataset_jobs_created_date": config['advanced_dataset_jobs_created_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_estimates"))
    def import_rpt_dataset_estimates_data(engine,config,api_config):
        config = {
            "dataset_estimates_sold_on_date": config['dataset_estimates_sold_on_date'],
            "advanced_dataset_estimates_sold_on_date": config['advanced_dataset_estimates_sold_on_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_calls"))
    def import_rpt_dataset_calls_data(engine,config,api_config):
        config = {
            "dataset_calls_call_date": config['dataset_calls_call_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_projects"))
    def import_rpt_dataset_projects_data(engine,config,api_config):
        config = {
            "dataset_projects_job_dates": config['dataset_projects_job_dates'],
            "dataset_projects_job_dates_chi": config['dataset_projects_job_dates_chi'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_office_audit_trail"))
    def import_rpt_dataset_office_audit_trail_data(engine,config,api_config):
        config = {
            "dataset_office_audit_trail": config['dataset_office_audit_trail'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_technician_performance"))
    def import_rpt_technician_performance_data(engine,config,api_config):
        config = {
            "technician_performance_TDY": config['technician_performance_TDY'],
            "technician_performance_TDY_CHI": config['technician_performance_TDY_CHI'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task()
    def cleanup_extra_staging_tables(engine):
        from python_libraries.servicetitan.reports import clean_up_uuid_staging_tables
        # Added to avoid errored staging tables from building up in the DB
        clean_up_uuid_staging_tables(engine, 'servicetitan')

    [import_rpt_dataset_jobs_data(engine,config,api_config),
     import_rpt_dataset_estimates_data(engine,config,api_config),
     import_rpt_dataset_calls_data(engine,config,api_config),
     import_rpt_dataset_projects_data(engine,config,api_config),
     import_rpt_dataset_office_audit_trail_data(engine,config,api_config),
     import_rpt_technician_performance_data(engine,config,api_config),
    ] >> cleanup_extra_staging_tables(engine)


with DAG(
    dag_id="ServiceTitan_05_API_Reports_Hourly",
    schedule_interval="@hourly",
    start_date=datetime(2024, 5, 15),
    catchup=False,
    tags=["ServiceTitan","API Import","Schema: servicetitan"],
    max_active_runs=1,
) as dag:

    from python_libraries.ecoplumbersdb import create_engine
    from python_libraries.servicetitan.reports import get_all_yml_configs_from_folder, run_all_reports
    from python_libraries.secrets import get_json_secret

    engine = create_engine('ecoplumbersdb')
    config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",file_search="*.yml")
    api_config = {
        "default": get_json_secret('servicetitan-hq-creds'),
        "119741958": get_json_secret('servicetitan-hq-creds'),
        "1019530021": get_json_secret('servicetitan-adv-creds'),
        }
    
    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_jobs"))
    def import_rpt_dataset_jobs_data(engine,config,api_config):
        config = {
            "dataset_jobs_completion_date": config['dataset_jobs_completion_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task()
    def cleanup_extra_staging_tables(engine):
        from python_libraries.servicetitan.reports import clean_up_uuid_staging_tables
        # Added to avoid errored staging tables from building up in the DB
        clean_up_uuid_staging_tables(engine, 'servicetitan')

    import_rpt_dataset_jobs_data(engine,config,api_config) >> cleanup_extra_staging_tables(engine)

with DAG(
    dag_id="ServiceTitan_05_API_Reports_Daily",
    schedule_interval="@daily",
    start_date=datetime(2024, 5, 15),
    catchup=False,
    tags=["ServiceTitan","API Import","Schema: servicetitan"],
    max_active_runs=1,
) as dag:

    from python_libraries.ecoplumbersdb import create_engine
    from python_libraries.servicetitan.reports import get_all_yml_configs_from_folder, run_all_reports
    from python_libraries.secrets import get_json_secret

    engine = create_engine('ecoplumbersdb')
    config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",file_search="*.yml")
    api_config = {
        "default": get_json_secret('servicetitan-hq-creds'),
        "119741958": get_json_secret('servicetitan-hq-creds'),
        "1019530021": get_json_secret('servicetitan-adv-creds'),
        }
    
    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_timesheets"))
    def import_rpt_dataset_timesheets_data(engine,config,api_config):
        config = {
            "dataset_timesheets": config['dataset_timesheets'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_jobs"))
    def import_rpt_dataset_jobs_data(engine,config,api_config):
        config = {
            "dataset_jobs_has_appointment_date": config['dataset_jobs_has_appointment_date'],
            "advanced_dataset_jobs_has_appointment_date": config['advanced_dataset_jobs_has_appointment_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_smart_dispatch"))
    def import_rpt_smart_dispatch_data(engine,config,api_config):
        config = {
            "smart_dispatch": config['smart_dispatch'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_technician_performance"))
    def import_rpt_technician_performance_data(engine,config,api_config):
        config = {
            "technician_performance_WTD": config['technician_performance_WTD'],
            "technician_performance_MTD": config['technician_performance_MTD'],
            "technician_performance_MTD": config['technician_performance_WTD_CHI'],
            "technician_performance_MTD": config['technician_performance_MTD_CHI'],
        }
        run_all_reports(config, engine, api_config=api_config)
        config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",report_date=datetime.today(),file_search="*.yml")
        config = {
            "technician_performance_WTD": config['technician_performance_MTH'],
            "technician_performance_MTD": config['technician_performance_LWK'],
            "technician_performance_MTD": config['technician_performance_MTH_CHI'],
            "technician_performance_MTD": config['technician_performance_LWK_CHI'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_invoice_items"))
    def import_rpt_dataset_invoice_items_data(engine,config,api_config):
        config = {
            "dataset_invoice_items_scheduled_date": config['dataset_invoice_items_scheduled_date'],
            "dataset_invoice_items_completion_date": config['dataset_invoice_items_completion_date'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task()
    def cleanup_extra_staging_tables(engine):
        from python_libraries.servicetitan.reports import clean_up_uuid_staging_tables
        # Added to avoid errored staging tables from building up in the DB
        clean_up_uuid_staging_tables(engine, 'servicetitan')

    [import_rpt_dataset_timesheets_data(engine,config,api_config),
     import_rpt_dataset_jobs_data(engine,config,api_config),
     import_rpt_smart_dispatch_data(engine,config,api_config),
     import_rpt_technician_performance_data(engine,config,api_config),
     import_rpt_dataset_invoice_items_data(engine,config,api_config),
     ] >> cleanup_extra_staging_tables(engine)

with DAG(
    dag_id="ServiceTitan_05_API_Reports_Weekly",
    schedule_interval="@weekly",
    start_date=datetime(2024, 5, 15),
    catchup=False,
    tags=["ServiceTitan","API Import","Schema: servicetitan"],
    max_active_runs=1,
) as dag:

    from python_libraries.ecoplumbersdb import create_engine
    from python_libraries.servicetitan.reports import get_all_yml_configs_from_folder, run_all_reports
    from python_libraries.secrets import get_json_secret

    engine = create_engine('ecoplumbersdb')
    config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",file_search="*.yml")
    api_config = {
        "default": get_json_secret('servicetitan-hq-creds'),
        "119741958": get_json_secret('servicetitan-hq-creds'),
        "1019530021": get_json_secret('servicetitan-adv-creds'),
        }
    
    @task(outlets=Dataset("servicetitan.api_v2_rpt_technician_performance"))
    def import_rpt_technician_performance_data(engine,config,api_config):
        config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",report_date=datetime.today(),file_search="*.yml")
        config = {
            "technician_performance_LWK": config['technician_performance_LWK'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_invoice_items"))
    def import_rpt_dataset_invoice_items_data_backfill(engine,config,api_config):
        config = {
            "dataset_invoice_items_scheduled_date_backfill": config['dataset_invoice_items_scheduled_date_backfill'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_jobs"))
    def import_rpt_dataset_jobs_data_backfill(engine,config,api_config):
        config = {
            "dataset_jobs_scheduled_date_backfill": config['dataset_jobs_scheduled_date_backfill'],
            "dataset_jobs_created_date_backfill": config['dataset_jobs_created_date_backfill'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_timesheets"))
    def import_rpt_dataset_timesheets_data_backfill(engine,config,api_config):
        config = {
            "dataset_timesheets_backfill": config['dataset_timesheets_backfill'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task(outlets=Dataset("servicetitan.api_v2_rpt_dataset_estimates"))
    def import_rpt_dataset_estimates_data_backfill(engine,config,api_config):
        config = {
            "dataset_estimates_sold_on_date_backfill": config['dataset_estimates_sold_on_date_backfill'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task()
    def cleanup_extra_staging_tables(engine):
        from python_libraries.servicetitan.reports import clean_up_uuid_staging_tables
        # Added to avoid errored staging tables from building up in the DB
        clean_up_uuid_staging_tables(engine, 'servicetitan')

    [import_rpt_technician_performance_data(engine,config,api_config),
     import_rpt_dataset_invoice_items_data_backfill(engine,config,api_config),
     import_rpt_dataset_jobs_data_backfill(engine,config,api_config),
     import_rpt_dataset_timesheets_data_backfill(engine,config,api_config),
     import_rpt_dataset_estimates_data_backfill(engine,config,api_config),
     ] >> cleanup_extra_staging_tables(engine)

with DAG(
    dag_id="ServiceTitan_05_API_Reports_Monthly",
    schedule_interval="@monthly",
    start_date=datetime(2024, 5, 19),
    catchup=False,
    tags=["ServiceTitan","API Import","Schema: servicetitan"],
    max_active_runs=1,
) as dag:

    from python_libraries.ecoplumbersdb import create_engine
    from python_libraries.servicetitan.reports import get_all_yml_configs_from_folder, run_all_reports
    from python_libraries.secrets import get_json_secret

    engine = create_engine('ecoplumbersdb')
    config = get_all_yml_configs_from_folder(folder_path="/home/airflow/gcs/dags//python_libraries/servicetitan/servicetitan_api_reports",file_search="*.yml")
    api_config = {
        "default": get_json_secret('servicetitan-hq-creds'),
        "119741958": get_json_secret('servicetitan-hq-creds'),
        "1019530021": get_json_secret('servicetitan-adv-creds'),
        }
    
    @task(outlets=Dataset("servicetitan.api_v2_rpt_technician_performance"))
    def import_rpt_technician_performance_data(engine,config,api_config):
        config = {
            "technician_performance_YTD": config['technician_performance_YTD'],
        }
        run_all_reports(config, engine, api_config=api_config)

    @task()
    def cleanup_extra_staging_tables(engine):
        from python_libraries.servicetitan.reports import clean_up_uuid_staging_tables
        # Added to avoid errored staging tables from building up in the DB
        clean_up_uuid_staging_tables(engine, 'servicetitan')

    import_rpt_technician_performance_data(engine,config,api_config) >> cleanup_extra_staging_tables(engine)