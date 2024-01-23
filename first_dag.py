import json
import pendulum
import pandas as pd
import json
import snowflake.connector
import os
from datetime import datetime
import numpy as np
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)

def tutorial_taskflow_api():
    def conn_snowflaks():
        try:
            logger.info("Connecting Snowflake...")
            conn = snowflake.connector.connect(
                user="JIGNESHP",
                password="Jignesh@123",
                account="qgb89466.us-east-1",
                role="ACCOUNTADMIN",
                warehouse="GLUE_TEST",
                database="TEST_DB",
                login_timeout=5,
            )
            logger.info("Snowflake connected successfully")
            return conn
        except Exception as e:
            # exc_type, exc_value, exc_traceback = sys.exc_info()
            # traceback.logger.info_exception(exc_type, exc_value, exc_traceback)
            # logger.info(f"Error [get_snowflake_connection]: {str(e)}")
            raise e


    def get_full_file_path_of_file(file_name):
        try:
            # Set the search_directory to None or an empty string to search in the current directory
            search_directory = "E://"
            # search_directory = os.getcwd()
            logger.info("search_directory :", search_directory)

            # Find the file by searching
            full_path = search_file(file_name, search_directory)

            if full_path is not None:
                logger.info(f"The full path of '{file_name}' is: {full_path}")
                return full_path
            else:
                logger.info(f"'{file_name}' not found in the current directory.")
                return file_name
        except Exception as e:
            raise e

    # Search for files from the given directory
    def search_file(filename, search_dir):
        try:
            for root, dirs, files in os.walk(search_dir):
                if filename in files:
                    return os.path.join(root, filename)
            return None
        except Exception as e:
            raise e


    # Delete the temporary local file
    def delete_uploaded_files(file_name):
        if os.path.exists(file_name):
            os.remove(file_name)
            return f"File '{file_name}' removed after upload."
        else:
            return f"File '{file_name}' does not exist or was not uploaded successfully."


    def delete_files_and_stages(database, schema, stage_name, stage_file_name, file_format_name,
                                file_name):
        try:
            # snowflake_database_manager = SnowflakeDatabaseManager()
            conn = conn_snowflaks()
            cursor = conn.cursor()
            cursor.execute(f'USE {database}.{schema}')

            logger.info("Deleting File from Stage..")
            cursor.execute(f"REMOVE @{stage_name}/{stage_file_name}.gz")
            logger.info("File Deleted")
            logger.info("Deleting Stage...")
            cursor.execute(f"DROP STAGE {stage_name};")
            logger.info("Stage Deleted from snowflake.")
            logger.info("Deleting File Format...")
            cursor.execute(f"DROP FILE FORMAT IF EXISTS {file_format_name};")
            logger.info("File Format Deleted from Snowflake stage.")
            logger.info("Deleting File from glue storage.")
            # logger.info(delete_uploaded_files(file_name))
        except Exception as e:
            raise e


    @task(multiple_outputs=True)
    def transform():
        table_name = context.op_config["table_name"]
        conn, cursor = conn_postgress()
        query = f"SELECT * from chpostgres.public.{table_name};"
        df = pd.read_sql_query(query, conn)
        logger.info(df)
        logger.info("Removing NaT and NaN")
        df.replace({pd.NaT: None, np.nan: None}, inplace=True)
        logger.info("Removed NaT and NaN")
        conn.close()
        return df

    @task()
    def load():
        table_name = context.op_config["table_name"]
        logger = get_dagster_logger()
        database = "TEST_DB"
        schema = "TEST"
        table = table_name
        dataframe = table1

        temp_columns = dataframe.columns
        columns = []
        for col in temp_columns:
            columns.append(col)

        col_list = []
        src_col_list = []
        update_list = []
        index = 1

        for col in columns:
            col_list.append(f"$1:{col} as {col}")
            src_col_list.append(f"source.{col}")
            update_list.append(f"target.{col} = source.{col}")
            index = index + 1

        file_format_name = f"{database}_{schema}_{table}_json_format_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        file_format_name = f"create or replace file format my_json_format type = 'json' strip_outer_array = true;"
        stage_name = f"{database}_{schema}_{table}_stage_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        file_name = f"{database}_{schema}_{table}_data_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.json"  # For Local

        try:
            total_data_count = len(dataframe)
            logger.info("Converting Dataframe into Dict")
            dict_data = dataframe.to_dict(orient='records')
            logger.info("Converted Dataframe into Dict")
            logger.info("Converting Dict into JSON")
            with open(file_name, 'w') as json_file:
                json.dump(dict_data, json_file, default=str)
            logger.info("Converted Dict into JSON")

            conn = conn_snowflaks()
            cursor = conn.cursor()

            # Create a temporary stage, file format, and upload the CSV file to the stage
            cursor.execute(f'USE {database};')
            cursor.execute(f'USE {schema};')
            logger.info("Creating File Format")
            cursor.execute(file_format_name)

            logger.info("Creating Stage")

            cursor.execute(f"CREATE OR REPLACE STAGE {stage_name} file_format = my_json_format;")
            logger.info("Stage Created")
            logger.info("Uploading File to Stage")


            logger.info(f"PUT file://E:\Squadrone\etl\etl\{file_name}")
            file_upload_command = f"PUT file://E:\Squadrone\etl\etl\{file_name} @{stage_name}"  # For Local
            logger.info('Window command')
            cursor.execute(file_upload_command)
            logger.info("File Uploaded")

            query = f'''COPY INTO {database}.{schema}.{table}
                                    FROM @{stage_name}/{file_name}
                                    FILE_FORMAT = (TYPE ="JSON" STRIP_OUTER_ARRAY = TRUE)
                                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                                        '''

            cursor.execute(query)
            logger.info("Dataaaaa Uploaded")



            # Upsert data into Snowflake table
            cursor.execute(query)
            return total_data_count
        except Exception as e:
            raise e

    postgres_data = transform()
    load(postgres_data)
tutorial_taskflow_api()
