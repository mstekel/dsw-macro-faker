import pyodbc
import uuid
import random
from datetime import datetime, timedelta

conn_mssql = {
    "DRIVER": "{SQL Server}",
    "SERVER": "spc23.DEV.pyramidanalytics.com",
    "DATABASE": "PyramidG2",
    "UID": "sa",
    "PWD": "snap3535"
}

# to make pyodbc work with pg:
# 1. Install the pg driver from here: https://ftp.postgresql.org/pub/odbc/versions/msi/psqlodbc_16_00_0000.zip
# 2. The server's IP should be added to the pg_hba.conf file (in the pg install dir)
# Example:
# IPv4 local connections:
# host    all     		all             172.29.3.203/32            md5
# IPv6 local connections:
# host    all             all             fe80::c6b9:b8aa:ff92:6d5a%9/128                 md5
conn_str_pg = {
    "DRIVER": "{PostgreSQL Unicode}",
    "SERVER": "spc23.DEV.pyramidanalytics.com",
    "DATABASE": "pyramidg2",
    "UID": "sa",
    "PWD": "snap3535"
}

# to save the staged models to a specific CMS folder - provide the folder ID (find it manually in Pyramid DB)
models_folder_id = '01eca701-b053-4e94-94b6-1b2a9567bbf1'

def connect_to_db():
    # Implement code to connect to the SQL Server DB
    connection = pyodbc.connect(**conn_mssql)
    return connection.cursor()


def generate_uuid():
    # Generate and return a new UUID as varchar
    return str(uuid.uuid4())


def create_staged_models(cursor):
    for i in range(20):
        model_id = generate_uuid()
        execution_id = generate_uuid()
        lab_id = generate_uuid()
        lab_name = f"Lab {i}"
        algo_engine = generate_random_algo_engine()
        ml_model_name = generate_random_model_name(algo_engine, i)
        algo_version_name = f"Version {i}"
        algo_config_id = generate_uuid()
        ml_category = random.randint(1, 5)
        data_model_name = f"Data Model {i}"
        server_id = generate_uuid()
        db_name = f"DB {i}"
        accuracy = random.uniform(0.7, 0.95)  # Random accuracy between 0.7 and 0.95
        total_instances = random.randint(100, 1000)
        lab_execution_id = generate_uuid()

        tags = generate_tags(algo_engine)

        insert_dsw_staged_model(accuracy, algo_config_id, algo_engine, algo_version_name, cursor, data_model_name, db_name,
                                execution_id, lab_id, lab_name, ml_category, ml_model_name, model_id, server_id, total_instances, lab_execution_id)
        insert_cms_item(cursor, ml_model_name, model_id)

        # Create ML model usages, prediction counts, and errors for each staged model
        insert_ml_model_usage(cursor, model_id)
        insert_prediction_counts(cursor, model_id)
        insert_prediction_errors(cursor, model_id)


def insert_cms_item(cursor, ml_model_name, model_id):
    query = """
        INSERT INTO content_tbl_item
        (id, created_by, created_date, description, family_id, is_active, is_deleted, item_type, 
        modified_by, modified_date, name, version, version_description, folder_id, sub_type, root_folder_type, 
        oneoff_document_id, locked_by_user)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(query, (model_id, 'bbc8925d-b5e9-4c4a-8e6d-539e528e3856', '2023-12-31 14:49:32.920', None,
                           '7918e931-2222-46e1-b3bf-2e638cb35b1a', 1, 0, 10, 'bbc8925d-b5e9-4c4a-8e6d-539e528e3856',
                           '2023-12-31 14:49:32.920', ml_model_name, '2023.11.038', None,
                           models_folder_id, None, 1, None, None))


def insert_dsw_staged_model(accuracy, algo_config_id, algo_engine, algo_version_name, cursor, data_model_name, db_name,
                            execution_id, lab_id, lab_name, ml_category, ml_model_name, model_id, server_id, total_instances, lab_execution_id):
    query = """
        INSERT INTO dsw_staged_models 
        (id, execution_id, lab_id, lab_name, ml_model_name, algo_engine, algo_version_name, algo_config_id,
        ml_category, data_model_name, server_id, db_name, accuracy, total_number_of_instances, lab_execution_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(query, (model_id, execution_id, lab_id, lab_name, ml_model_name, algo_engine, algo_version_name,
                           algo_config_id, ml_category, data_model_name, server_id, db_name, accuracy,
                           total_instances, lab_execution_id))


def generate_random_algo_engine():
    algorithms = ['Weka', 'MLlib', 'Scikit-learn', 'R', 'Python']
    return random.choice(algorithms)


def generate_random_model_name(algorithm, index):
    return f"{algorithm} Model {index}"


def generate_tags(algorithm):
    tags = ['model', algorithm, random.choice(['regression', 'classification']),
            algorithm, random.choice(['java', 'python'])]
    return tags


def insert_ml_model_usage(cursor, staged_model_id):
    for i in range(3, 6):
        usage_id = generate_uuid()
        materialized_data_model_id = generate_uuid()
        predict_node_id = generate_uuid()
        last_drift_score = random.uniform(0.1, 0.5)  # Random drift score between 0.1 and 0.5
        last_error_rate = random.uniform(0.01, 0.1)  # Random error rate between 0.01 and 0.1

        query = """
        INSERT INTO ml_model_usage 
        (id, ml_model_id, materialized_data_model_id, predict_node_id, last_drift_score, last_error_rate)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query, (usage_id, staged_model_id, materialized_data_model_id, predict_node_id,
                               last_drift_score, last_error_rate))


def insert_prediction_counts(cursor, staged_model_id):
    for month in range(1, 13):
        execution_id = generate_uuid()
        timestamp = datetime(2023, month, 1)
        success_count = int(50 + month * 10)  # Increase success count every month
        error_count = int(100 - month * 5)  # Decrease error count every month

        query = """
        INSERT INTO prediction_count 
        (execution_id, timestamp, ml_model_id, success_count, error_count)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (execution_id, timestamp, staged_model_id, success_count, error_count))


def insert_prediction_errors(cursor, staged_model_id):
    for month in range(1, 13):
        execution_id = generate_uuid()
        timestamp = datetime(2023, month, 1)
        error_type = random.choice(['Invalid Argument: Incompatible shapes', 'Non Numeric Data',
                                    'Inconsistent Label Encoding',
                                    'Feature Scaling', 'IncompatibleClassException'])
        error_count = int(30 - month * 2)  # Decrease error count every month

        query = """
        INSERT INTO prediction_error 
        (execution_id, timestamp, ml_model_id, error_type, error_count)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (execution_id, timestamp, staged_model_id, error_type, error_count))


def main():
    cursor = connect_to_db()

    try:
        create_staged_models(cursor)
        cursor.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        cursor.rollback()

    finally:
        cursor.close()


if __name__ == "__main__":
    main()
