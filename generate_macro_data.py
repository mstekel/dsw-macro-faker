import pyodbc
import uuid
import random
import math
from datetime import datetime
from collections import namedtuple

# Define connection details
CONN_MSSQL = {
    "DRIVER": "{SQL Server}",
    "SERVER": "spc23.DEV.pyramidanalytics.com",
    "DATABASE": "PyramidG2",
    "UID": "sa",
    "PWD": "snap3535"
}

CONN_STR_PG = {
    "DRIVER": "{PostgreSQL Unicode}",
    "SERVER": "spc23.DEV.pyramidanalytics.com",
    "DATABASE": "pyramidg2",
    "UID": "sa",
    "PWD": "snap3535"
}

# Define constants
MODELS_FOLDER_ID = '52f88791-fd6e-4c41-8442-4f3ce8a6af71'
ALGORITHMS = ['Weka', 'MLlib', 'Scikit-learn', 'R', 'Python']
ML_CATEGORIES = range(1, 6)
ERROR_TYPES = ['Invalid Argument: Incompatible shapes', 'Non Numeric Data',
               'Inconsistent Label Encoding', 'Feature Scaling', 'IncompatibleClassException']

# Define named tuples for data structures
StagedModel = namedtuple('StagedModel', ['model_id', 'execution_id', 'lab_id', 'lab_name', 'algo_engine',
                                         'ml_model_name', 'algo_version_name', 'algo_config_id', 'ml_category',
                                         'data_model_name', 'server_id', 'db_name', 'accuracy', 'total_instances',
                                         'lab_execution_id'])

ModelUsage = namedtuple('ModelUsage', ['usage_id', 'staged_model_id', 'materialized_data_model_id',
                                       'predict_node_id', 'last_drift_score', 'last_error_rate'])

PredictionCount = namedtuple('PredictionCount', ['execution_id', 'timestamp', 'staged_model_id',
                                                 'success_count', 'error_count'])

PredictionError = namedtuple('PredictionError', ['execution_id', 'timestamp', 'staged_model_id',
                                                 'error_type', 'error_count'])


def connect_to_db():
    # Implement code to connect to the SQL Server DB
    connection = pyodbc.connect(**CONN_MSSQL)
    return connection.cursor()


def generate_uuid():
    # Generate and return a new UUID as varchar
    return str(uuid.uuid4())


def generate_random_algo_engine():
    return random.choice(ALGORITHMS)


def generate_random_model_name(algorithm, index):
    return f"{algorithm} Model {index}"


def generate_tags(algorithm):
    return ['model', algorithm, random.choice(['regression', 'classification']),
            algorithm, random.choice(['java', 'python'])]


def create_staged_models(cursor, num_models=20):
    staged_models = []
    for i in range(num_models):
        model_id = generate_uuid()
        execution_id = generate_uuid()
        lab_id = generate_uuid()
        lab_name = f"Lab {i}"
        algo_engine = generate_random_algo_engine()
        ml_model_name = generate_random_model_name(algo_engine, i)
        algo_version_name = f"Version {i}"
        algo_config_id = generate_uuid()
        ml_category = random.choice(ML_CATEGORIES)
        data_model_name = f"Data Model {i}"
        server_id = generate_uuid()
        db_name = f"DB {i}"
        accuracy = random.uniform(0.7, 0.95)  # Random accuracy between 0.7 and 0.95
        total_instances = random.randint(100, 1000)
        lab_execution_id = generate_uuid()

        staged_model = StagedModel(model_id, execution_id, lab_id, lab_name, algo_engine, ml_model_name,
                                   algo_version_name, algo_config_id, ml_category, data_model_name,
                                   server_id, db_name, accuracy, total_instances, lab_execution_id)
        staged_models.append(staged_model)

        insert_dsw_staged_model(cursor, staged_model)
        insert_cms_item(cursor, ml_model_name, model_id)
        insert_tags(cursor, model_id, generate_tags(algo_engine))

    return staged_models


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
                           MODELS_FOLDER_ID, None, 1, None, None))

def insert_tags(cursor, model_id, tags):
    query0 = """
    SELECT tag_id FROM content_tbl_tag WHERE tag_description = ?
    """
    query1 = """
        INSERT INTO content_tbl_tag
        (tag_id, created_date, created_by, tag_description, tag_type)
        VALUES(?, ?, ?, ?, ?)        
    """
    query2 = """
        INSERT INTO content_tbl_tag_usage
        (id, item_id, tag_id)
        VALUES(?, ?, ?)        
    """

    for tag in tags:
        cursor.execute(query0, (tag))
        record = cursor.fetchone()
        if record is not None:
            tag_id = record[0]
        else:
            tag_id = generate_uuid()
            cursor.execute(query1, (tag_id, '2023-12-31 14:49:32.920', '17398184-6c03-4bfe-ad82-ebfb02dd3d10', tag, 0))

        tag_usage_id = generate_uuid()
        cursor.execute(query2, (tag_usage_id, model_id, tag_id))



def insert_dsw_staged_model(cursor, staged_model):
    query = """
        INSERT INTO dsw_staged_models 
        (id, execution_id, lab_id, lab_name, algo_engine, ml_model_name, algo_version_name, algo_config_id,
        ml_category, data_model_name, server_id, db_name, accuracy, total_number_of_instances, lab_execution_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(query, staged_model)


def insert_ml_model_usage(cursor, staged_model_id):
    model_usages = []
    for _ in range(3):
        usage_id = generate_uuid()
        materialized_data_model_id = generate_uuid()
        predict_node_id = generate_uuid()
        last_drift_score = random.uniform(0.1, 0.5)  # Random drift score between 0.1 and 0.5
        last_error_rate = random.uniform(0.01, 0.1)  # Random error rate between 0.01 and 0.1

        model_usage = ModelUsage(usage_id, staged_model_id, materialized_data_model_id, predict_node_id,
                                 last_drift_score, last_error_rate)
        model_usages.append(model_usage)

        query = """
        INSERT INTO ml_model_usage 
        (id, ml_model_id, materialized_data_model_id, predict_node_id, last_drift_score, last_error_rate)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query, model_usage)

    return model_usages


def insert_prediction_counts(cursor, staged_model_id):
    prediction_counts = []
    for month in range(1, 13):
        execution_id = generate_uuid()
        timestamp = datetime(2023, month, 1)
        success_count = int(50 + month * 10)  # Increase success count every month
        error_count = int(100 - month * 5)  # Decrease error count every month

        prediction_count = PredictionCount(execution_id, timestamp, staged_model_id, success_count, error_count)
        prediction_counts.append(prediction_count)

        query = """
        INSERT INTO prediction_count 
        (execution_id, timestamp, ml_model_id, success_count, error_count)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, prediction_count)

    return prediction_counts


def insert_prediction_errors(cursor, staged_model_id):
    prediction_errors = []
    for month in range(1, 13):
        execution_id = generate_uuid()
        timestamp = datetime(2023, month, 1)
        error_type = random.choice(ERROR_TYPES)
        error_count = int(30 - month * 2)  # Decrease error count every month

        prediction_error = PredictionError(execution_id, timestamp, staged_model_id, error_type, error_count)
        prediction_errors.append(prediction_error)

        query = """
        INSERT INTO prediction_error 
        (execution_id, timestamp, ml_model_id, error_type, error_count)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, prediction_error)

    return prediction_errors


def truncate_tables(cursor):
    """
    Truncates specific tables involved in the data population script.
    """
    tables = [
        "dsw_staged_models",
        "ml_model_usage",
        "prediction_count",
        "prediction_error",
        "content_tbl_tag"
    ]

    for table in tables:
        query = f"TRUNCATE TABLE {table}"
        cursor.execute(query)

    cursor.execute("DELETE FROM content_tbl_item WHERE item_type=10")


def main():
    cursor = connect_to_db()

    try:
        truncate_tables(cursor)
        staged_models = create_staged_models(cursor)
        for staged_model in staged_models:
            insert_ml_model_usage(cursor, staged_model.model_id)
            insert_prediction_counts(cursor, staged_model.model_id)
            insert_prediction_errors(cursor, staged_model.model_id)

        cursor.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        cursor.rollback()

    finally:
        cursor.close()

if __name__ == "__main__":
    main()