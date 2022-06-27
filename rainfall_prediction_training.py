from google.cloud import bigquery
import json
import os
import time


# Set environment variable
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "project-credentials.json"

# Load config
with open("config.json", "r") as fd:
    config = json.loads(fd.read())

my_id = config["my_id"]
project_id = config["project_id"]
service_account_json = config["service_account_json"]
cloud_region = config["cloud_region"]


def preview_table(table_id):
    client = bigquery.Client(project=project_id, location=cloud_region)
    table = client.get_table(table_id)
    rows = client.list_rows(table, max_results=10).to_dataframe()
    df = rows[["Month", "DryBulbCelsius", "RelativeHumidity", "WindSpeed", "StationPressure", "isRain"]]
    print(df)


def train_model(ml_model_id):
    client = bigquery.Client(project=project_id, location=cloud_region)
    query = f"""    
        CREATE OR REPLACE MODEL
          `{ml_model_id}` OPTIONS ( model_type='logistic_reg', 
            AUTO_CLASS_WEIGHTS = TRUE,
            DATA_SPLIT_METHOD = 'RANDOM',
            DATA_SPLIT_EVAL_FRACTION = 0.2
        ) AS
        SELECT
          isRain AS label,
          RelativeHumidity AS humidity, 
          DryBulbCelsius AS temperature,
          Month AS month
        FROM
          `ml_meteo_dataset.weather_azure`
        """
    client.query(query)
    print(f"Model created.")


def get_training_info(ml_model_id):
    client = bigquery.Client(project=project_id, location=cloud_region)
    query = f"""    
         SELECT
           *
         FROM
           ML.TRAINING_INFO(MODEL `{ml_model_id}`)
        """
    query_job = client.query(query)
    for row in query_job:
        row_list = list(row.items())
        print(row_list)


def evaluate_model(ml_model_id, table_id):
    client = bigquery.Client(project=project_id, location=cloud_region)
    query = f"""    
        SELECT
           *
        FROM
           ML.EVALUATE(MODEL `{ml_model_id}`, (
        SELECT
           isRain AS label,
           RelativeHumidity AS humidity,
           DryBulbCelsius AS temperature,
           Month as month
        FROM
           `{table_id}`))
       """
    query_job = client.query(query)
    for row in query_job:
        row_list = list(row.items())
        print(row_list)


def predict_rain():
    bq_client = bigquery.Client()
    query = f"""
        SELECT
          *
        FROM
           ML.PREDICT(MODEL `ml_model_meteo.rainfall_prediction_{my_id}`, (
        SELECT
           CAST(humidity as INT64) AS humidity,
           CAST(temperature as FLOAT64) AS temperature,
           EXTRACT(MONTH FROM timestamp) AS month
        FROM
           `meteorological_station_dataset.station_{my_id}`
        WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) AND CURRENT_TIMESTAMP() 
        ))
        ORDER BY predicted_label DESC
        LIMIT 1
            """
    query_job = bq_client.query(query)

    for row in query_job:
        # Row((1, [{'label': 1, 'prob': 0.5441665957315596}, {'label': 0, 'prob': 0.45583340426844043}], 87, 23.9, 4), {'predicted_label': 0, 'predicted_label_probs': 1, 'humidity': 2, 'temperature': 3, 'month': 4})
        row_list = list(row.items())
        print(row_list)


def main():
    dataset_table_id = "ml_meteo_dataset.weather_azure"
    evaluation_dataset_table_id = "ml_meteo_dataset.weather_azure_evaluation"
    ml_model_id = f"ml_model_meteo.rainfall_prediction_{my_id}"

    print(f"Preview training dataset {dataset_table_id}...")
    preview_table(dataset_table_id)

    print(f"...\nCreating model {ml_model_id}...")
    train_model(ml_model_id)

    print("Getting training info...")
    get_training_info(ml_model_id)

    time.sleep(10)
    print(f"Evaluate model {ml_model_id}...")
    evaluate_model(ml_model_id, evaluation_dataset_table_id)

    print(f"Test prediction with data from table meteorological_station_dataset.station_{my_id}")
    predict_rain()


if __name__ == '__main__':
    main()