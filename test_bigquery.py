from google.cloud import bigquery
import json
import os


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


def preview_weather_station_table(table_id):
    client = bigquery.Client(project=project_id, location=cloud_region)
    table = client.get_table(table_id)
    rows = client.list_rows(table, max_results=10).to_dataframe()
    df = rows[["humidity", "temperature", "water_level", "timestamp"]]
    print(df)


def main():
    table_id = f"meteorological_station_dataset.station_{my_id}"
    print(f"Reading data from BigQuery table {table_id}...")
    preview_weather_station_table(table_id)


if __name__ == '__main__':
    main()