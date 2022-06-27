import google.cloud.logging
from datetime import datetime, timedelta, timezone
import os
import json

# Set environment variable
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "project-credentials.json"

# Load config
with open("config.json", "r") as fd:
    config = json.loads(fd.read())

my_id = config["my_id"]
project_id = config["project_id"]
service_account_json = config["service_account_json"]


def get_logs(filter_):
    client = google.cloud.logging.Client()
    for entry in client.list_entries(filter_=filter_):
        print(f"[{entry.timestamp}][{entry.severity}] {entry.resource.labels['function_name']}")
        print(f"\t{entry.payload}")


def main():
    period_interval = 2
    time_limit = datetime.now(timezone.utc) - timedelta(hours=period_interval)
    time_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    print(f"Getting last {period_interval} hours logs...")
    filter_ = (f'logName="projects/{project_id}/logs/cloudfunctions.googleapis.com%2Fcloud-functions"'
               f' AND resource.labels.function_name="flood-prediction-{my_id}"'
               f' AND timestamp>="{time_limit.strftime(time_format)}"')
    get_logs(filter_)


if __name__ == '__main__':
    main()


