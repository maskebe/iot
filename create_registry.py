from google.api_core.exceptions import AlreadyExists
from google.api_core.exceptions import NotFound
import manager
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

# Registry parameters
registry_id = f"registre-{my_id}"
default_topic_id = f"flood_{my_id}"
default_pubsub_topic = f"projects/{project_id}/topics/{default_topic_id}"
subfolders_topic_id = [{"topic_id": f"weather_station_{my_id}", "subfolder": "weather_station"},
                       {"topic_id": f"canal_cleaner_{my_id}", "subfolder": "canal_cleaner"}]


def create_topic(project_id, topic_id):
    try:
        manager.create_iot_topic(project_id, topic_id)
        print("Topic created")
    except AlreadyExists:
        print(f"Topic '{topic_id}' already exist. Skip.")


def main():
    # Create PubSub topics if not exist
    event_notification_configs = []
    for topic in subfolders_topic_id:
        pubsub_topic = f"projects/{project_id}/topics/{topic['topic_id']}"
        event_notification_configs.append({"pubsub_topic_name": pubsub_topic,
                                           "subfolder_matches": topic['subfolder']})
        print(f"Creating PubSub topic {pubsub_topic}...")
        create_topic(project_id, topic['topic_id'])
    event_notification_configs.append({"pubsub_topic_name": default_pubsub_topic})
    print(f"Creating PubSub topic {default_pubsub_topic}...")
    create_topic(project_id, default_topic_id)

    # Create registry if not exist
    print(f"Creating registry {registry_id}...")
    try:
        manager.get_registry(service_account_json, project_id, cloud_region, registry_id)
        print(f"Registry already exist. Skip.")
    except NotFound:
        response = manager.create_registry_v2(service_account_json, project_id, cloud_region, default_pubsub_topic,
                                   registry_id, event_notification_configs)
        print(response)


if __name__ == '__main__':
    main()