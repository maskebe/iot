import manager
import json
import os

from google.cloud import iot_v1
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import functions_v1
from googleapiclient.discovery import build
import google.auth


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
serial_port = config["serial_port"]

# GCP services parameters
registry_id = f"registre-{my_id}"


def get_auth_method(auth_value):
    auth_method = "UNKNOW"
    for number, method in enumerate(iot_v1.GatewayAuthMethod):
        if auth_value == number:
            auth_method = method.name
    return auth_method


def show_general_info():
    print("############################ GENERAL INFO ######################################")
    print(f"Project ID : {project_id}")
    print(f"Cloud Region : {cloud_region}")
    print(f"User ID : {my_id}")


def registry_summary():
    print("############################## REGISTRY ########################################")
    registry = manager.get_registry(service_account_json, project_id, cloud_region, registry_id)
    if not registry:
        print(f"Registry '{registry_id}' not exist.")
        return
    print(f"{registry.id}")
    print(f"\t- name : {registry.name}")
    print(f"\t- MQTT : {registry.mqtt_config.mqtt_enabled_state.name}")
    print(f"\t- HTTP : {registry.http_config.http_enabled_state.name}")
    print(f"\t- PubSub topics :")
    for config in registry.event_notification_configs:
        print(f"\t\t* {config.pubsub_topic_name}")
        print(f"\t\t  - subfolder : {config.subfolder_matches}")


def list_devices():
    print("############################## DEVICES #######################################")
    devices = manager.list_devices(service_account_json, project_id, cloud_region, registry_id)
    if not devices:
        print("No device found!")
        print("############################### GATEWAYS #######################################")
        print("No gateway found!")
        return
    i = 1
    gateways = []
    for device in devices:
        if device.gateway_config.gateway_type.name == "GATEWAY":
            gateways.append(device)
        else:
            print(f"{device.id}")
            print(f"\t- name : {device.name}")
            print(f"\t- auth method : {get_auth_method(device.gateway_config.gateway_auth_method)}")
            print(f"\t- last event : {device.last_event_time}")
            print(f"\t- last error : '{device.last_error_status.message}'")
            i += 1
    # Gateways info
    print("############################### GATEWAYS #######################################")
    if not gateways:
        print("No gateway found!")
        return
    i = 1
    for gateway in gateways:
        print(f"{gateway.id}")
        print(f"\t- name : {gateway.name}")
        print(f"\t- auth method : {get_auth_method(gateway.gateway_config.gateway_auth_method)}")
        print(f"\t- certificate format : {list(gateway.credentials)[0].public_key.format.name}")
        print(f"\t- last heartbeat : {gateway.last_heartbeat_time}")
        print(f"\t- last error : '{gateway.last_error_status.message}'")
        # Bound devices
        bound_devices = manager.list_devices_for_gateway(service_account_json, project_id, cloud_region, registry_id,
                                                         gateway.id)
        print(f"\t- bound devices :")
        if not devices:
            print(f"\t\t* No device!")
        else :
            for device in bound_devices:
                print(f"\t\t* {device.id}")


def list_pubsub_topics():
    print("############################### PUBSUB #######################################")
    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"
    topics = publisher.list_topics(request={"project": project_path})
    if not topics:
        print("No topic found!")
        return
    found = False
    for topic in topics:
        if topic.name.endswith(f"_{my_id}"):
            if not found:
                print("Topics :")
                found = True
            print(f"\t- {topic.name}")
    if not found:
        print("No topic found!")


def list_dataflow_jobs():
    print("############################### DATAFLOW #######################################")
    df_service = build('dataflow', 'v1b3')
    response = df_service.projects().locations().jobs().list(projectId=project_id, location=cloud_region).execute()
    for job in response["jobs"]:
        print(job['name'])
        print(f"\t- type : {job['type']}")
        print(f"\t- create time : {job['createTime']}")
        print(f"\t- state : {job['currentState']}")


def list_bq_datasets():
    print("############################### BIGQUERY #######################################")
    sensors_datasets = ["meteorological_station_dataset"]
    training_datasets = ["ml_meteo_dataset"]
    bqml_datasets = ["ml_model_meteo"]
    print("Sensors datasets :")
    for dataset_id in sensors_datasets:
        print(f"\t- {dataset_id}")
        list_bq_tables(dataset_id, filter_=f"_{my_id}")
    print("Training datasets :")
    for dataset_id in training_datasets:
        print(f"\t- {dataset_id}")
        list_bq_tables(dataset_id)
    print("BigQuery ML models :")
    for dataset_id in bqml_datasets:
        print(f"\t- {dataset_id}")
        list_bqml_models(dataset_id)


def list_bq_tables(dataset_id, filter_=None):
    client = bigquery.Client()
    tables = client.list_tables(dataset_id)
    found = False
    for table in tables:
        if filter_:
            if table.table_id.endswith(filter_):
                found = True
                print(f"\t\t* {table.table_id}")
        else:
            print(f"\t\t* {table.table_id}")
    if filter_ and not found:
        print("\t\t* No table found!")


def list_bqml_models(dataset_id):
    client = bigquery.Client()
    models = client.list_models(dataset_id)
    for model in models:
        if model.model_id.endswith(f"_{my_id}"):
            print(f"\t\t* {model.model_id}")


def list_cloud_functions():
    print("########################### CLOUD FUNCTIONS ####################################")
    client = functions_v1.CloudFunctionsServiceClient()
    request = functions_v1.ListFunctionsRequest(parent=f"projects/{project_id}/locations/{cloud_region}")
    functions = client.list_functions(request=request)
    found = False
    for function in functions:
        if function.name.endswith(f"-{my_id}"):
            found = True
            function_id = function.name.split("/")[-1]
            print(function_id)
            print(f"\t- name : {function.name}")
            print(f"\t- runtime : {function.runtime}")
            print(f"\t- entry point : {function.entry_point}")
            print(f"\t- trigger :")
            print(f"\t\t- type : {function.event_trigger.event_type}")
            print(f"\t\t- resource (topic) : {function.event_trigger.resource}")
            print(f"\t- memory (MB) : {function.available_memory_mb}")
            print(f"\t- source code : {function.source_archive_url}")
            print(f"\t- last update : {function.update_time}")
            print(f"\t- status : {function.status.name}")
    if not found:
        print("No function found!")


def main():
    # General info
    show_general_info()

    # Show registry summary
    registry_summary()

    # List devices and gateways
    list_devices()

    # List PubSub topics
    list_pubsub_topics()

    # List dataflow jobs
    #list_dataflow_jobs()
    print("############################### DATAFLOW #######################################")
    print("No job found!")

    # List BigQuery datasets
    list_bq_datasets()

    # List cloud functions
    list_cloud_functions()

    print("################################################################################")


if __name__ == '__main__':
    main()