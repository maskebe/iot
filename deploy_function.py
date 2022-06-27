from google.cloud import storage
from google.cloud import functions_v1
import os
import json
import zipfile


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

# PubSup parameters
topic_id = f"canal_cleaner_{my_id}"


def create_bucket(bucket_name, location, bucket_class="STANDARD"):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = bucket_class
    new_bucket = storage_client.create_bucket(bucket, location=location)
    print(f"Created bucket {new_bucket.name} in {new_bucket.location} with storage class {new_bucket.storage_class}")
    return new_bucket


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {bucket_name}/{destination_blob_name}")


def zip_directory(folder_path, zip_path):
    with zipfile.ZipFile(zip_path, mode='w') as zipf:
        len_dir_path = len(folder_path)
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, file_path[len_dir_path:])


def list_functions(project, location):
    # Create a client
    client = functions_v1.CloudFunctionsServiceClient()
    # Initialize request argument(s)
    request = functions_v1.ListFunctionsRequest(parent=f"projects/{project}/locations/{location}")
    # Make the request
    return client.list_functions(request=request)


def function_exists(project, location, name):
    functions_list = list_functions(project, location)
    # Handle the response
    for response in functions_list:
        if name == response.name:
            return True
    return None


def create_function(params):
    # Create a client
    client = functions_v1.CloudFunctionsServiceClient()
    # Initialize request argument(s)
    function = functions_v1.CloudFunction()
    function.source_archive_url = params["source_archive_url"]
    function.name = params["name"]
    function.runtime = params["runtime"]
    function.entry_point = params["entry_point"]
    function.event_trigger = params["event_trigger"]
    request = functions_v1.CreateFunctionRequest(location=params["location"], function=function)
    # Make the request
    operation = client.create_function(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()
    # Handle the response
    print(response)


def update_function(params):
    # Create a client
    client = functions_v1.CloudFunctionsServiceClient()
    # Initialize request argument(s)
    function = functions_v1.CloudFunction()
    function.source_archive_url = params["source_archive_url"]
    function.name = params["name"]
    function.runtime = params["runtime"]
    function.entry_point = params["entry_point"]
    function.event_trigger = params["event_trigger"]
    request = functions_v1.UpdateFunctionRequest(function=function,)
    # Make the request
    operation = client.update_function(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()
    # Handle the response
    print(response)


def main():
    # Create bucket if not exist
    storage_client = storage.Client()
    bucket_name = f"functions_repository"
    bucket = storage_client.bucket(bucket_name)
    print("Creating bucket...")
    if not bucket.exists():
        create_bucket(bucket_name, cloud_region, bucket_class="STANDARD")
    else:
        print(f"The bucket '{bucket_name}' already exists. SKip...")
    # Zip the function code
    print("Creating zip archive of function source code...")
    source_path = "cloud-functions/flood_prediction"
    zip_path = f"flood_prediction_{my_id}.zip"
    zip_directory(source_path, zip_path)
    # Upload on created bucket
    print("Uploading zipped file...")
    upload_blob(bucket_name, zip_path, zip_path)
    # Create function if not exists, else update
    params = {
        "location": f"projects/{project_id}/locations/{cloud_region}",
        "source_archive_url": f"gs://{bucket_name}/{zip_path}",
        "name": f"projects/{project_id}/locations/{cloud_region}/functions/flood-prediction-{my_id}",
        "runtime": "python38",
        "entry_point": "predict",
        "event_trigger": {
            "event_type": "providers/cloud.pubsub/eventTypes/topic.publish",
            "resource": f"projects/{project_id}/topics/{topic_id}",
            "service": "",
        }
    }
    print("Creating function...")
    if not function_exists(project_id, cloud_region, params["name"]):
        create_function(params)
    else:
        print("Function already exists. Updating...")
        update_function(params)


if __name__ == '__main__':
    main()