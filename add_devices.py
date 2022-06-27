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

# Devices parameters
registry_id = f"registre-{my_id}"
gateway_id = "flood-control-gw"
devices_list = ["canal-cleaner", "weather-station"]


def main():
    # Create devices
    print("Creating devices...")
    for device_id in devices_list:
        manager.create_device(service_account_json, project_id, cloud_region, registry_id, device_id)
    # Bind created device to gateway
    print("Binding devices to Gateway...")
    for device_id in devices_list:
        manager.bind_device_to_gateway(service_account_json, project_id, cloud_region, registry_id, device_id,
                                       gateway_id)


if __name__ == '__main__':
    main()