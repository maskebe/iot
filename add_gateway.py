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

# Gateway parameters
registry_id = f"registre-{my_id}"
device_id = None
gateway_id = "flood-control-gw"
certificate_file = "rsa_cert.pem"
algorithm = "RS256"


def main():
    # Create gateway
    print("Creating gateway...")
    manager.create_gateway(service_account_json, project_id, cloud_region, registry_id, device_id, gateway_id,
                           certificate_file, algorithm)


if __name__ == '__main__':
    main()
