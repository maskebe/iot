from concurrent.futures import TimeoutError
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
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

# Pubsub parameters
subscriptions = [{"subscription_id": f"sub_weather_station_{my_id}", "topic_id": f"weather_station_{my_id}"},
                 {"subscription_id": f"sub_canal_cleaner_{my_id}", "topic_id": f"canal_cleaner_{my_id}"}]
timeout = 5.0


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    #print(f"Received {message}")
    print(message)
    print(message.data)
    message.ack()


def subscribe(subscription_id, topic_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    topic_path = subscriber.topic_path(project_id, topic_id)

    try:
        print(f"Creation of subscription {subscription_path}")
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
    except AlreadyExists:
        print(f"Subscription already exists. Skip.")
    print(f"Listening for messages on PubSub topic projects/{project_id}/topics/{topic_id}...\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.


def main():
    for subscription in subscriptions:
        subscribe(subscription["subscription_id"], subscription["topic_id"])


if __name__ == '__main__':
    main()