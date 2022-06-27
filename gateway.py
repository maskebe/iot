import datetime
import json
import ssl
import time
import jwt
import paho.mqtt.client as mqtt
from rfc3339 import rfc3339


# Load config
with open("config.json", "r") as fd:
    config = json.loads(fd.read())

my_id = config["my_id"]
project_id = config["project_id"]
service_account_json = config["service_account_json"]
cloud_region = config["cloud_region"]
serial_port = config["serial_port"]

# Gateway parameters
registry_id = f"registre-{my_id}"
device_id = None
gateway_id = "flood-control-gw"
certificate_file = "rsa_cert.pem"
private_key_file = "rsa_private.pem"
ca_certs = "roots.pem"
algorithm = "RS256"
mqtt_bridge_hostname = "mqtt.googleapis.com"
mqtt_bridge_port = 8883
jwt_expires_minutes = 1200


class GatewayState:
    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = ''

    # Host the gateway will connect to
    mqtt_bridge_hostname = ''
    mqtt_bridge_port = 8883

    # For all PUBLISH messages which are waiting for PUBACK. The key is 'mid'
    # returned by publish().
    pending_responses = {}

    # For all SUBSCRIBE messages which are waiting for SUBACK. The key is
    # 'mid'.
    pending_subscribes = {}

    # for all SUBSCRIPTIONS. The key is subscription topic.
    subscriptions = {}

    # Indicates if MQTT client is connected or not
    connected = False

gateway_state = GatewayState()

# [START iot_mqtt_jwt]
def create_jwt(project_id, private_key_file, algorithm, jwt_expires_minutes):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
            Args:
             project_id: The cloud project ID this device belongs to
             private_key_file: A path to a file containing either an RSA256 or
                             ES256 private key.
             algorithm: Encryption algorithm to use. Either 'RS256' or 'ES256'
             jwt_expires_minutes: The time in minutes before the JWT expires.
            Returns:
                An MQTT generated from the given project_id and private key,
                which expires in 20 minutes. After 20 minutes, your client will
                be disconnected, and a new JWT will have to be generated.
            Raises:
                ValueError: If the private_key_file does not contain a known
                key.
            """

    token = {
        # The time that the token was issued at
        'iat': datetime.datetime.now(tz=datetime.timezone.utc),
        # The time the token expires.
        'exp': (
            datetime.datetime.now(tz=datetime.timezone.utc) +
            datetime.timedelta(minutes=jwt_expires_minutes)),
        # The audience field should always be set to the GCP project id.
        'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
        algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)
# [END iot_mqtt_jwt]


# [START iot_mqtt_config]
def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print('on_connect', mqtt.connack_string(rc))

    gateway_state.connected = True

    # Subscribe to the config topic.
    #client.subscribe(gateway_state.mqtt_config_topic, qos=1)


def on_disconnect(client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print('on_disconnect', error_str(rc))
    gateway_state.connected = False

    # re-connect
    # NOTE: should implement back-off here, but it's a tutorial
    client.connect(
        gateway_state.mqtt_bridge_hostname, gateway_state.mqtt_bridge_port)


def on_publish(unused_client, userdata, mid):
    """Paho callback when a message is sent to the broker."""
    print('on_publish, userdata {}, mid {}'.format(userdata, mid))
    """
    try:
        client_addr, message = gateway_state.pending_responses.pop(mid)
        print(client_addr, message)
        #udpSerSock.sendto(message.encode(), client_addr)
        print('Pending response count {}'.format(
                len(gateway_state.pending_responses)))
    except KeyError:
        print('Unable to find key {}'.format(mid))
    """


def on_subscribe(unused_client, unused_userdata, mid, granted_qos):
    print('on_subscribe: mid {}, qos {}'.format(mid, granted_qos))
    """
    try:
        client_addr, response = gateway_state.pending_subscribes[mid]
        #udpSerSock.sendto(response.encode(), client_addr)
    except KeyError:
        print('Unable to find mid: {}'.format(mid))
    """


def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = message.payload
    qos = message.qos
    print('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
            payload.decode("utf-8"), message.topic, qos))
    try:
        client_addr = gateway_state.subscriptions[message.topic]
        #udpSerSock.sendto(payload, client_addr)
        print('Sent message to device')
    except KeyError:
        print('Nobody subscribes to topic {}'.format(message.topic))


def get_client(project_id, cloud_region, registry_id, gateway_id, private_key_file, algorithm, ca_certs,
                mqtt_bridge_hostname, mqtt_bridge_port, jwt_expires_minutes):
    """Create our MQTT client. The client_id is a unique string that
    identifies this device. For Google Cloud IoT Core, it must be in the
    format below."""
    client_template = 'projects/{}/locations/{}/registries/{}/devices/{}'
    client_id = client_template.format(project_id, cloud_region, registry_id, gateway_id)
    client = mqtt.Client(client_id)

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(username='unused',
                           password=create_jwt(project_id, private_key_file, algorithm, jwt_expires_minutes))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks.
    #     https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example,
    # the callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_subscribe = on_subscribe

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    return client
# [END iot_mqtt_config]


def attach_device(client, device_id):
    attach_topic = '/devices/{}/attach'.format(device_id)
    print(attach_topic)
    return client.publish(attach_topic, "", qos=1)


def detatch_device(client, device_id):
    detach_topic = '/devices/{}/detach'.format(device_id)
    print(detach_topic)
    return client.publish(detach_topic, "", qos=1)


# [START Serial port]
def read_serial_data(ser):
  """Read Arduino sensors from serial interface"""
  try:
      response = serial_receive(ser)
      response = response.rstrip().decode()
      print('Received from Arduino: {}'.format(response))
  except IOError:
    print('I/O Error')
    return None
  return response


def serial_receive(ser):
  """Write string to serial connection and return any response."""
  while True:
    try:
      time.sleep(0.01)
      state = ser.readline()
      if state:
        return state
    except:
      pass
  sleep(0.1)
  return 'E'


def init_serial(serial_port):
  import serial
  print('Creating and flushing serial port.')
  ser = serial.Serial(serial_port)
  with ser:
    ser.setDTR(False)
    time.sleep(1)
    ser.flushInput()
    ser.setDTR(True)
  ser = serial.Serial(serial_port, 9600, timeout=0.1)
  return ser


def get_devive_id(id):
    if (id == "1"):
        return "canal-cleaner"
    elif (id == "2"):
        return "weather-station"
    else :
        return None


def parse_sensors_data(data):
    if data[0] == '#':
        sensors_data = data.split("#")[1]
        command = sensors_data.split(",")[0]
        if (command == "detach"):
            id = sensors_data.split(",")[1]
            device = get_devive_id(id)
            return {"action": "detach", "device": device}
        elif (command == "attach"):
            id = sensors_data.split(",")[1]
            device = get_devive_id(id)
            return {"action": "attach", "device": device}
        else :
            id = sensors_data.split(",")[0]
            device = get_devive_id(id)
            timestamp = rfc3339(datetime.datetime.now())
            if (id == "1"):
                obstruction = int(sensors_data.split(",")[1])
                data = {"device_id": device, "obstruction": obstruction, "timestamp": timestamp}
                return {"action": "event", "device": device, "subfolder": "canal_cleaner", "data": data}
            elif (id == "2"):
                humidity = float(sensors_data.split(",")[1])
                temperature = float(sensors_data.split(",")[2])
                water_level = int(sensors_data.split(",")[3])
                data = {"device_id": device, "humidity": humidity, "temperature": temperature,
                        "water_level": water_level, "timestamp": timestamp}
                return {"action": "event", "device": device, "subfolder": "weather_station", "data": data}
            else:
                print("Unrecongnized sensor node ID")
                return None
    else:
        print('Error getting Arduino sensor values over serial')
        return None
# [END Serial port]


# [START iot_mqtt_run]
def main():
    global gateway_state
    gateway_state.mqtt_config_topic = f"/devices/{gateway_id}/config"
    gateway_state.mqtt_bridge_hostname = mqtt_bridge_hostname
    gateway_state.mqtt_bridge_port = mqtt_bridge_port

    client = get_client(project_id, cloud_region, registry_id, gateway_id, private_key_file, algorithm, ca_certs,
                        mqtt_bridge_hostname, mqtt_bridge_port, jwt_expires_minutes)

    ser = init_serial(serial_port)

    while True:
        client.loop()
        if gateway_state.connected is False:
            print('connect status {}'.format(gateway_state.connected))
            time.sleep(1)
            continue

        serial_data = read_serial_data(ser)
        command = parse_sensors_data(serial_data)
        if not command:
            print(f"invalid json command {serial_data}")
            continue
        action = command["action"]
        device_id = command["device"]
        template = '{{ "device": "{}", "command": "{}", "status" : "ok" }}'
        if action == 'event':
            print(f"Sending telemetry event for device {device_id}")
            payload = json.dumps(command["data"])
            mqtt_topic = f"/devices/{device_id}/events/{command['subfolder']}"
            print('Publishing message to topic {} with payload \'{}\''.format(mqtt_topic, payload))
            _, event_mid = client.publish(mqtt_topic, payload, qos=0)
            #response = template.format(device_id, 'event')
            #print('Save mid {} for response {}'.format(event_mid, response))
            #gateway_state.pending_responses[event_mid] = (client_addr, response)
        elif action == 'attach':
            _, attach_mid = attach_device(client, device_id)
            response = template.format(device_id, 'attach')
            print('Save mid {} for response {}'.format(attach_mid, response))
            #gateway_state.pending_responses[attach_mid] = (client_addr, response)
        elif action == 'detach':
            _, detach_mid = detatch_device(client, device_id)
            response = template.format(device_id, 'detach')
            print('Save mid {} for response {}'.format(detach_mid, response))
            #gateway_state.pending_responses[detach_mid] = (client_addr, response)
        elif action == "subscribe":
            print('subscribe config for {}'.format(device_id))
            subscribe_topic = '/devices/{}/config'.format(device_id)
            _, mid = client.subscribe(subscribe_topic, qos=1)
            response = template.format(device_id, 'subscribe')
            #gateway_state.subscriptions[subscribe_topic] = (client_addr)
            print('Save mid {} for response {}'.format(mid, response))
            #gateway_state.pending_subscribes[mid] = (client_addr, response)
        else:
            print('undefined action: {}'.format(action))

    print('Finished.')
# [END iot_mqtt_run]


if __name__ == '__main__':
    main()
