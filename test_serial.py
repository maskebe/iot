from time import sleep
import json

# Load config
with open("config.json", "r") as fd:
    config = json.loads(fd.read())

serial_port = config["serial_port"]


def read_serial_data(ser):
  """Read Arduino sensors from serial interface"""
  try:
      response = serial_receive(ser)
      response = response.rstrip().decode()
      print(f"Received from Arduino (raw data): {response}")
  except IOError:
    print('I/O Error')
    return None
  return response

def serial_receive(ser):
  """Write string to serial connection and return any response."""
  while True:
    try:
      sleep(0.01)
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
    sleep(1)
    ser.flushInput()
    ser.setDTR(True)
  ser = serial.Serial(serial_port, 9600, timeout=0.1)
  return ser

def get_devive_id(id):
    if (id == "1"):
        return f"canal-cleaner"
    elif (id == "2"):
        return f"weather-station"
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
            if (id == "1"):
                obstruction = float(sensors_data.split(",")[1])
                data = {"device_id": device, "obstruction": obstruction}
                return {"action": "event", "device": device, "data": data}
            elif (id == "2"):
                humidity = float(sensors_data.split(",")[1])
                temperature = float(sensors_data.split(",")[2])
                water_level = int(sensors_data.split(",")[3])
                data = {"device_id": device,"humidity": humidity, "temperature": temperature, "water_level": water_level}
                return {"action": "event", "device": device, "data": data}
            else:
                print("Unrecongnized sensor node ID")
                return None
    else:
        print('Error getting Arduino sensor values over serial')
        return None


def main():
  ser = init_serial(serial_port)
  while True:
    data = read_serial_data(ser)
    sensors_data = parse_sensors_data(data)
    print(f"Decoded sensor data : {sensors_data}")
    print("----")
    sleep(1)


if __name__ == '__main__':
  main()
