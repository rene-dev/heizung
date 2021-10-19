import can
import ctypes
import sys
import json
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = '192.168.178.24'
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'heizung'

#sudo ip link set can0 up type can bitrate 10000 sample-point 0.875
bus = can.Bus(interface='socketcan',channel='can0',bitrate=10000)

#influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)
#databases = influxdb_client.get_list_database()
#if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
#    influxdb_client.create_database(INFLUXDB_DATABASE)
#influxdb_client.switch_database(INFLUXDB_DATABASE)


from paho.mqtt import client as mqtt_client


broker = '192.168.178.70'
port = 1883
topic = "test"
# generate client ID with pub prefix randomly
client_id = 'python-mqtt'
username = 'homeassistant'
password = 'quuw6eegai1HahSaeR1AR4AihuX5oohae1deeWohquoiPae6xeenaepheich2doh'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(rc)
            print(flags)
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count} BEMIS!!!!"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def publish2(client, topic, msg, retain=False):
    result = client.publish(topic, msg, retain=retain)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")


client = connect_mqtt()
#client.loop_start()

convert = {
    0x207: {
        "name": "Aussentemperatur",
        "convert": lambda d: ctypes.c_int16((d[0] << 8) + d[1]).value/100.0,
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "icon": "mdi:thermometer",
		"unique_id": "neandere7458057xl",
    },
   0x205: {
        "name": "WT_ist",
        "convert": lambda d: d[0]*0.5,
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "icon": "mdi:coolant-temperature",
		"unique_id": "arhfthfgewe333ht",
   },
   0x201 : {
        'name': 'vorlauf',
        'convert': lambda d: d[0]*0.5,
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "icon": "mdi:coolant-temperature",
		"unique_id": "arhfthfgewe3365433ht",
   },
}

state_topic = {}

for key,value in convert.items():
    state_topic[key] = {"name": value["name"],
                        "state_topic": "heizung/sensor/" + value["name"] + "/temperature",
                        "device_class": value["device_class"],
                        "unit_of_measurement": value["unit_of_measurement"],
                        #"value_template": value["value_template"],
                        "icon": value["icon"],
                        "unique_id": value["unique_id"],
                        "device": {"identifiers": "h312un5jaganztoll2", 
								   "name": "Heizung",
                                   "model": "PI",
                                   "manufacturer": "Junkers feat Bemistec Unlimited Enterprises"}
                        }
    publish2(client, f"homeassistant/sensor/{value['name']}/config", json.dumps(state_topic[key], ensure_ascii=False), retain=True)

#print (state_topic)

    # publish2(client, "homeassistant/sensor/heizung_temp3/config", '{"name": "Temp_3", "state_topic": "homeassistant/sensor/heizung/state", "device_class": "temperature", "unit_of_measurement": "°C", "value_template": "{{ value_json.temperature}}", "unique_id": "dztstzhdetsrthsss22", "device": {"identifiers": "834242st333hsv", "name": "devicename_temp_3", "sw_version": "0.00000000001", "model": "lars laptop", "manufacturer": "lars"}}', retain=True)
    # publish2(client, "homeassistant/sensor/heizung_temp4/config", '{"name": "Temp_4", "state_topic": "homeassistant/sensor/heizung/state", "device_class": "temperature", "unit_of_measurement": "°C", "value_template": "{{ value_json.temperature}}", "unique_id": "dztgrhsgfrthsss22", "device": {"identifiers": "834242st333hsv", "name": "Heizung", "model": "PI", "manufacturer": "Junkers feat Bemistec Unlimited Enterprises"}}', retain=True)


#for key,value in state_topic:
#    publish2(client, "homeassistant/sensor/heizung_temp1/config", json.dumps(config, ensure_ascii=False), retain=True)

#sys.exit(0)
print("start")
while True:
    message = bus.recv()
    #print("msg")
    if message.arbitration_id in convert.keys():
        key = convert[message.arbitration_id]['name']
        value = convert[message.arbitration_id]['convert'](message.data)
        #print ("{} {}".format(key,value))
        json_body = [
        {
            'measurement': "heizwerte",
            'tags': {
                'location': "vorne"
            },
            'fields': {
                key: value,
            }
        }
        ]
        #print(json_body)
        publish2(client, state_topic[message.arbitration_id]['state_topic'], value)
        #influxdb_client.write_points(json_body)
    else:
        pass
        #print("unknown message: {}".format(message))
