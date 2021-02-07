import can
import ctypes
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = '192.168.178.24'
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'heizung'

#sudo ip link set can0 up type can bitrate 10000 sample-point 0.875
bus = can.Bus(interface='socketcan',channel='can0',bitrate=10000)

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)
databases = influxdb_client.get_list_database()
if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
    influxdb_client.create_database(INFLUXDB_DATABASE)
influxdb_client.switch_database(INFLUXDB_DATABASE)


convert = {
#0x0f9 : ["UNK_0x0f9", lambda d: d[0]],
0x200 : ["vorlauf max1",lambda d: d[0]*0.5],
0x201 : ["vorlauf",lambda d: d[0]*0.5],
0x204 : ["WT_max",lambda d: d[0]*0.5],
0x205 : ["WT_ist",lambda d: d[0]*0.5],
0x206 : ["Störung",lambda d: d[0]],
0x207 : ["außentemperatur",lambda d: ctypes.c_int16((d[0] << 8) + d[1]).value/100.0],
0x208 : ["UNK_0x208",lambda d: d[0]],
0x209 : ["flamme",lambda d: d[0]],
0x20a : ["pumpe heizung",lambda d: d[0]],
0x20b : ["speicherladung",lambda d: d[0]],
0x20c : ["sommer/winter",lambda d: d[0]],
0x20d : ["UNK_0x20d",lambda d: d[0]], #Status of the heating??? Goes from 0x18 to 0x19 when the mixer inside the heating is feeding the hot water tank
0x250 : ["heizbetrieb",lambda d: d[0]],
0x252 : ["vorlauf soll bedienteil",lambda d: d[0]*0.5],
0x253 : ["wasser soll bedienteil",lambda d: d[0]*0.5],
0x254 : ["UNK_0x254",lambda d: d[0]],
0x255 : ["wasser soll bedienteil2",lambda d: d[0]*0.5],
0x256 : ["UNK_0x256",lambda d: (d[0] << 24) + (d[1] << 16) + (d[2] << 8) + d[3]],
0x258 : ["UNK_0x258",lambda d: d[0]],
0x25a : ["UNK_0x25a", lambda d: ((d[0] << 8) + d[1])],
0x404 : ["MT_pumpe", lambda d: d[1]],
0x405 : ["MT_soll", lambda d: d[1]*0.5],
0x440 : ["MT_ist", lambda d: d[1]*0.5],
0x441 : ["MT_max", lambda d: d[1]*0.5],
0x4f9 : ["UNK_0x4f9", lambda d: d[0]],
0x4fa : ["UNK_0x4fa", lambda d: ((d[0] << 8) + d[1])],
#0x256 : ["uhrzeit",lambda d: d],
#0x440 : ["mischer",lambda d: d],
#0x441 : ["mischer",lambda d: d],
#0x4fa : ["mischer",lambda d: d],
#0x4fc : ["mischer",lambda d: d[0]],
}

while True:
    message = bus.recv()
    if message.arbitration_id in convert.keys():
        key = convert[message.arbitration_id][0]
        value = convert[message.arbitration_id][1](message.data)
        print ("{} {}".format(key,value))
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
        print(json_body)
        influxdb_client.write_points(json_body)
    else:
        pass
        print("unknown message: {}".format(message))
