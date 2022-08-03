import json
import logging
from datetime import datetime

from flask import Flask, request
from psycopgwrapper import PG
from waitress import serve

app = Flask(__name__)


@app.route('/sensordata', methods=['GET', 'POST'])
def add_sensor_data():
    pg = PG.get_default_postgres()

    # Get request for debugging
    if request.method == 'GET':
        logging.warning("Debug: GET request")
        packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltage': 3.91,
        'S2_R1': 111.1, 'S2_R2': 790123, 'S2_Voltage': 3.64,
        'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltage': 2.39,
        'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltage': 2.57,
        'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltage': 3.89,
        'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltage': 4.02,
        'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltage': 3.95,
        'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltage': 4.33,
        'CFG': 0, 'T': 33.5, 'TH': 35.5, 'H': 33.9, 'RH': 32.7,
        'P': 988, 'G': 3172.33, 'IAQ': 56.4, 'CO2': 631.96, 'VOC': 0.82, 'IAC_comp': 3,
        'timestamp': 1659106708162, 'node_id': 'appa1-debug'}
    else:
        packet = request.json

    node_id = pg.fetchone("select id from node where node_name = %s", [packet["node_id"]])
    sensor_ts = datetime.fromtimestamp(packet["timestamp"] / 1000.0)

    # TODO: add attrs that do not "key.startswith("S") and key.endswith(("_R1", "_R2", "_Voltage"))"
    attrs_dict = {
        k: v
        for k, v in packet.items()
        if not k.startswith("S")
        and not k.endswith(("_R1", "_R2", "_Voltage"))
        and k not in ["timestamp", "node_id"]
    }    
    attrs = json.dumps(attrs_dict)
    packet_id = pg.fetchone("insert into packet(node_id, sensor_ts, attrs) values (%s, %s, %s) returning id", [node_id, sensor_ts, attrs])
    sensor_ids = pg.fetchall("select id, name from sensor where node_id = %s", [node_id])
    
    for sensor_id, sensor_name in sensor_ids:
        # e.g. sensor_id = 481
        # e.g. sensor_name: S3_ID
        # e.g. sensor_val: value 1 to 8
        sensor_val = sensor_name[1]
        pg.upsert("packet_data", {
            "packet_id": packet_id,
            "sensor_id": sensor_id,
            "r1": packet[f"S{sensor_val}_R1"],
            "r2": packet[f"S{sensor_val}_R2"],
            "volt": packet[f"S{sensor_val}_Voltage"],
        }, ["packet_id", "sensor_id"])

    pg.close()
    logging.warning(f"{request.method} request for /sensordata {packet['node_id']}")
    return packet


@app.route('/nodeinfo', methods=['GET', 'POST'])
def add_node_info():
    pg = PG.get_default_postgres()

    # Get request for debugging
    if request.method == 'GET':
        logging.warning("Debug: GET request")
        content = {'node_id': 'appa1-debug', 'lat': 46.062934, 'lon': 11.126217,
         'description': 'Appa 1 - S. Chiara',
         'S1_ID': 'SnO2_c_MH20_L2', 'S2_ID': 'LaFeO3_1-2021_MH17_L2',
         'S3_ID': 'WO3_1-2021_MH17_L5', 'S4_ID': 'WO3_1-2021_MH17_L5',
         'S5_ID': 'ZnOR_2018_MH17_L2', 'S6_ID': 'ZnOR_2018_MH17_L2',
         'S7_ID': 'SmFeO3_1-2021_MH20_L2', 'S8_ID': 'STN_2018_MH20_L2'}
    else:
        content = request.json

    pg.upsert("node", {
        "node_name": content["node_id"],
        "lat": content["lat"],
        "lon": content["lon"],
        "description": content["description"] # TODO: check if it's correct, returns always SnO2_c_MH20_L2
    }, ["node_name"])
    node_id = pg.fetchone("select id from node where node_name=%s", [content["node_id"]])
    for key, val in content.items():
        if key.startswith("S") and key.endswith("_ID"):
            pg.upsert("sensor", {
                "node_id": node_id,
                "name": key,
                "description": val,
            }, ["node_id", "name"])
    pg.close()
    logging.warning(f"{request.method} request for /nodeinfo {content['node_id']}")
    return content


if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=5000) 