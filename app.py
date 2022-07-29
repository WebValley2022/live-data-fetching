from flask import Flask, request
import logging
from psycopgwrapper import PG
app = Flask(__name__)


@app.route('/sensordata', methods=['GET', 'POST'])
def add_sensor_data():
    if request.method == 'GET':
        packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltage': 3.91,
        'S2_R1': 1`11.1, 'S2_R2': 790123, 'S2_Voltage': 3.64,
        'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltage': 2.39,
        'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltage': 2.57,
        'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltage': 3.89,
        'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltage': 4.02,
        'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltage': 3.95,
        'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltage': 4.33,
        'CFG': 0, 'T': 33.5, 'TH': 35.5, 'H': 33.9, 'RH': 32.7,
        'P': 988, 'G': 3172.33, 'IAQ': 56.4, 'CO2': 631.96, 'VOC': 0.82, 'IAC_comp': 3,
        'timestamp`': 1659106708162, 'node_id': 'appa1-gevo5'}
        
        # TODO: attrs is a 

        # node_id = pg.fetchone("select id from node where node_name = %s", [packet["node_id"]])
        # sensor_ids = pd.fetchall("select id, name from sensor where node_id = %s", [node_id])
        # packet_id = pg.fetchone("insert into packet(node_id, sensor_ts, attrs) values (%s, %s, %s) returning id", [])

        # for key, value in S_.values():
        #     # Name: e.g. S1_ID ??
        return "Hello sensor!"

    content = request.json
    logging.warning(content)
    return content


@app.route('/nodeinfo', methods=['GET', 'POST'])
def add_node_info():
    pg = PG.get_default_postgres()

    if request.method == 'GET':
        mariolino1 = {'node_id': 'appa1-mariolino', 'lat': 46.062934, 'lon': 11.126217,
         'description': 'Appa 1 - S. Chiara',
         'S1_ID': 'SnO2_c_MH20_L2', 'S2_ID': 'LaFeO3_1-2021_MH17_L2',
         'S3_ID': 'WO3_1-2021_MH17_L5', 'S4_ID': 'WO3_1-2021_MH17_L5',
         'S5_ID': 'ZnOR_2018_MH17_L2', 'S6_ID': 'ZnOR_2018_MH17_L2',
         'S7_ID': 'SmFeO3_1-2021_MH20_L2', 'S8_ID': 'STN_2018_MH20_L2'}
        pg.upsert("node", {
            "node_name": mariolino1["node_id"],
            "lat": mariolino1["lat"],
            "lon": mariolino1["lon"],
            "description": mariolino1["description"]
        }, ["node_name"])
        node_id = pg.fetchone("select id from node where node_name=%s", [mariolino1["node_id"]])
        for key, val in mariolino1.items():
            if key.startswith("S") and key.endswith("_ID"):
                pg.upsert("sensor", {
                    "node_id": node_id,
                    "name": key,
                    "description": val,
                }, ["node_id", "name"])
        pg.close()
        return "Hello node!"
    content = request.json
    logging.warning(content)
    return content


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
