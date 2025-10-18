import sys
import xmlrpc.client
from itertools import cycle
from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import json
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
DATA_NODES = ['http://data-node-1:7001', 'http://data-node-2:7002', 'http://data-node-3:7003']
data_node_cycler = cycle(DATA_NODES)
REDIS_HOST, REDIS_PORT = 'redis', 6379

app = Flask(__name__)
CORS(app)

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_client.ping()
    print(f"[*] AppNode-{sys.argv[1]}: Successfully connected to Redis.")
except Exception as e:
    print(f"[ERROR] AppNode-{sys.argv[1]}: Could not connect to Redis: {e}"); redis_client = None

def publish_log(level, message):
    if redis_client:
        log_entry = {'level': level, 'service': f'AppNode-{app.port}', 'message': message}
        redis_client.publish('system_logs', json.dumps(log_entry))

# --- Custom Transport with a 15-second timeout ---
class TimeoutTransport(xmlrpc.client.Transport):
    timeout = 15.0
    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

def send_rpc_to_data_node(rpc_message):
    for i in range(len(DATA_NODES)):
        target_node = next(data_node_cycler)
        action = rpc_message['action']
        log_msg = f"Attempting RPC '{action}' to {target_node.split('/')[-1]}"
        publish_log('debug', log_msg)
        try:
            transport = TimeoutTransport()
            with xmlrpc.client.ServerProxy(target_node, transport=transport, allow_none=True) as proxy:
                response = proxy.dispatch_rpc(action, rpc_message['data'])
            publish_log('info', f"RPC '{action}' to {target_node.split('/')[-1]} successful.")
            return response
        except Exception as e:
            error_log = f"RPC to {target_node.split('/')[-1]} failed: {e}. Retrying... ({i+1}/{len(DATA_NODES)})"
            publish_log('warn', error_log)
            
    final_error_msg = f"All data nodes are unavailable. Could not complete action '{rpc_message['action']}'."
    publish_log('error', final_error_msg)
    return {"status": "error", "code": 503, "error": final_error_msg}

# --- Health Check Endpoint ---
@app.route('/health', methods=['GET'])
def get_health():
    active_nodes = 0
    total_nodes = len(DATA_NODES)
    def ping_node(node_url):
        try:
            class HealthCheckTransport(xmlrpc.client.Transport):
                timeout = 3.0
                def make_connection(self, host):
                    conn = super().make_connection(host)
                    conn.timeout = self.timeout
                    return conn
            transport = HealthCheckTransport()
            with xmlrpc.client.ServerProxy(node_url, transport=transport, allow_none=True) as proxy:
                if proxy.dispatch_rpc('ping', {}).get('status') == 'success':
                    return True
        except Exception: return False
        return False
    with ThreadPoolExecutor(max_workers=total_nodes) as executor:
        results = executor.map(ping_node, DATA_NODES)
        active_nodes = sum(1 for r in results if r)
    return jsonify({"active_nodes": active_nodes, "total_nodes": total_nodes}), 200

# --- API Endpoints ---
@app.route('/register_patient', methods=['POST'])
def register_patient():
    publish_log('info', "Received /register_patient request.")
    response = send_rpc_to_data_node({"action": "register_patient", "data": request.get_json()})
    return jsonify(response), response.get('code', 500)

@app.route('/register_doctor', methods=['POST'])
def register_doctor():
    publish_log('info', "Received /register_doctor request.")
    response = send_rpc_to_data_node({"action": "register_doctor", "data": request.get_json()})
    return jsonify(response), response.get('code', 500)

@app.route('/login', methods=['POST'])
def login():
    publish_log('info', "Received /login request.")
    response = send_rpc_to_data_node({"action": "login", "data": request.get_json()})
    return jsonify(response.get('data', {"error": response.get('error')})), response.get('code', 500)

@app.route('/record', methods=['POST'])
def add_record():
    publish_log('info', "Received /record POST request.")
    response = send_rpc_to_data_node({"action": "add_record", "data": request.get_json()})
    return jsonify(response), response.get('code', 500)

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    publish_log('info', f"Received /records GET request for {patient_uuid}.")
    response = send_rpc_to_data_node({"action": "get_records_by_uuid", "data": {"uuid": patient_uuid}})
    return jsonify(response.get('data', {"error": response.get('error')})), response.get('code', 500)

@app.route('/doctors', methods=['GET'])
def get_all_doctors():
    publish_log('info', "Received /doctors GET request.")
    response = send_rpc_to_data_node({"action": "get_all_doctors", "data": {}})
    return jsonify(response.get('data', [])), response.get('code', 200)
    
@app.route('/appointments', methods=['POST'])
def book_appointment():
    publish_log('info', "Received /appointments POST request.")
    response = send_rpc_to_data_node({"action": "book_appointment", "data": request.get_json()})
    return jsonify(response), response.get('code', 500)

@app.route('/appointments/<string:user_id>', methods=['GET'])
def get_appointments(user_id):
    publish_log('info', f"Received /appointments GET request for {user_id}.")
    response = send_rpc_to_data_node({"action": "get_appointments", "data": {"user_id": user_id}})
    return jsonify(response.get('data', [])), response.get('code', 200)

@app.route('/patients', methods=['GET'])
def get_all_patients():
    publish_log('info', "Received /patients GET request for admin.")
    response = send_rpc_to_data_node({"action": "get_all_patients", "data": {}})
    return jsonify(response.get("data", {})), response.get('code', 200)

if __name__ == '__main__':
    if len(sys.argv) != 2: print("Usage: python app_node.py <port>"); sys.exit(1)
    app.port = int(sys.argv[1]); app.run(host='0.0.0.0', port=app.port, debug=False, threaded=True)