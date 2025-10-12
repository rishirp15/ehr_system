import sys
import xmlrpc.client
from itertools import cycle
from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import json

# --- Configuration ---
DATA_NODES = ['http://data-node-1:7001', 'http://data-node-2:7002', 'http://data-node-3:7003']
data_node_cycler = cycle(DATA_NODES)
REDIS_HOST, REDIS_PORT = 'redis', 6379
CACHE_TTL_SECONDS = 300

app = Flask(__name__)
CORS(app)

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping(); print(f"[*] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    print(f"[ERROR] Could not connect to Redis: {e}. Caching will be disabled."); redis_client = None

def send_rpc_to_data_node(rpc_message):
    target_node = next(data_node_cycler)
    try:
        with xmlrpc.client.ServerProxy(target_node, allow_none=True) as proxy:
            return proxy.dispatch_rpc(rpc_message['action'], rpc_message['data'])
    except Exception as e:
        error_msg = f"Data service at {target_node} is unavailable: {e}"
        print(f"[ERROR] {error_msg}")
        return {"status": "error", "code": 503, "error": error_msg}

# --- API Endpoints ---
@app.route('/register_patient', methods=['POST'])
def register_patient():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "register_patient", "data": payload})
    if redis_client and rpc_response.get('status') == 'success':
        redis_client.delete("all_patients")
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/register_doctor', methods=['POST'])
def register_doctor():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "register_doctor", "data": payload})
    if redis_client and rpc_response.get('status') == 'success':
        redis_client.delete("all_doctors")
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/login', methods=['POST'])
def login():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "login", "data": payload})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error') # Standardized to just use 'error'
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/record', methods=['POST'])
def add_record():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "add_record", "data": payload})
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    accessor_id = request.args.get('accessor_id', 'Unknown Clinician')
    payload = {"uuid": patient_uuid, "accessor_id": accessor_id}
    rpc_response = send_rpc_to_data_node({"action": "get_records_by_uuid", "data": payload})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error') or rpc_response.get('message') # Kept for backward compatibility just in case
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/doctors', methods=['GET'])
def get_all_doctors():
    rpc_response = send_rpc_to_data_node({"action": "get_all_doctors", "data": {}})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/appointments', methods=['POST'])
def book_appointment():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "book_appointment", "data": payload})
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/appointments/<string:user_id>', methods=['GET'])
def get_appointments(user_id):
    rpc_response = send_rpc_to_data_node({"action": "get_appointments", "data": {"user_id": user_id}})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code
    
@app.route('/patients', methods=['GET'])
def get_all_patients():
    response = send_rpc_to_data_node({"action": "get_all_patients", "data": {}})
    status_code = 200 if response.get("status") == "success" else 500
    return jsonify(response.get("data", [])), status_code

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>"); sys.exit(1)
    app.port = int(sys.argv[1])
    app.run(host='0.0.0.0', port=app.port, debug=False)