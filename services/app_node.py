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
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_client.ping()
    print(f"[*] AppNode-{sys.argv[1]}: Successfully connected to Redis.")
except Exception as e:
    print(f"[ERROR] AppNode-{sys.argv[1]}: Could not connect to Redis: {e}"); redis_client = None

def publish_log(level, message):
    if redis_client:
        log_entry = {'level': level, 'service': f'AppNode-{app.port}', 'message': message}
        redis_client.publish('system_logs', json.dumps(log_entry))

def send_rpc_to_data_node(rpc_message):
    """
    Sends an RPC message to a data node.
    Includes a retry loop to handle single-node failures.
    """
    # Retry up to the number of data nodes to handle failures.
    for i in range(len(DATA_NODES)):
        target_node = next(data_node_cycler)
        action = rpc_message['action']
        
        log_msg = f"Attempting RPC '{action}' to {target_node.split('/')[-1]}"
        print(f"[*] AppNode-{app.port}: {log_msg}")
        publish_log('debug', log_msg)

        try:
            with xmlrpc.client.ServerProxy(target_node, allow_none=True) as proxy:
                response = proxy.dispatch_rpc(action, rpc_message['data'])
            
            success_log = f"RPC '{action}' to {target_node.split('/')[-1]} successful."
            publish_log('info', success_log)
            return response

        except Exception as e:
            error_log = f"RPC to {target_node.split('/')[-1]} failed: {e}. Retrying with next node... ({i+1}/{len(DATA_NODES)})"
            print(f"[WARN] AppNode-{app.port}: {error_log}")
            publish_log('warn', error_log)
    
    # If the loop completes, all nodes have failed.
    final_error_msg = f"All data nodes are unavailable. Could not complete action '{rpc_message['action']}'."
    publish_log('error', final_error_msg)
    return {"status": "error", "code": 503, "error": final_error_msg}

# --- All API Endpoints remain the same ---
# (They will now automatically use the resilient send_rpc_to_data_node function)

@app.route('/register_patient', methods=['POST'])
def register_patient():
    publish_log('info', "Received /register_patient request.")
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "register_patient", "data": payload})
    if redis_client and rpc_response.get('status') == 'success':
        redis_client.delete("all_patients")
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/register_doctor', methods=['POST'])
def register_doctor():
    publish_log('info', "Received /register_doctor request.")
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "register_doctor", "data": payload})
    if redis_client and rpc_response.get('status') == 'success':
        redis_client.delete("all_doctors")
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/login', methods=['POST'])
def login():
    publish_log('info', "Received /login request.")
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "login", "data": payload})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/record', methods=['POST'])
def add_record():
    publish_log('info', "Received /record request.")
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "add_record", "data": payload})
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    accessor_id = request.args.get('accessor_id', 'Unknown Clinician')
    publish_log('info', f"Received /records request for {patient_uuid} by {accessor_id}")
    payload = {"uuid": patient_uuid, "accessor_id": accessor_id}
    rpc_response = send_rpc_to_data_node({"action": "get_records_by_uuid", "data": payload})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/doctors', methods=['GET'])
def get_all_doctors():
    publish_log('info', "Received /doctors request.")
    rpc_response = send_rpc_to_data_node({"action": "get_all_doctors", "data": {}})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code

@app.route('/appointments', methods=['POST'])
def book_appointment():
    publish_log('info', "Received /appointments POST request.")
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "book_appointment", "data": payload})
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/appointments/<string:user_id>', methods=['GET'])
def get_appointments(user_id):
    publish_log('info', f"Received /appointments GET request for {user_id}.")
    rpc_response = send_rpc_to_data_node({"action": "get_appointments", "data": {"user_id": user_id}})
    status_code = rpc_response.get('code', 500)
    error_message = rpc_response.get('error')
    return jsonify(rpc_response.get('data', {"error": error_message})), status_code
    
@app.route('/patients', methods=['GET'])
def get_all_patients():
    publish_log('info', "Received /patients request.")
    response = send_rpc_to_data_node({"action": "get_all_patients", "data": {}})
    status_code = 200 if response.get("status") == "success" else 500
    return jsonify(response.get("data", [])), status_code

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>"); sys.exit(1)
    app.port = int(sys.argv[1])
    app.run(host='0.0.0.0', port=app.port, debug=False, threaded=True)