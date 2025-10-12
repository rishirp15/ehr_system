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

# --- Redis Connection ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    print(f"[*] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    print(f"[ERROR] Could not connect to Redis: {e}. Caching will be disabled.")
    redis_client = None

# --- RPC Communication ---
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
@app.route('/add_account', methods=['POST'])
def add_account():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "add_account", "data": payload})
    if redis_client and rpc_response.get('status') == 'success':
        redis_client.delete("all_patients")
    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/get_data/<string:username>', methods=['GET'])
def get_data(username):
    auth = request.authorization
    if not auth:
        return jsonify({"error": "Authentication required"}), 401
    
    cache_key = f"user_data:{auth.username}"
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200

    payload = {"username": auth.username, "password": auth.password}
    rpc_response = send_rpc_to_data_node({"action": "get_data", "data": payload})
    
    if redis_client and rpc_response.get('status') == 'success':
        data_to_cache = rpc_response.get('data')
        if data_to_cache:
            redis_client.set(cache_key, json.dumps(data_to_cache), ex=CACHE_TTL_SECONDS)
            
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/record', methods=['POST'])
def add_record():
    payload = request.get_json()
    rpc_response = send_rpc_to_data_node({"action": "add_record", "data": payload})
    
    if redis_client and rpc_response.get('status') == 'success':
        patient_uuid = payload.get('patient_uuid')
        if patient_uuid:
            redis_client.delete(f"records:{patient_uuid}")

    return jsonify(rpc_response), rpc_response.get('code', 500)

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    accessor_id = request.args.get('accessor_id', 'Unknown Clinician')
    payload = {"uuid": patient_uuid, "accessor_id": accessor_id}
    # We don't cache this read because it needs to generate a fresh audit log entry every time.
    rpc_response = send_rpc_to_data_node({"action": "get_records_by_uuid", "data": payload})
            
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/patients', methods=['GET'])
def get_all_patients():
    cache_key = "all_patients"
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
            
    response = send_rpc_to_data_node({"action": "get_all_patients", "data": {}})
    
    if redis_client and response.get("status") == "success":
        data_to_cache = response.get("data")
        if data_to_cache:
            redis_client.set(cache_key, json.dumps(data_to_cache), ex=CACHE_TTL_SECONDS)
            
    status_code = 200 if response.get("status") == "success" else 500
    return jsonify(response.get("data", [])), status_code

@app.route('/audit/<string:record_id>', methods=['GET'])
def get_audit_log(record_id):
    # Audit logs should not be cached to ensure real-time accuracy.
    rpc_response = send_rpc_to_data_node({
        "action": "get_audit_log",
        "data": {"record_id": record_id}
    })
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>")
        sys.exit(1)
    
    app.port = int(sys.argv[1])
    app.run(host='0.0.0.0', port=app.port, debug=False)