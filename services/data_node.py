import sys
import sqlite3
import threading
import uuid
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
import redis
import json

from berkeley_clock import BerkeleyClock
from maekawa_mutex import MaekawaMutex

# --- Configuration & Globals (no changes) ---
NODE_ID, NODE_PORT, DB_NAME_GLOBAL = 0, 0, ""
ALL_DATA_NODES = { 1: {'host': 'data-node-1', 'rpc': 7001}, 2: {'host': 'data-node-2', 'rpc': 7002}, 3: {'host': 'data-node-3', 'rpc': 7003} }
QUORUM_SETS = {1: [2, 3], 2: [1, 3], 3: [1, 2]}
QUORUM_W, QUORUM_R = 2, 2
clock_service, mutex_service, rpc_proxies, redis_client = None, None, {}, None

# --- Utility Functions (with logging) ---
def publish_log(level, message):
    if redis_client:
        log_entry = {'level': level, 'service': f'DataNode-{NODE_ID}', 'message': message}
        redis_client.publish('system_logs', json.dumps(log_entry))

def init_db(db_name):
    conn = sqlite3.connect(f"/data/{db_name}", check_same_thread=False); cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS appointments"); cursor.execute("DROP TABLE IF EXISTS records"); cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute('CREATE TABLE users (uuid TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, age INTEGER, role TEXT NOT NULL, specialization TEXT)')
    cursor.execute('CREATE TABLE records (record_id TEXT PRIMARY KEY, patient_uuid TEXT NOT NULL, doctor_name TEXT, description TEXT, prescription TEXT, resources_used TEXT, timestamp REAL, FOREIGN KEY (patient_uuid) REFERENCES users (uuid))')
    cursor.execute('CREATE TABLE appointments (appointment_id TEXT PRIMARY KEY, patient_uuid TEXT NOT NULL, doctor_uuid TEXT NOT NULL, appointment_date TEXT NOT NULL, status TEXT NOT NULL, FOREIGN KEY (patient_uuid) REFERENCES users (uuid), FOREIGN KEY (doctor_uuid) REFERENCES users (uuid))')
    conn.commit(); conn.close()
    publish_log('info', f"Database '{db_name}' initialized.")

def connect_to_peers():
    for peer_id, peer_info in ALL_DATA_NODES.items():
        if peer_id != NODE_ID:
            proxy_url = f"http://{peer_info['host']}:{peer_info['rpc']}/"
            rpc_proxies[peer_id] = xmlrpc.client.ServerProxy(proxy_url, allow_none=True)
    publish_log('info', f"Connected to peers: {list(rpc_proxies.keys())}")

def send_rpc_to_peer(peer_id, action, data):
    try:
        if peer_id in rpc_proxies: return rpc_proxies[peer_id].dispatch_rpc(action, data)
    except Exception as e: publish_log('error', f"Could not call '{action}' on peer {peer_id}: {e}")
    return None

def perform_quorum_write(cursor, record_type, record_data):
    handle_replicate_write(cursor, {"record_type": record_type, "record_data": record_data})
    peer_ids, ack_count = [i for i in ALL_DATA_NODES if i != NODE_ID], 1
    def replicate(peer_id):
        publish_log('debug', f"Replicating '{record_type}' write to peer {peer_id}")
        response = send_rpc_to_peer(peer_id, "replicate_write", {"record_type": record_type, "record_data": record_data})
        if response and response.get("status") == "success": return True
        return False
    with ThreadPoolExecutor(max_workers=len(peer_ids)) as executor:
        results = executor.map(replicate, peer_ids)
        ack_count += sum(1 for r in results if r)
    publish_log('info', f"Quorum write completed with {ack_count}/{len(ALL_DATA_NODES)} ACKs.")
    return ack_count >= QUORUM_W, ack_count

# --- Mutual Exclusion Logic (with detailed logging) ---
def acquire_lock():
    if mutex_service.first_acquire_attempt:
        with mutex_service.lock:
            if mutex_service.first_acquire_attempt:
                publish_log('debug', "First lock acquire, waiting for network..."); time.sleep(8); mutex_service.first_acquire_attempt = False
    publish_log('info', "Attempting to ACQUIRE distributed lock (Maekawa)...")
    mutex_service.state, mutex_service.request_ts = 'WANTED', time.time()
    mutex_service.outstanding_replies = set(mutex_service.quorum_ids)
    request_data = {'requester_id': NODE_ID, 'ts': mutex_service.request_ts}
    def send_request(peer_id):
        publish_log('debug', f"Sending lock REQUEST to peer {peer_id}")
        response = send_rpc_to_peer(peer_id, "request_lock", request_data)
        if response and response == 'REPLY':
            publish_log('debug', f"Received lock REPLY from peer {peer_id}")
            mutex_service.receive_reply(peer_id)
    with ThreadPoolExecutor() as executor: executor.map(send_request, mutex_service.quorum_ids)
    while mutex_service.state != 'HELD': time.sleep(0.1)
    publish_log('info', "Lock ACQUIRED!")

def release_lock():
    publish_log('info', "RELEASING distributed lock...")
    mutex_service.state, mutex_service.request_ts = 'RELEASED', None
    def send_release(peer_id):
        publish_log('debug', f"Sending lock RELEASE to peer {peer_id}")
        send_rpc_to_peer(peer_id, "release_lock", {})
    with ThreadPoolExecutor() as executor: executor.map(send_release, mutex_service.quorum_ids)

# --- All Action Handlers remain the same ---
def handle_register_patient(cursor, data):
    acquire_lock()
    try:
        user_uuid = str(uuid.uuid4()).split('-')[0]
        user_data = {'uuid': user_uuid, 'username': data['username'], 'password': data['password'], 'first_name': data['first_name'], 'last_name': data['last_name'], 'age': data.get('age'), 'role': 'patient', 'specialization': None}
        success, acks = perform_quorum_write(cursor, "user", user_data)
        if success: return {"status": "success", "code": 201, "message": "Patient account created"}
        else: return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally: release_lock()

def handle_register_doctor(cursor, data):
    acquire_lock()
    try:
        user_uuid = str(uuid.uuid4()).split('-')[0]
        user_data = {'uuid': user_uuid, 'username': data['username'], 'password': data['password'], 'first_name': data['first_name'], 'last_name': data['last_name'], 'age': data.get('age'), 'role': 'doctor', 'specialization': data['specialization']}
        success, acks = perform_quorum_write(cursor, "user", user_data)
        if success: return {"status": "success", "code": 201, "message": "Doctor account created"}
        else: return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally: release_lock()

def handle_login(cursor, data):
    acquire_lock()
    try:
        cursor.execute("SELECT * FROM users WHERE username = ?", (data.get('username'),))
        user_row = cursor.fetchone()
        if user_row is None: return {"status": "error", "code": 404, "error": "User not found"}
        user_columns = [desc[0] for desc in cursor.description]
        user_data = dict(zip(user_columns, user_row))
        if user_data.get('password') != data.get('password'): return {"status": "error", "code": 401, "error": "Authentication failed"}
        del user_data['password']
        if user_data['role'] == 'patient':
            cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (user_data['uuid'],))
            record_rows = cursor.fetchall()
            user_data['records'] = [dict(zip([d[0] for d in cursor.description], r)) for r in record_rows] if record_rows else []
        return {"status": "success", "code": 200, "data": user_data}
    finally: release_lock()

def handle_add_record(cursor, data):
    acquire_lock()
    try:
        record_id = str(uuid.uuid4()).split('-')[0]
        new_record = {"record_id": record_id, "patient_uuid": data['patient_uuid'], "doctor_name": data['doctor_name'], "description": data['description'], "prescription": data['prescription'], "resources_used": data.get('resources_used'), "timestamp": clock_service.get_time()}
        success, acks = perform_quorum_write(cursor, "record", new_record)
        if success: return {"status": "success", "code": 201, "message": "Record added"}
        else: return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally: release_lock()

def handle_get_records_by_uuid(cursor, data):
    cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (data.get('uuid'),))
    rows = cursor.fetchall()
    if not rows: return {"status": "success", "code": 200, "data": []}
    columns = [desc[0] for desc in cursor.description]
    return {"status": "success", "code": 200, "data": [dict(zip(columns, row)) for row in rows]}

def handle_get_all_doctors(cursor, data):
    cursor.execute("SELECT uuid, first_name, last_name, age, specialization FROM users WHERE role = 'doctor'")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return {"status": "success", "code": 200, "data": [dict(zip(columns, row)) for row in rows]}

def handle_book_appointment(cursor, data):
    acquire_lock()
    try:
        app_data = {"appointment_id": str(uuid.uuid4()).split('-')[0], "patient_uuid": data['patient_uuid'], "doctor_uuid": data['doctor_uuid'], "appointment_date": data['appointment_date'], "status": "BOOKED"}
        success, acks = perform_quorum_write(cursor, "appointment", app_data)
        if success: return {"status": "success", "code": 201, "message": "Appointment booked"}
        else: return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally: release_lock()

def handle_get_appointments(cursor, data):
    user_id = data.get('user_id')
    cursor.execute("SELECT role FROM users WHERE uuid = ?", (user_id,)); role_row = cursor.fetchone()
    if not role_row: return {"status": "error", "code": 404, "error": "User not found"}
    role = role_row[0]
    if role == 'patient': query = "SELECT a.appointment_date, a.status, u.first_name, u.last_name, u.specialization FROM appointments a JOIN users u ON a.doctor_uuid = u.uuid WHERE a.patient_uuid = ? ORDER BY a.appointment_date DESC"
    else: query = "SELECT a.appointment_date, a.status, u.first_name, u.last_name FROM appointments a JOIN users u ON a.patient_uuid = u.uuid WHERE a.doctor_uuid = ? ORDER BY a.appointment_date DESC"
    cursor.execute(query, (user_id,)); rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return {"status": "success", "code": 200, "data": [dict(zip(columns, row)) for row in rows]}
    
def handle_get_all_patients(cursor, data):
    cursor.execute("SELECT uuid, first_name, last_name, age FROM users WHERE role = 'patient'")
    rows = cursor.fetchall()
    return {"status": "success", "data": {row[0]: {"patient_id": row[0], "name": f"{row[1]} {row[2]}", "age": row[3]} for row in rows}}

def handle_replicate_write(cursor, data):
    record_type, record_data = data.get("record_type"), data.get("record_data")
    if record_type == "user": cursor.execute("INSERT OR REPLACE INTO users (uuid, username, password, first_name, last_name, age, role, specialization) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (record_data['uuid'], record_data['username'], record_data['password'], record_data['first_name'], record_data['last_name'], record_data.get('age'), record_data['role'], record_data.get('specialization')))
    elif record_type == "record": cursor.execute("INSERT OR REPLACE INTO records (record_id, patient_uuid, doctor_name, description, prescription, resources_used, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", (record_data['record_id'], record_data['patient_uuid'], record_data['doctor_name'], record_data['description'], record_data['prescription'], record_data.get('resources_used'), record_data['timestamp']))
    elif record_type == "appointment": cursor.execute("INSERT OR REPLACE INTO appointments (appointment_id, patient_uuid, doctor_uuid, appointment_date, status) VALUES (?, ?, ?, ?, ?)", (record_data['appointment_id'], record_data['patient_uuid'], record_data['doctor_uuid'], record_data['appointment_date'], record_data['status']))
    else: return {"status": "error", "message": "Unknown record type"}
    return {"status": "success"}

ACTION_MAP = {"register_patient": handle_register_patient, "register_doctor": handle_register_doctor, "login": handle_login, "get_all_doctors": handle_get_all_doctors, "book_appointment": handle_book_appointment, "get_appointments": handle_get_appointments, "get_all_patients": handle_get_all_patients, "add_record": handle_add_record, "get_records_by_uuid": handle_get_records_by_uuid}

def dispatch_rpc(action, data):
    conn = sqlite3.connect(f"/data/{DB_NAME_GLOBAL}", check_same_thread=False)
    cursor, response = conn.cursor(), {}
    try:
        if action in ACTION_MAP: response = ACTION_MAP[action](cursor, data)
        elif action == "replicate_write": response = handle_replicate_write(cursor, data)
        elif action == "get_time_for_sync": return clock_service.get_time_for_sync()
        elif action == "adjust_time": clock_service.adjust_time(data['adjustment']); return "OK"
        elif action == "request_lock":
            publish_log('debug', f"Received lock REQUEST from peer {data['requester_id']}")
            response = mutex_service.handle_request_rpc(data['requester_id'], data['ts'])
            publish_log('debug', f"Sending lock {response} to peer {data['requester_id']}")
            return response
        elif action == "release_lock":
            publish_log('debug', "Received lock RELEASE from a peer.")
            return mutex_service.handle_release_rpc()
        elif action == "receive_grant":
            publish_log('debug', f"Received a delegated GRANT from peer {data['sender_id']}")
            mutex_service.receive_reply(data['sender_id']); return "OK"
        elif action == "ping": return {"status": "success"}
        else: response = {"status": "error", "error": "Unknown action"}
        conn.commit()
    except Exception as e:
        conn.rollback(); response = {"status": "error", "error": f"Internal error: {e}"}; print(f"[ERROR] on '{action}': {e}")
    finally: conn.close()
    return response

def master_sync_loop():
    time.sleep(10); publish_log('info', "MASTER clock starting sync loop.")
    while True:
        try:
            publish_log('debug', "Starting new clock sync round.")
            master_time, offsets, peer_offsets = time.time(), [], {}
            for peer_id, proxy in rpc_proxies.items():
                try:
                    peer_time = proxy.dispatch_rpc("get_time_for_sync", {}); offset = peer_time - master_time
                    offsets.append(offset); peer_offsets[peer_id] = offset
                except Exception as e: publish_log('warn', f"Clock Master: FAILED to get time from Peer {peer_id}")
            if offsets:
                offsets.append(0.0); average_offset = mean(offsets)
                publish_log('info', f"Clock sync round complete. Average offset: {average_offset:.4f}s")
                for peer_id, proxy in rpc_proxies.items():
                    if peer_id in peer_offsets: send_rpc_to_peer(peer_id, "adjust_time", {'adjustment': average_offset - peer_offsets[peer_id]})
                clock_service.adjust_time(average_offset)
        except Exception as e: publish_log('error', f"Exception in master_sync_loop: {e}")
        time.sleep(20)

def main(port, db_name):
    global NODE_PORT, NODE_ID, DB_NAME_GLOBAL, clock_service, mutex_service, redis_client
    NODE_PORT, DB_NAME_GLOBAL = port, db_name
    for i, p in ALL_DATA_NODES.items():
        if p['rpc'] == NODE_PORT: NODE_ID = i; break
    
    try:
        redis_client = redis.Redis(host='redis', port=6379, db=0)
        redis_client.ping()
        print(f"[DataNode-{NODE_ID}] Connected to Redis for logging.")
    except Exception as e:
        print(f"[DataNode-{NODE_ID}] ERROR: Could not connect to Redis: {e}")

    init_db(db_name)
    is_master_clock = (NODE_ID == 1)
    clock_service, mutex_service = BerkeleyClock(NODE_ID, is_master=is_master_clock), MaekawaMutex(NODE_ID, QUORUM_SETS[NODE_ID])
    time.sleep(5)
    connect_to_peers()
    if is_master_clock: threading.Thread(target=master_sync_loop, daemon=True).start()
    with SimpleXMLRPCServer(('0.0.0.0', port), allow_none=True) as server:
        server.register_introspection_functions(); server.register_function(dispatch_rpc, 'dispatch_rpc')
        print(f"Data Node {NODE_ID} RPC server listening on {port}")
        server.serve_forever()

if __name__ == '__main__':
    if len(sys.argv) != 3: print("Usage: python data_node.py <port> <db_name>"); sys.exit(1)
    port_arg, db_name_arg = int(sys.argv[1]), sys.argv[2]
    main(port_arg, db_name_arg)