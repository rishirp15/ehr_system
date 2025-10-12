import sys
import sqlite3
import threading
import uuid
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor
from statistics import mean

# Import local modules
from berkeley_clock import BerkeleyClock
from maekawa_mutex import MaekawaMutex

# --- Configuration ---
NODE_ID = 0
NODE_PORT = 0
DB_NAME_GLOBAL = ""

ALL_DATA_NODES = {
    1: {'host': 'data-node-1', 'rpc': 7001},
    2: {'host': 'data-node-2', 'rpc': 7002},
    3: {'host': 'data-node-3', 'rpc': 7003},
}

QUORUM_SETS = {
    1: [2, 3],
    2: [1, 3],
    3: [1, 2],
}

QUORUM_W = 2 # Write Quorum
QUORUM_R = 2 # Read Quorum

# --- Global Service Instances ---
clock_service = None
mutex_service = None
rpc_proxies = {} # Stores client connections to peers

def init_db(db_name):
    """Initializes the SQLite database with the new audit_logs table."""
    conn = sqlite3.connect(f"/data/{db_name}", check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            uuid TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, first_name TEXT NOT NULL,
            last_name TEXT NOT NULL, dob TEXT NOT NULL, password TEXT NOT NULL
        )''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            record_id TEXT PRIMARY KEY, patient_uuid TEXT NOT NULL, doctor_name TEXT,
            description TEXT, resources_used TEXT, prescription TEXT,
            timestamp REAL, FOREIGN KEY (patient_uuid) REFERENCES users (uuid)
        )''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            log_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, patient_uuid TEXT NOT NULL,
            accessor_id TEXT NOT NULL, action TEXT NOT NULL, timestamp REAL,
            FOREIGN KEY (patient_uuid) REFERENCES users (uuid)
        )''')
    conn.commit()
    conn.close()
    print(f"Database '/data/{db_name}' initialized with audit log table.")

def connect_to_peers():
    """Establishes XML-RPC client proxies to all other nodes."""
    for peer_id, peer_info in ALL_DATA_NODES.items():
        if peer_id != NODE_ID:
            proxy_url = f"http://{peer_info['host']}:{peer_info['rpc']}/"
            rpc_proxies[peer_id] = xmlrpc.client.ServerProxy(proxy_url, allow_none=True)
    print(f"[Node-{NODE_ID}] Connected to peers: {list(rpc_proxies.keys())}")

# --- RPC & Replication Logic ---
def send_rpc_to_peer(peer_id, action, data):
    """Sends a single RPC call to a specified peer."""
    try:
        if peer_id in rpc_proxies:
            return rpc_proxies[peer_id].dispatch_rpc(action, data)
    except Exception as e:
        print(f"[ERROR] Node-{NODE_ID}: Could not call '{action}' on peer {peer_id}: {e}")
    return None

def perform_quorum_write(cursor, record_type, record_data):
    """Writes to self and replicates to peers until write quorum is met."""
    handle_replicate_write(cursor, {"record_type": record_type, "record_data": record_data})
    
    peer_ids = [i for i in ALL_DATA_NODES if i != NODE_ID]
    ack_count = 1
    
    def replicate(peer_id):
        response = send_rpc_to_peer(peer_id, "replicate_write", {"record_type": record_type, "record_data": record_data})
        if response and response.get("status") == "success":
            return True
        return False

    with ThreadPoolExecutor(max_workers=len(peer_ids)) as executor:
        results = executor.map(replicate, peer_ids)
        ack_count += sum(1 for r in results if r)

    return ack_count >= QUORUM_W, ack_count

# --- Mutual Exclusion Logic (using RPC) ---
def acquire_lock():
    """Acquires a distributed lock using Maekawa's algorithm over RPC."""
    print(f"\n[Mutex-{NODE_ID}] Attempting to ACQUIRE lock...")
    mutex_service.state = 'WANTED'
    mutex_service.request_ts = time.time()
    mutex_service.outstanding_replies = set(mutex_service.quorum_ids)

    request_data = {'requester_id': NODE_ID, 'ts': mutex_service.request_ts}
    
    def send_request(peer_id):
        response = send_rpc_to_peer(peer_id, "request_lock", request_data)
        if response and response == 'REPLY':
            mutex_service.receive_reply(peer_id)
    
    with ThreadPoolExecutor() as executor:
        executor.map(send_request, mutex_service.quorum_ids)

    while mutex_service.state != 'HELD':
        time.sleep(0.1)

def release_lock():
    """Releases the distributed lock."""
    print(f"🛑 [Mutex-{NODE_ID}] RELEASING lock...")
    mutex_service.state = 'RELEASED'
    mutex_service.request_ts = None
    
    def send_release(peer_id):
        response = send_rpc_to_peer(peer_id, "release_lock", {})
        if response and response.get('status') == 'GRANT_DEFERRED':
            grant_to_id = response['to']
            print(f"[Mutex-{NODE_ID}] Peer {peer_id} delegated grant to {grant_to_id}")
            send_rpc_to_peer(grant_to_id, "receive_grant", {'sender_id': NODE_ID})

    with ThreadPoolExecutor() as executor:
        executor.map(send_release, mutex_service.quorum_ids)


# --- Helper to Log Audits ---
def log_audit_event(cursor, patient_uuid, record_id, accessor_id, action):
    log_entry = {
        "log_id": str(uuid.uuid4()),
        "patient_uuid": patient_uuid,
        "record_id": record_id,
        "accessor_id": accessor_id,
        "action": action,
        "timestamp": clock_service.get_time()
    }
    cursor.execute(
        "INSERT INTO audit_logs (log_id, patient_uuid, record_id, accessor_id, action, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (log_entry['log_id'], log_entry['patient_uuid'], log_entry['record_id'], log_entry['accessor_id'], log_entry['action'], log_entry['timestamp'])
    )
    print(f"AUDIT: Logged '{action}' action for record '{record_id}' by '{accessor_id}'")

# --- Action Handlers (Updated for Auditing) ---

def handle_add_account(cursor, data):
    acquire_lock()
    try:
        user_uuid = str(uuid.uuid4())
        user_data = {
            'uuid': user_uuid, 'username': data['username'], 'first_name': data['first_name'],
            'last_name': data['last_name'], 'dob': data['dob'], 'password': data['password']
        }
        success, acks = perform_quorum_write(cursor, "user", user_data)
        if success:
            return {"status": "success", "code": 201, "message": "Account created", "uuid": user_uuid}
        else:
            return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally:
        release_lock()

def handle_add_record(cursor, data):
    acquire_lock()
    try:
        record_id = str(uuid.uuid4())
        patient_uuid = data['patient_uuid']
        synchronized_time = clock_service.get_time()
        new_record = {
            "record_id": record_id, "patient_uuid": patient_uuid, "doctor_name": data['doctor_name'],
            "description": data['description'], "resources_used": data.get('resources_used'),
            "prescription": data['prescription'], "timestamp": synchronized_time
        }
        success, acks = perform_quorum_write(cursor, "record", new_record)
        if success:
            log_audit_event(cursor, patient_uuid, record_id, data['doctor_name'], "CREATE")
            return {"status": "success", "code": 201, "message": "Record added"}
        else:
            return {"status": "error", "code": 500, "error": f"Quorum failed ({acks}/{QUORUM_W})"}
    finally:
        release_lock()

def handle_get_data(cursor, data):
    peer_ids = [i for i in ALL_DATA_NODES if i != NODE_ID]
    reachable_nodes = 1
    for peer_id in peer_ids:
        response = send_rpc_to_peer(peer_id, "ping", {})
        if response and response.get("status") == "success":
            reachable_nodes += 1
    
    if reachable_nodes < QUORUM_R:
        return {"status": "error", "code": 503, "error": f"Read Quorum failed. Only {reachable_nodes}/{QUORUM_R} nodes available."}

    cursor.execute("SELECT * FROM users WHERE username = ?", (data.get('username'),))
    user_row = cursor.fetchone()
    if user_row is None:
        return {"status": "error", "code": 404, "error": "User not found"}
    
    user_columns = [desc[0] for desc in cursor.description]
    user_data = dict(zip(user_columns, user_row))
    
    if user_data.get('password') != data.get('password'):
        return {"status": "error", "code": 401, "error": "Authentication failed"}
    del user_data['password']
    
    cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (user_data['uuid'],))
    record_rows = cursor.fetchall()
    record_columns = [desc[0] for desc in cursor.description]
    records_list = [dict(zip(record_columns, row)) for row in record_rows]
    
    return {"status": "success", "code": 200, "data": {"user_info": user_data, "records": records_list}}

def handle_get_records_by_uuid(cursor, data):
    patient_uuid = data.get('uuid')
    accessor_id = data.get('accessor_id', 'Unknown')
    
    cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (patient_uuid,))
    record_rows = cursor.fetchall()

    for row in record_rows:
        record_id = row[0] 
        log_audit_event(cursor, patient_uuid, record_id, accessor_id, "VIEW")

    if not record_rows:
        return {"status": "success", "code": 200, "data": []}
        
    record_columns = [desc[0] for desc in cursor.description]
    records_list = [dict(zip(record_columns, row)) for row in record_rows]
    return {"status": "success", "code": 200, "data": records_list}

def get_all_patients(cursor, data):
    cursor.execute("SELECT uuid, first_name, last_name, dob FROM users")
    rows = cursor.fetchall()
    patients = {row[0]: {"patient_id": row[0], "name": f"{row[1]} {row[2]}", "dob": row[3]} for row in rows}
    return {"status": "success", "data": patients}

def handle_get_audit_log(cursor, data):
    record_id = data.get('record_id')
    cursor.execute("SELECT * FROM audit_logs WHERE record_id = ? ORDER BY timestamp DESC", (record_id,))
    log_rows = cursor.fetchall()
    
    log_columns = [desc[0] for desc in cursor.description]
    logs_list = [dict(zip(log_columns, row)) for row in log_rows]
    return {"status": "success", "code": 200, "data": logs_list}

def handle_replicate_write(cursor, data):
    record_type = data.get("record_type")
    record_data = data.get("record_data")
    if record_type == "user":
        cursor.execute(
            "INSERT OR REPLACE INTO users (uuid, username, first_name, last_name, dob, password) VALUES (?, ?, ?, ?, ?, ?)",
            (record_data['uuid'], record_data['username'], record_data['first_name'], record_data['last_name'], record_data['dob'], record_data['password'])
        )
    elif record_type == "record":
        cursor.execute(
            "INSERT OR REPLACE INTO records (record_id, patient_uuid, doctor_name, description, resources_used, prescription, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (record_data['record_id'], record_data['patient_uuid'], record_data['doctor_name'], record_data['description'], record_data.get('resources_used'), record_data['prescription'], record_data['timestamp'])
        )
    else:
        return {"status": "error", "message": "Unknown record type"}
    return {"status": "success", "message": "Replication successful"}

ACTION_MAP = {
    "add_account": handle_add_account,
    "add_record": handle_add_record,
    "get_data": handle_get_data,
    "get_records_by_uuid": handle_get_records_by_uuid,
    "get_all_patients": get_all_patients,
    "get_audit_log": handle_get_audit_log,
}

def dispatch_rpc(action, data):
    conn = sqlite3.connect(f"/data/{DB_NAME_GLOBAL}", check_same_thread=False)
    cursor = conn.cursor()
    response = {}
    try:
        if action in ACTION_MAP:
            response = ACTION_MAP[action](cursor, data)
        elif action == "replicate_write":
            response = handle_replicate_write(cursor, data)
        elif action == "get_time_for_sync":
            return clock_service.get_time_for_sync()
        elif action == "adjust_time":
            clock_service.adjust_time(data['adjustment'])
            return "OK"
        elif action == "request_lock":
            return mutex_service.handle_request_rpc(data['requester_id'], data['ts'])
        elif action == "release_lock":
            return mutex_service.handle_release_rpc()
        elif action == "receive_grant":
            mutex_service.receive_reply(data['sender_id'])
            return "OK"
        elif action == "ping":
            return {"status": "success"}
        else:
            response = {"status": "error", "message": "Unknown action"}
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        response = {"status": "error", "message": f"An internal error occurred: {e}"}
        print(f"[ERROR] Exception during action '{action}': {e}")
    finally:
        conn.close()
    return response

def master_sync_loop():
    time.sleep(10)
    print(f"[Clock-{NODE_ID}] I am the MASTER. Starting sync loop.")
    while True:
        try:
            master_time, offsets, peer_offsets = time.time(), [], {}
            
            for peer_id, proxy in rpc_proxies.items():
                try:
                    peer_time = proxy.dispatch_rpc("get_time_for_sync", {})
                    offset = peer_time - master_time
                    offsets.append(offset)
                    peer_offsets[peer_id] = offset
                except Exception as e:
                    print(f"[Clock Master] -> FAILED to get time from Peer {peer_id}: {e}")
            
            if offsets:
                offsets.append(0.0)
                average_offset = mean(offsets)
                
                for peer_id, proxy in rpc_proxies.items():
                    if peer_id in peer_offsets:
                        adjustment = average_offset - peer_offsets[peer_id]
                        send_rpc_to_peer(peer_id, "adjust_time", {'adjustment': adjustment})

                clock_service.adjust_time(average_offset)
        except Exception as e:
            print(f"[ERROR] Unhandled exception in master_sync_loop: {e}")
        
        time.sleep(20)

def main(port, db_name):
    global NODE_PORT, NODE_ID, DB_NAME_GLOBAL, clock_service, mutex_service
    NODE_PORT, DB_NAME_GLOBAL = port, db_name

    for i, p in ALL_DATA_NODES.items():
        if p['rpc'] == NODE_PORT:
            NODE_ID = i
            break
    
    init_db(db_name)
    
    is_master_clock = (NODE_ID == 1)
    clock_service = BerkeleyClock(NODE_ID, is_master=is_master_clock)
    mutex_service = MaekawaMutex(NODE_ID, QUORUM_SETS[NODE_ID])
    
    time.sleep(5)
    connect_to_peers()

    if is_master_clock:
        master_thread = threading.Thread(target=master_sync_loop, daemon=True)
        master_thread.start()
    
    with SimpleXMLRPCServer(('0.0.0.0', port), allow_none=True) as server:
        server.register_introspection_functions()
        server.register_function(dispatch_rpc, 'dispatch_rpc')
        print(f"Data Node {NODE_ID} RPC server listening on {port}")
        server.serve_forever()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python data_node.py <port> <db_name>")
        sys.exit(1)
    port_arg, db_name_arg = int(sys.argv[1]), sys.argv[2]
    main(port_arg, db_name_arg)