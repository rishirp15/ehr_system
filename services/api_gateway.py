from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import requests
from itertools import cycle
import redis
import json

app = Flask(__name__)
CORS(app)

# --- Redis Connection for Logging ---
try:
    redis_client = redis.Redis(host='redis', port=6379, db=0)
    redis_client.ping()
    print("[Gateway] Connected to Redis for logging.")
except Exception as e:
    print(f"[Gateway] ERROR: Could not connect to Redis: {e}")
    redis_client = None

def publish_log(message):
    if redis_client:
        redis_client.publish('system_logs', message)

# --- App Node Configuration ---
APP_NODE_URLS = [
    "http://host.docker.internal:6001",
    "http://host.docker.internal:6002",
    "http://host.docker.internal:6003",
]
app_node_cycler = cycle(APP_NODE_URLS)

@app.route('/stream_logs')
def stream_logs():
    def event_stream():
        if not redis_client:
            yield f"data: [ERROR] Cannot connect to Redis log stream.\n\n"
            return

        pubsub = redis_client.pubsub()
        pubsub.subscribe('system_logs')
        print("[Gateway] Client subscribed to log stream.")
        for message in pubsub.listen():
            if message['type'] == 'message':
                log_data = message['data'].decode('utf-8')
                yield f"data: {log_data}\n\n"
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy_request(path):
    target_node_url = next(app_node_cycler)
    url = f"{target_node_url}/{path}"
    
    log_msg = f"[Gateway] Forwarding {request.method} /{path} to {target_node_url.split(':')[-2]}"
    print(log_msg)
    publish_log(json.dumps({'level': 'info', 'service': 'API Gateway', 'message': log_msg}))

    try:
        response = requests.request(
            method=request.method,
            url=url,
            headers={key: value for (key, value) in request.headers if key != 'Host'},
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=20
        )
        headers = [(name, value) for (name, value) in response.raw.headers.items()]
        return response.content, response.status_code, headers

    except requests.exceptions.RequestException as e:
        log_msg = f"[Gateway] Service unavailable error forwarding to {target_node_url}: {e}"
        print(f"[ERROR] {log_msg}")
        publish_log(json.dumps({'level': 'error', 'service': 'API Gateway', 'message': log_msg}))
        return jsonify({"error": "Service temporarily unavailable"}), 503

if __name__ == '__main__':
    print("API Gateway is running on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)