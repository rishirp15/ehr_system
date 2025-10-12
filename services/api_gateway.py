from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from itertools import cycle

app = Flask(__name__)
CORS(app)

APP_NODE_URLS = [
    "http://host.docker.internal:6001",
    "http://host.docker.internal:6002",
    "http://host.docker.internal:6003",
]

app_node_cycler = cycle(APP_NODE_URLS)

@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy_request(path):
    target_node_url = next(app_node_cycler)
    url = f"{target_node_url}/{path}"
    
    print(f"[*] API Gateway forwarding request for '{path}' to {url}")

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
        print(f"[ERROR] Could not forward request to {url}: {e}")
        return jsonify({"error": "Service temporarily unavailable"}), 503

if __name__ == '__main__':
    print("API Gateway is running on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)