"""Lightweight local-only MDWorkflow shim for testing simulator <-> MDWorkflow HTTP exchange.

Usage:
  python3 MDWorkflow_local.py --host 0.0.0.0 --port 5001 --sim-host localhost --sim-port 5000

This script accepts POST /simulate with JSON payloads and immediately POSTs back a small
recommendation JSON to the simulator at / (simulator listens on / by default in the edits).
This is intentionally tiny and does not run Flink or the real pipeline.
"""
import argparse, json, time
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import requests

class SimHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get('content-length', 0))
            body = self.rfile.read(length)
            data = json.loads(body.decode('utf-8'))
            print('[MDWorkflow_local] Received simulate:', data)
            # build a fake recommendation payload
            rec = {
                'timestamp': int(time.time()*1000),
                'smiles': data.get('smiles', 'C'),
                'recommendation_score': 1.23,
                'model_id': data.get('model_id', 0)
            }
            # post recommendation back to simulator
            sim_url = f'http://{self.server.sim_host}:{self.server.sim_port}/'
            try:
                r = requests.post(sim_url, json=rec, timeout=5)
                print(f'[MDWorkflow_local] Posted recommendation -> {sim_url} status={r.status_code}')
            except Exception as e:
                print('[MDWorkflow_local] Failed to post back to simulator:', e)
        except Exception as e:
            print('[MDWorkflow_local] Error handling POST:', e)
        self.send_response(200)
        self.end_headers()

def run_server(host, port, sim_host, sim_port):
    server = HTTPServer((host, port), SimHandler)
    server.sim_host = sim_host
    server.sim_port = sim_port
    print(f'[MDWorkflow_local] Listening on http://{host}:{port} for /simulate')
    server.serve_forever()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5001)
    parser.add_argument('--sim-host', default='localhost')
    parser.add_argument('--sim-port', type=int, default=5000)
    args = parser.parse_args()
    run_server(args.host, args.port, args.sim_host, args.sim_port)
