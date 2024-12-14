import asyncio
import websockets
import logging
import json
import argparse
import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import socket
import time

# Move logging configuration to after argument parsing
logging.getLogger().setLevel(logging.WARNING)  # Set default to WARNING (less verbose)

def start_http_server(directory, port):
    """Start HTTP server in a separate thread and wait until it's ready"""
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', '*')
            super().end_headers()

        def do_OPTIONS(self):
            self.send_response(200)
            self.end_headers()

    httpd = HTTPServer(('0.0.0.0', port), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()

    return httpd

def find_free_port():
    """Find a free port to use for the HTTP server"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
        return port

async def handle_connection(websocket, sql_query, parquet_file, server):
    try:
        logging.debug(f"New client connected from {websocket.remote_address[0]}:{websocket.remote_address[1]}")
        
        # If SQL query is provided, send it in the expected format
        if sql_query:
            query_message = {
                "sql": {
                    "query": sql_query
                }
            }
            await websocket.send(json.dumps(query_message))
            logging.debug("SQL query sent")
        
        # If parquet file is provided, start HTTP server and send file info
        if parquet_file:
            if not os.path.exists(parquet_file):
                logging.error(f"Parquet file not found: {parquet_file}")
                await websocket.close()
                return

            # Start HTTP server in the directory containing the parquet file
            file_dir = os.path.dirname(os.path.abspath(parquet_file))
            if not file_dir:
                file_dir = '.'
            
            http_port = find_free_port()
            http_server = start_http_server(file_dir, http_port)
            
            # Send file information
            file_message = {
                "parquet_file": {
                    "file_name": os.path.basename(parquet_file),
                    "server_address": f"http://localhost:{http_port}",
                }
            }
            await websocket.send(json.dumps(file_message))
            logging.debug(f"File information sent for {parquet_file}")
        
        # Wait for acknowledgment from client
        while True:
            try:
                response = await websocket.recv()
                logging.debug(f"Received response: {response}")
                response_json = json.loads(response)
                if response_json.get("message_type") == "ack":
                    logging.debug("Received acknowledgment from client")
                    break  # Exit the loop when we get valid JSON with correct message type
                else:
                    logging.warning(f"Received unexpected message type: {response_json.get('message_type')}")
                    # Continue waiting for correct message type
            except json.JSONDecodeError:
                logging.warning("Received invalid JSON response, waiting for valid response...")
                # Continue waiting for valid JSON
            except websockets.exceptions.ConnectionClosed:
                raise
        
        # Close the connection gracefully
        await websocket.close()
        
        # Shutdown HTTP server if it was started
        if parquet_file:
            http_server.shutdown()
            http_server.server_close()
        
        # Close the websocket server after handling one connection
        server.close()
        
    except websockets.exceptions.ConnectionClosed:
        logging.debug("Client disconnected")
    except Exception as e:
        logging.error(f"Error handling connection: {e}")

async def main(sql_query, parquet_file):
    # Create a done event to signal when to stop the server
    done = asyncio.Event()
    
    async def connection_handler(websocket):
        server = ws_server
        await handle_connection(websocket, sql_query, parquet_file, server)
        done.set()  # Signal that we're done after handling one connection
    
    ws_server = await websockets.serve(connection_handler, "0.0.0.0", 12306)
    logging.debug("WebSocket server started on ws://0.0.0.0:12306")
    
    # Wait for the done event
    await done.wait()
    
    # Clean up
    ws_server.close()
    await ws_server.wait_closed()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='WebSocket server for SQL queries')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-q', '--sql', type=str, help='SQL query to send to the client')
    group.add_argument('-f', '--file', type=str, help='Path to parquet file to send to the client')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging based on verbose flag
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    else:
        logging.basicConfig(
            level=logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    try:
        asyncio.run(main(args.sql, args.file))
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
        sys.exit(0)
