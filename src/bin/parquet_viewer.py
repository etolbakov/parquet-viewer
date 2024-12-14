import asyncio
import websockets
import logging
import json
import argparse
import sys

# Move logging configuration to after argument parsing
logging.getLogger().setLevel(logging.WARNING)  # Set default to WARNING (less verbose)

async def handle_connection(websocket, sql_query, server):
    try:
        logging.debug(f"New client connected from {websocket.remote_address[0]}:{websocket.remote_address[1]}")
        
        # Send the SQL query provided via command line
        query_message = {
            "message_type": "sql",
            "query": sql_query
        }
        await websocket.send(json.dumps(query_message))
        logging.debug("Query sent, waiting for acknowledgment")
        
        # Wait for acknowledgment from client
        response = await websocket.recv()
        try:
            logging.debug(f"Received response: {response}")
            response_json = json.loads(response)
            if response_json.get("message_type") == "ack":
                logging.debug("Received acknowledgment from client")
            else:
                logging.warning(f"Received unexpected message type: {response_json.get('message_type')}")
        except json.JSONDecodeError:
            logging.warning("Received invalid JSON response")
        
        # Close the connection gracefully
        await websocket.close()
        
        # Close the server after handling one connection
        server.close()
        
    except websockets.exceptions.ConnectionClosed:
        logging.debug("Client disconnected")
    except Exception as e:
        logging.error(f"Error handling connection: {e}")

async def main(sql_query):
    # Create a done event to signal when to stop the server
    done = asyncio.Event()
    
    async def connection_handler(websocket):
        server = ws_server
        await handle_connection(websocket, sql_query, server)
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
    parser.add_argument('sql_query', type=str, help='SQL query to send to the client')
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
        asyncio.run(main(args.sql_query))
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
        sys.exit(0)
