import asyncio
import websockets
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def handle_connection(websocket):
    try:
        logging.info(f"New client connected from {websocket.remote_address[0]}:{websocket.remote_address[1]}")
        
        # Send an initial SQL query as an example
        initial_query = {
            "message_type": "sql",
            "query": "SELECT * FROM hits LIMIT 15"
        }
        await websocket.send(json.dumps(initial_query))
        
        # Keep the connection alive and log incoming messages
        while True:
            message = await websocket.recv()
            logging.info(f"Received message from client: {message}")
            await asyncio.sleep(1)
            
    except websockets.exceptions.ConnectionClosed:
        logging.info("Client disconnected")
    except Exception as e:
        logging.error(f"Error handling connection: {e}")

async def main():
    server = await websockets.serve(
        handle_connection,
        "0.0.0.0",
        12306
    )
    logging.info("WebSocket server started on ws://0.0.0.0:12306")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
