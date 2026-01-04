import asyncio
import websockets
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wss_test_docker_no_ctx")

async def test_connect():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    logger.info(f"Connecting to {url}...")
    try:
        # Mimic app usage exactly
        ws = await websockets.connect(url, ping_interval=None, open_timeout=20)
        logger.info("Connected successfully (no context manager)!")
        await ws.send('{"assets_ids": [], "type": "market"}')
        logger.info("Sent subscription")
        await asyncio.sleep(2)
        await ws.close()
        logger.info("Closed and Done")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_connect())
