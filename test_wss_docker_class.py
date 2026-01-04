import asyncio
import logging
import sys
import websockets

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wss_test_class")

# Add project root to path
import os
sys.path.append(os.getcwd())

from apps.collector.adapters.polymarket_wss import PolymarketWebSocket

async def test_class_connect():
    logger.info(f"websockets location: {websockets.__file__}")
    
    client = PolymarketWebSocket()
    logger.info("Created client")
    
    try:
        # Pass empty asset list to avoid subscription logic for now or simple test
        await client.connect([]) 
        logger.info("Client connected!")
        
        await asyncio.sleep(2)
        await client.close()
        logger.info("Client closed")
        
    except Exception as e:
        logger.error(f"Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_class_connect())
