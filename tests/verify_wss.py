import asyncio
import logging
import sys
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_wss")

# Mock settings if needed or ensure env is loaded
from packages.core.settings import settings

async def verify_wss():
    """Verify WSS connection and message parsing."""
    logger.info("Starting WSS verification...")
    
    from apps.collector.adapters.polymarket_wss import PolymarketWebSocket
    from apps.collector.adapters.wss_messages import PriceUpdate
    
    client = PolymarketWebSocket()
    
    try:
        # 1. Test Connection
        logger.info("Connecting to WSS...")
        # We need at least one asset to sub to, or pass empty list
        # Using a tough-to-expire asset or just empty list for handshake
        await client.connect([])
        
        if client._metrics.mode == "wss":
            logger.info("✅ Connection Successful (Mode: WSS)")
        else:
            logger.error(f"❌ Connection Failed using mode: {client._metrics.mode}")
            sys.exit(1)
            
        # 2. Test Message Parsing (Unit Test part)
        from apps.collector.adapters.wss_messages import parse_wss_message
        logger.info("Testing message parsing...")
        
        sample = {
            "event_type": "price_change",
            "asset_id": "123",
            "price": "0.75",
            "timestamp": "1678888888000"
        }
        result = parse_wss_message(sample)
        if isinstance(result, PriceUpdate) and result.token_id == "123" and result.price == 0.75:
             logger.info("✅ Message Parsing Successful")
        else:
             logger.error(f"❌ Message Parsing Failed: {result}")
             sys.exit(1)
             
    except Exception as e:
        logger.error(f"❌ Verification Failed with error: {e}")
        sys.exit(1)
    finally:
        await client.close()
        logger.info("Client closed.")

if __name__ == "__main__":
    asyncio.run(verify_wss())
