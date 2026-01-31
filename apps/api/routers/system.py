from fastapi import APIRouter
from packages.core.storage.db import get_db_pool
import time

router = APIRouter()

@router.get("/status")
async def get_system_status():
    """Get real-time system status and latency."""
    db = get_db_pool()
    rows = db.execute("SELECT key, value, updated_at FROM system_status", fetch=True) or []
    
    status = {
        "services": {},
        "timestamp": time.time()
    }
    
    for row in rows:
        key = row["key"]
        val = row["value"]
        # Add timestamp of when DB record was updated
        if row["updated_at"]:
            val["db_updated_at"] = row["updated_at"].timestamp()
        status["services"][key] = val
        
    return status
