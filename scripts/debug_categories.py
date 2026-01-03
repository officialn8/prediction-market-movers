
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.collector.adapters.polymarket import PolymarketAdapter

def main():
    logging.basicConfig(level=logging.INFO)
    adapter = PolymarketAdapter()
    
    # Search for Trump markets which likely have Politics category
    # We can't search via API params easily without implementing search, so just fetch 500 and filter
    markets = adapter.fetch_markets(limit=500, active=True)
    
    found = 0
    for m in markets:
        if "Trump" in m.title:
            print(f"ConditionID: {m.condition_id}")
            print(f"Title: {m.title}")
            print(f"Category: {m.category}")
            print("-" * 20)
            if m.category:
                found += 1
            
    print(f"Found {found} Trump markets with category")
        
    adapter.close()

if __name__ == "__main__":
    main()
