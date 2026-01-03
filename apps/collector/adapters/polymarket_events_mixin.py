
    def fetch_markets_via_events(self, limit: int = 100, active: bool = True) -> List[PolymarketMarket]:
        """
        Fetch markets by iterating through events, which is the recommended way
        to get all active markets and ensures better category/metadata coverage.
        """
        markets = []
        offset = 0
        page_size = 50  # Default page size for events
        
        while True:
            params = {
                "limit": page_size,
                "offset": offset,
                "closed": str(not active).lower(),
                "order": "id",
                "ascending": "false" # Newest first
            }
            
            try:
                # Use /events endpoint
                url = f"{self.base_url}/events"
                data = self._get(url, params)
                
                if not data:
                    break
                    
                # Process events
                for event in data:
                    event_category = event.get("category")
                    
                    # Each event has a 'markets' list
                    event_markets = event.get("markets", [])
                    for m_data in event_markets:
                        # Enrich market data with event metadata if needed
                        if not m_data.get("category") and event_category:
                            m_data["category"] = event_category
                        
                        # Add event tags to market tags if missing
                        if "tags" not in m_data and "tags" in event:
                             m_data["tags"] = event["tags"]
                             
                        # Parse
                        pm_market = self._parse_market(m_data)
                        if pm_market:
                            markets.append(pm_market)
                
                # Check if we reached the limit requested
                if len(markets) >= limit:
                    break
                
                if len(data) < page_size:
                    break
                    
                offset += page_size
                
            except Exception as e:
                logger.error(f"Error fetching events page at offset {offset}: {e}")
                break
                
        return markets[:limit]
