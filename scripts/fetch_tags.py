
import requests
import json

def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    url = "https://gamma-api.polymarket.com/tags"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tags = response.json()
        
        # Sort by usage? The API doesn't seem to give count, but let's print them.
        # Check if it's a list or dict
        if isinstance(tags, list):
            print(f"Found {len(tags)} tags.")
            for t in tags[:50]: # Print first 50
                print(f"ID: {t.get('id')} | Label: {t.get('label')} | Slug: {t.get('slug')}")
                
            # save to file for inspection
            with open("polymarket_tags.json", "w") as f:
                json.dump(tags, f, indent=2)
                
    except Exception as e:
        print(f"Error fetching tags: {e}")

if __name__ == "__main__":
    main()
