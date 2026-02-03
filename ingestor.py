import requests
import json
import os
from datetime import date, timedelta

class OireachtasIngestor:
    def __init__(self):
        self.base_url = "https://api.oireachtas.ie/v1/debates"
        self.output_dir = "raw_data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def find_latest_sitting_date(self):
        """
        Scans the last 100 days to find the most recent date 
        that actually has Dáil debates.
        """
        print("Searching for the latest Dáil sitting...")
        
        # Look back 100 days from today
        today = date.today()
        start_date = today - timedelta(days=100)
        
        params = {
            "date_start": start_date.isoformat(),
            "date_end": today.isoformat(),
            "chamber_type": "house",
            "limit": 1000 # Fetch metadata for last 100 days (lightweight)
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # 1. Collect all dates that have a Dáil debate
            sitting_dates = set()
            for result in data.get('results', []):
                # Robust check for Dáil URI
                if '/house/dail/' in result['debateRecord']['house']['uri']:
                    sitting_dates.add(result['contextDate'])
            
            if not sitting_dates:
                return None
                
            # 2. Sort dates and pick the newest one
            latest_date = sorted(list(sitting_dates), reverse=True)[0]
            print(f"Latest sitting found: {latest_date}")
            return latest_date

        except Exception as e:
            print(f"Error searching for dates: {e}")
            return None

    def fetch_dail_debates(self, target_date: str = None):
        """
        Fetches debate data. If no date is provided, finds the latest one.
        """
        if target_date is None:
            target_date = self.find_latest_sitting_date()
            if target_date is None:
                print("Could not find any recent debates.")
                return None

        # Standard Fetch Logic
        params = {
            "date_start": target_date,
            "date_end": target_date,
            "chamber_type": "house",
            "limit": 100 
        }

        print(f"Fetching full debates for {target_date}...")
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Filter for Dáil only
            dail_results = []
            for result in data.get('results', []):
                if '/house/dail/' in result['debateRecord']['house']['uri']:
                    dail_results.append(result)

            if not dail_results:
                print(f"Warning: No Dáil debates found for {target_date}.")
                return None

            # Save Data
            save_data = {
                "date": target_date,
                "results": dail_results
            }
            
            filename = f"{self.output_dir}/dail_debates_{target_date}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=4, ensure_ascii=False)
            
            print(f"Saved raw data to {filename}")
            return target_date # Return the date so the parser knows what to use

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

if __name__ == "__main__":
    ingestor = OireachtasIngestor()
    # Call without arguments to auto-find the latest date
    latest_date = ingestor.fetch_dail_debates()
    
    if latest_date:
        print(f"\nSUCCESS! Data for {latest_date} is ready.")
        print(f"Now run: python parser.py")
