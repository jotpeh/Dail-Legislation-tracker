import json
import requests
import os
import sys
from lxml import etree

class OireachtasParser:
    def __init__(self, raw_data_dir="raw_data"):
        self.raw_data_dir = raw_data_dir
        # The Oireachtas XML uses specific namespaces
        self.namespaces = {
            'ns': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }

    def load_json_record(self, date_str):
        """Loads the metadata JSON we fetched in Module A."""
        filepath = f"{self.raw_data_dir}/dail_debates_{date_str}.json"
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"No data found for {date_str}. Run ingestor first.")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    def fetch_and_cache_xml(self, uri):
        """Fetches XML transcript and saves it locally."""
        filename = uri.split('/')[-1]
        if not filename.endswith('.xml'):
            filename += ".xml"
            
        local_path = os.path.join(self.raw_data_dir, filename)

        if os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                return f.read()
        
        full_url = uri if uri.startswith('http') else f"https://data.oireachtas.ie{uri}"
        
        print(f"Fetching XML transcript: {full_url}")
        response = requests.get(full_url)
        
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return response.content
        else:
            print(f"Failed to download XML: {response.status_code}")
            return None

    def parse_debate_xml(self, xml_content):
        """Extracts clean text from the Akoma Ntoso XML."""
        if not xml_content:
            return []

        root = etree.fromstring(xml_content)
        results = []
        
        speeches = root.findall('.//ns:speech', self.namespaces)
        
        for speech in speeches:
            by_attrib = speech.get('by')
            speaker_name = by_attrib.replace('#', '') if by_attrib else "Unknown"
            
            paragraphs = speech.findall('.//ns:p', self.namespaces)
            text_content = "\n".join([p.text for p in paragraphs if p.text])
            
            if text_content.strip():
                results.append({
                    'speaker': speaker_name,
                    'text': text_content
                })
                
        return results

    def run(self, date_str):
        data = self.load_json_record(date_str)
        results = data.get('results', [])
        
        daily_summary = []
        print(f"Parsing {len(results)} debate sections...")

        for result in results:
            # Depending on API version, contextDate or date might be used
            topic_name = result.get('contextDate', date_str) 
            
            # Try to find a human-readable title if possible, else fallback
            if 'debateRecord' in result and 'title' in result['debateRecord']:
                 topic_name = result['debateRecord']['title']

            uri = result['debateRecord']['formats']['xml']['uri']
            
            if uri:
                xml_content = self.fetch_and_cache_xml(uri)
                speeches = self.parse_debate_xml(xml_content)
                
                daily_summary.append({
                    'topic': topic_name,
                    'speeches': speeches
                })
        
        return daily_summary

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    parser = OireachtasParser()
    target_date = None
    
    # 1. Check if date provided via command line
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        # 2. Auto-detect latest file
        try:
            if not os.path.exists("raw_data"):
                print("Error: 'raw_data' folder not found.")
                sys.exit(1)
                
            files = [f for f in os.listdir("raw_data") if f.startswith("dail_debates_")]
            files.sort(reverse=True)
            
            if files:
                target_date = files[0].replace("dail_debates_", "").replace(".json", "")
                print(f"Auto-detected latest file: {target_date}")
            else:
                print("No data files found. Run ingestor.py first.")
                sys.exit(1)
        except Exception as e:
            print(f"Error detecting file: {e}")
            sys.exit(1)

    # 3. Run Parser
    if target_date:
        try:
            parsed_data = parser.run(target_date)
            
            # Save to JSON
            output_filename = f"parsed_debates_{target_date}.json"
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(parsed_data, f, indent=4, ensure_ascii=False)
                
            print(f"\nSUCCESS: Parsed {len(parsed_data)} sections.")
            print(f"Saved structured data to: {output_filename}")
            
        except Exception as e:
            print(f"Error during parsing: {e}")
