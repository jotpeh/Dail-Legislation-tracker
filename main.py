import sys
import os
from ingestor import OireachtasIngestor
from parser import OireachtasParser
from analyzer import LocalAnalyzer

def run_pipeline(target_date=None):
    print("="*60)
    print("  DAIL DEBATE AUTOMATION PIPELINE")
    print("="*60)

    # --- STEP 1: INGEST ---
    print("\n[1/3] Starting Ingestor...")
    ingestor = OireachtasIngestor()
    
    # If no date provided, find the latest sitting
    date_processed = ingestor.fetch_dail_debates(target_date)
    
    if not date_processed:
        print("Pipeline aborted: No data found.")
        return

    # --- STEP 2: PARSE ---
    print(f"\n[2/3] Starting Parser for {date_processed}...")
    parser = OireachtasParser()
    try:
        parsed_data = parser.run(date_processed)
        
        # Save intermediate file (good for debugging)
        parsed_file = f"parsed_debates_{date_processed}.json"
        import json
        with open(parsed_file, 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, indent=4, ensure_ascii=False)
            
    except Exception as e:
        print(f"CRITICAL ERROR in Parser: {e}")
        return

    # --- STEP 3: ANALYZE ---
    print(f"\n[3/3] Starting Analyzer (LLM: gemma3:latest)...")
    analyzer = LocalAnalyzer(model="gemma3:latest")
    
    # We pass the parsed data object directly in memory! 
    # (No need to reload from disk, though we saved it as a backup)
    
    # Update the analyzer to accept data directly if you haven't already,
    # or just let it load the file we just saved. 
    # For simplicity here, we rely on the file we just saved:
    analyzer.run(date_processed)

    print("\n" + "="*60)
    print(f"  DONE. Blog post ready: blog_post_{date_processed}.md")
    print("="*60)

if __name__ == "__main__":
    # Allow manual date override: python main.py 2024-12-18
    selected_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipeline(selected_date)
