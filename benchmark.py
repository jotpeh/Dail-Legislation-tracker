import time
import textstat
import sys
import os
import json
from analyzer import LocalAnalyzer

# --- CONFIGURATION ---
# Add the models you want to test here. 
# Ensure you have pulled them first (e.g., `ollama pull mistral`)
MODELS_TO_TEST = ["gemma3:latest", "llama3", "mistral", "qwen2.5:7b"]

def get_test_topic(date_str):
    """
    Loads the parsed data and picks the longest topic to use as a stress test.
    """
    filename = f"parsed_debates_{date_str}.json"
    if not os.path.exists(filename):
        print(f"Error: {filename} not found. Run parser.py first.")
        return None

    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Find the topic with the most speeches to ensure a rigorous test
    best_topic = None
    max_speeches = 0
    
    for section in data:
        count = len(section.get('speeches', []))
        if count > max_speeches:
            max_speeches = count
            best_topic = section
            
    print(f"Selected Test Topic: '{best_topic['topic']}' ({max_speeches} speeches)")
    return best_topic

def run_benchmark(target_date):
    topic_data = get_test_topic(target_date)
    if not topic_data:
        return

    print(f"\n{'='*80}")
    print(f"  BENCHMARKING LOCAL LLMS - {target_date}")
    print(f"{'='*80}")
    print(f"{'Model':<20} | {'Time (s)':<10} | {'Words':<10} | {'Flesch-Kincaid':<15} | {'Speed (w/s)':<10}")
    print("-" * 80)

    results = []

    for model_name in MODELS_TO_TEST:
        try:
            # 1. Initialize Analyzer with specific model
            analyzer = LocalAnalyzer(model=model_name)
            
            # 2. Start Timer
            start_time = time.perf_counter()
            
            # 3. Run Analysis (Directly call the method, don't write to file)
            # We use the existing logic in your class
            output_text = analyzer.analyze_topic(topic_data)
            
            # 4. Stop Timer
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            
            # 5. Calculate Metrics
            if output_text and "Error" not in output_text:
                word_count = textstat.lexicon_count(output_text, removepunct=True)
                fk_score = textstat.flesch_kincaid_grade(output_text)
                wps = word_count / elapsed_time if elapsed_time > 0 else 0
                
                print(f"{model_name:<20} | {elapsed_time:<10.2f} | {word_count:<10} | {fk_score:<15.1f} | {wps:<10.1f}")
                
                results.append({
                    "model": model_name,
                    "time": elapsed_time,
                    "words": word_count,
                    "readability": fk_score
                })
            else:
                print(f"{model_name:<20} | FAILED (Model output empty or error)")

        except Exception as e:
            print(f"{model_name:<20} | ERROR: {str(e)}")

    return results

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        run_benchmark(target_date)
    else:
        # Auto-detect latest parsed file
        try:
            files = [f for f in os.listdir(".") if f.startswith("parsed_debates_")]
            files.sort(reverse=True)
            if files:
                target_date = files[0].replace("parsed_debates_", "").replace(".json", "")
                run_benchmark(target_date)
            else:
                print("No parsed data found.")
        except Exception:
            pass
