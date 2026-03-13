import requests
from datetime import datetime, timedelta

def diagnostic_v3():
    last_week = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    url = f"https://api.oireachtas.ie/v1/debates?date_start={last_week}&limit=5"
    
    res = requests.get(url).json()
    results = res.get('results', [])
    
    print(f"--- Deep Diagnostic: {len(results)} records found ---")
    
    for item in results:
        # 1. Dig into the 'context' or 'debateRecord'
        dr = item.get('debateRecord', {})
        date = dr.get('date', 'Unknown Date')
        
        # 2. Extract Section Name from the nested structure
        # Often it's in a list called 'debateSections'
        sections = dr.get('debateSections', [])
        section_name = "No Section Name Found"
        if sections:
            # We take the first section's name
            section_name = sections[0].get('sectionName', 'Unnamed Section')
        
        # 3. Check for Bill Links (Crucial for our Brain module)
        # The API usually provides a 'bills' list at this level
        bills = dr.get('bills', [])
        bill_refs = [b.get('billRef') for b in bills if b.get('billRef')]
        
        print(f"📅 {date} | Topic: {section_name}")
        if bill_refs:
            print(f"   🔗 Linked Bills: {', '.join(bill_refs)}")
        else:
            print("   ⚠️ No direct Bill URIs linked to this record.")

diagnostic_v3()
