import json

def main():
    with open('cs_papers_full.json', 'r') as f:
        data = json.load(f)
    
    for doc in data:
        doc.pop("volume_no", None)
        doc.pop("issue_no", None)
        if 'cs_subfield' in doc:
            seen = set()
            deduplicated = []
            for field in doc['cs_subfield']:
                if field not in seen:
                    seen.add(field)
                    deduplicated.append(field)
            
            doc['academic_fields'] = deduplicated
            del doc['cs_subfield']

    with open('cs_papers_cleaned.json', 'w') as f:
        json.dump(data, f, indent=2)
        
if __name__ == "__main__":
    main()