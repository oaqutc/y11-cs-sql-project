import requests
import json
import time

API_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"

FIELDS = "title,citationCount,year,authors,venue,fieldsOfStudy,openAccessPdf,externalIds,publicationVenue"

QUERIES = [
    "algorithm",
    "computing",
    "machine learning",
    "network",
    "data",
    "deep learning",
    "computer vision",
    "natural language",
    "database",
    "distributed system",
    "security",
    "graphics",
    "complexity theory",
    "software engineering",
    "robotics",
]

def fetch_top_papers(query, limit=100, max_retries=3):
    """Fetch top papers for a single query, sorted by citation count."""
    for attempt in range(max_retries):
        try:
            r = requests.get(
                API_URL,
                params={
                    "query": query,
                    "fieldsOfStudy": "Computer Science",
                    "sort": "citationCount:desc",
                    "fields": FIELDS,
                    "limit": limit,
                },
                timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                papers = data.get("data", [])
                print(f"  {query:>20} | got {len(papers)} papers")
                return papers
            elif r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  {query:>20} | rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  {query:>20} | ERROR {r.status_code}: {r.text[:150]}")
                return []
        except requests.exceptions.Timeout:
            print(f"  {query:>20} | timeout, retry {attempt+1}/{max_retries}")
            time.sleep(5)
        except Exception as e:
            print(f"  {query:>20} | exception: {e}")
            time.sleep(5)
    return []

def main():
    all_papers = {}

    for q in QUERIES:
        papers = fetch_top_papers(q, limit=100)
        for p in papers:
            pid = p.get("paperId")
            if pid and pid not in all_papers:
                all_papers[pid] = p
        time.sleep(2)

    print(f"\nTotal unique papers collected: {len(all_papers)}")
    
    with open("raw_bulk_data.json", mode="w", encoding="utf-8") as f:
        json.dump(all_papers, f, indent = 2)

if __name__ == "__main__":
    main()