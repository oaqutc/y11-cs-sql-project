#!/usr/bin/env python3
"""
fetch_cs_papers.py
==================
End-to-end script to build a JSON database of the most-cited computer science
papers from the Semantic Scholar API.

The script runs in three phases:
  Phase 1 – Bulk search:  For each query term, fetch up to 1000 papers sorted
            by citation count via the /paper/search/bulk endpoint.
  Phase 2 – Enrichment:   Fetch full metadata (publicationDate, journal,
            s2FieldsOfStudy) for every collected paper via /paper/batch.
  Phase 3 – Transform:    Map the raw Semantic Scholar fields onto the target
            schema and write the final JSON file.

Output schema
-------------
Each record in the output JSON has these fields:

    title            – str   Paper title
    author           – str   Comma-separated author names
    publication_date – str   ISO date string (YYYY-MM-DD), or YYYY-01-01 when
                             only the year is known
    journal_name     – str   Journal or conference name
    volume_no        – str   Journal volume (empty string if unavailable)
    issue_no         – str   Journal issue (empty string; S2 rarely provides)
    citation_count   – int   Number of citations according to Semantic Scholar
    cs_subfield      – list  Category strings from s2FieldsOfStudy / fieldsOfStudy
    pdf_link         – str   Open-access PDF URL, Sci-Hub link (via DOI), or
                             ArXiv PDF URL; empty string if none available

Usage
-----
    python3 fetch_cs_papers.py                       # all three phases
    python3 fetch_cs_papers.py --phase 1             # bulk search only
    python3 fetch_cs_papers.py --phase 2             # enrichment only
    python3 fetch_cs_papers.py --phase 3             # schema transform only
    python3 fetch_cs_papers.py --api-key YOUR_KEY    # higher rate limits
    python3 fetch_cs_papers.py --output papers.json  # custom output path

Rate limits
-----------
  Without API key: ~100 requests per 5 minutes
  With API key:    ~1 request / second  (apply at
                    https://www.semanticscholar.org/product/api#api-key-form)

Typical runtime
---------------
  Phase 1: ~3 minutes   (15 queries × 1 page each)
  Phase 2: ~30 minutes  (12 chunks × 2 batch calls, with rate-limit waits)
  Phase 3: <5 seconds   (local transform only)
"""

from __future__ import annotations

import argparse
import json
import os
import time
import sys
from pathlib import Path

import requests

# ── Configuration ────────────────────────────────────────────────────────────

API_BASE = "https://api.semanticscholar.org/graph/v1"

# Fields returned by the bulk-search endpoint (Phase 1).
BULK_FIELDS = (
    "title,citationCount,year,authors,venue,fieldsOfStudy,"
    "openAccessPdf,externalIds,publicationVenue"
)

# Fields requested from the paper-batch endpoint (Phase 2).
# NOTE: "topics" and "s2FieldsOfStudy" are NOT supported by /paper/search/bulk,
#       but ARE available on /paper/batch and /paper/{id}.
ENRICH_FIELDS = (
    "title,citationCount,year,publicationDate,authors,venue,journal,"
    "s2FieldsOfStudy,openAccessPdf,externalIds,publicationVenue"
)

# Each query fetches the top N most-cited CS papers matching that keyword.
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

PAPERS_PER_QUERY = 1000   # max per bulk-search call
ENRICH_BATCH_SIZE = 500   # max per /paper/batch call
ENRICH_CHUNK_SIZE = 1000  # papers per intermediate chunk file (for resumability)

# Throttling / retries
INTER_REQUEST_DELAY = 1.5  # seconds between successful requests
RETRY_DELAY_BASE    = 15   # base seconds to wait on 429 / network error
MAX_RETRIES         = 5

# Default file paths
DEFAULT_INTERMEDIATE_DIR = Path(__file__).parent / ".s2_cache"
DEFAULT_OUTPUT           = Path(__file__).parent / "cs_papers.json"


# ── API client ───────────────────────────────────────────────────────────────

class SemanticScholarClient:
    """Thin, retry-aware wrapper around the Semantic Scholar REST API."""

    def __init__(self, api_key: str | None = None):
        self.session = requests.Session()
        if api_key:
            self.session.headers["x-api-key"] = api_key

    # -- GET (used by bulk search) ------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    wait = RETRY_DELAY_BASE * attempt
                    print(f"    ⏳ 429 rate-limited, waiting {wait}s "
                          f"(attempt {attempt}/{MAX_RETRIES})…")
                    time.sleep(wait)
                    continue
                print(f"    ❌ HTTP {r.status_code}: {r.text[:200]}")
                return {}
            except requests.exceptions.Timeout:
                print(f"    ⏳ Timeout, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY_BASE)
            except requests.exceptions.ConnectionError:
                print(f"    ⏳ Connection error, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY_BASE * 2)
            except Exception as e:
                print(f"    ❌ Unexpected error: {e}")
                return {}
        print("    ❌ Max retries exceeded")
        return {}

    # -- POST (used by paper/batch enrichment) ------------------------------

    def _post(self, url: str, params: dict | None = None,
              json_body: dict | None = None) -> list:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.post(url, params=params, json=json_body,
                                      timeout=90)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    wait = RETRY_DELAY_BASE * attempt
                    print(f"    ⏳ 429 rate-limited, waiting {wait}s "
                          f"(attempt {attempt}/{MAX_RETRIES})…")
                    time.sleep(wait)
                    continue
                print(f"    ❌ HTTP {r.status_code}: {r.text[:200]}")
                return []
            except requests.exceptions.Timeout:
                print(f"    ⏳ Timeout, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY_BASE)
            except requests.exceptions.ConnectionError:
                print(f"    ⏳ Connection error, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY_BASE * 2)
            except Exception as e:
                print(f"    ❌ Unexpected error: {e}")
                return []
        print("    ❌ Max retries exceeded")
        return []

    # -- High-level methods -------------------------------------------------

    def bulk_search(self, query: str, limit: int = PAPERS_PER_QUERY) -> list[dict]:
        """Fetch up to *limit* papers matching *query*, sorted by citation count."""
        all_papers: list[dict] = []
        token: str | None = None

        while len(all_papers) < limit:
            params = {
                "query": query,
                "fieldsOfStudy": "Computer Science",
                "sort": "citationCount:desc",
                "fields": BULK_FIELDS,
                "limit": min(1000, limit - len(all_papers)),
            }
            if token:
                params["token"] = token

            data = self._get(f"{API_BASE}/paper/search/bulk", params)
            if not data or "data" not in data:
                break

            batch = data["data"]
            all_papers.extend(batch)
            print(f"    Fetched {len(batch)} (total: {len(all_papers)})")

            token = data.get("token")
            if not token:
                break
            time.sleep(INTER_REQUEST_DELAY)

        return all_papers[:limit]

    def enrich_batch(self, paper_ids: list[str]) -> list[dict]:
        """Enrich up to 500 paper IDs via /paper/batch."""
        results = self._post(
            f"{API_BASE}/paper/batch",
            params={"fields": ENRICH_FIELDS},
            json_body={"ids": paper_ids},
        )
        return [r for r in results if r]  # filter out nulls


# ── Phase 1: Bulk search ────────────────────────────────────────────────────

def phase1_collect(client: SemanticScholarClient,
                   cache_dir: Path) -> dict[str, dict]:
    """Collect the top 1000 most-cited CS papers for each query term.

    Returns {paperId: raw_s2_dict, ...}.
    """
    output_path = cache_dir / "phase1_raw.json"
    if output_path.exists():
        print("📦 Phase 1 cache found – loading from disk. "
              "Delete the cache to re-fetch.")
        with open(output_path, encoding="utf-8") as f:
            return json.load(f)

    print("=" * 70)
    print("PHASE 1: Bulk-search top papers per query")
    print("=" * 70)

    all_papers: dict[str, dict] = {}

    for q in QUERIES:
        print(f'\n🔍 Query: "{q}"')
        papers = client.bulk_search(q, limit=PAPERS_PER_QUERY)

        new_count = 0
        for p in papers:
            pid = p.get("paperId")
            if pid and pid not in all_papers:
                all_papers[pid] = p
                new_count += 1

        print(f"  ✅ {len(papers)} fetched, {new_count} new unique "
              f"(total unique: {len(all_papers)})")
        time.sleep(INTER_REQUEST_DELAY)

    # Persist to cache
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_papers, f)

    print(f"\n📊 Phase 1 complete: {len(all_papers)} unique papers → {output_path}")
    return all_papers


# ── Phase 2: Enrichment ────────────────────────────────────────────────────

def phase2_enrich(client: SemanticScholarClient,
                  papers: dict[str, dict],
                  cache_dir: Path) -> dict[str, dict]:
    """Enrich every paper with journal, publicationDate, s2FieldsOfStudy.

    Processes papers in chunks of ENRICH_CHUNK_SIZE (1000).  Each chunk is
    saved to a separate file so that progress is preserved across restarts.
    """
    print("\n" + "=" * 70)
    print("PHASE 2: Enriching papers with full metadata")
    print("=" * 70)

    cache_dir.mkdir(parents=True, exist_ok=True)
    paper_ids = list(papers.keys())
    total = len(paper_ids)
    enriched: dict[str, dict] = {}

    # Calculate chunk boundaries
    n_chunks = (total + ENRICH_CHUNK_SIZE - 1) // ENRICH_CHUNK_SIZE

    for chunk_idx in range(n_chunks):
        chunk_path = cache_dir / f"phase2_chunk{chunk_idx}.json"

        # Load from cache if already done
        if chunk_path.exists():
            print(f"  📦 Chunk {chunk_idx}: loading from cache")
            with open(chunk_path, encoding="utf-8") as f:
                chunk_data = json.load(f)
            enriched.update(chunk_data)
            continue

        # Fetch this chunk
        start = chunk_idx * ENRICH_CHUNK_SIZE
        end = min(start + ENRICH_CHUNK_SIZE, total)
        chunk_ids = paper_ids[start:end]

        print(f"\n  Chunk {chunk_idx + 1}/{n_chunks} "
              f"({len(chunk_ids)} papers, IDs {start}–{end - 1})…")

        chunk_data: dict[str, dict] = {}
        for i in range(0, len(chunk_ids), ENRICH_BATCH_SIZE):
            batch_ids = chunk_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = i // ENRICH_BATCH_SIZE + 1
            total_batches = (len(chunk_ids) + ENRICH_BATCH_SIZE - 1) // ENRICH_BATCH_SIZE
            print(f"    Batch {batch_num}/{total_batches} ({len(batch_ids)} papers)…")

            results = client.enrich_batch(batch_ids)
            for ep in results:
                pid = ep.get("paperId")
                if pid:
                    chunk_data[pid] = ep

            print(f"    ✅ Enriched {len(results)}")
            time.sleep(INTER_REQUEST_DELAY)

        # Save chunk to disk
        with open(chunk_path, "w", encoding="utf-8") as f:
            json.dump(chunk_data, f)

        enriched.update(chunk_data)

    # Merge: keep Phase 1 data for any papers not enriched
    missing = 0
    for pid, p in papers.items():
        if pid not in enriched:
            enriched[pid] = p
            missing += 1

    # Save merged result
    merged_path = cache_dir / "phase2_enriched.json"
    with open(merged_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f)

    print(f"\n📊 Phase 2 complete: {len(enriched)} papers enriched "
          f"({missing} fell back to Phase 1 data) → {merged_path}")
    return enriched


# ── Phase 3: Schema transform ───────────────────────────────────────────────

def make_pdf_link(paper: dict) -> str:
    """Construct a PDF link: open-access → Sci-Hub (DOI) → ArXiv → ""."""
    oap = paper.get("openAccessPdf")
    if oap and isinstance(oap, dict):
        url = oap.get("url", "")
        if url and url.startswith("http"):
            return url

    ext = paper.get("externalIds") or {}
    doi = ext.get("DOI", "")
    if doi:
        return f"https://sci-hub.se/{doi}"

    arxiv = ext.get("ArXiv", "")
    if arxiv:
        return f"https://arxiv.org/pdf/{arxiv}"

    return ""


def extract_subfields(paper: dict) -> list[str]:
    """Pull category strings from s2FieldsOfStudy, falling back to fieldsOfStudy."""
    subfields: list[str] = []
    s2fos = paper.get("s2FieldsOfStudy")
    if s2fos and isinstance(s2fos, list):
        for entry in s2fos:
            if isinstance(entry, dict):
                cat = entry.get("category", "")
                if cat:
                    subfields.append(cat)
            elif isinstance(entry, str):
                subfields.append(entry)
    if not subfields:
        fos = paper.get("fieldsOfStudy")
        if fos and isinstance(fos, list):
            subfields = fos
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for s in subfields:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique


def extract_journal_info(paper: dict) -> dict[str, str]:
    """Extract journal name, volume, and issue from the enriched data."""
    journal_name = ""
    volume_no = ""
    issue_no = ""

    journal = paper.get("journal")
    if journal and isinstance(journal, dict):
        journal_name = journal.get("name", "")
        volume_no = str(journal.get("volume", "") or "")
        issue_no = str(journal.get("issue", "") or "")

    if not journal_name:
        venue = paper.get("venue")
        if venue:
            journal_name = venue
        else:
            pv = paper.get("publicationVenue")
            if pv and isinstance(pv, dict):
                journal_name = pv.get("name", "")

    return {"journal_name": journal_name, "volume_no": volume_no,
            "issue_no": issue_no}


def format_authors(paper: dict) -> str:
    """Return a comma-separated author string."""
    authors = paper.get("authors")
    if not authors or not isinstance(authors, list):
        return ""
    names = []
    for a in authors:
        if isinstance(a, dict):
            name = a.get("name", "")
            if name:
                names.append(name)
        elif isinstance(a, str):
            names.append(a)
    return ", ".join(names)


def format_publication_date(paper: dict) -> str | None:
    """Return an ISO date string, falling back to year-only."""
    pub_date = paper.get("publicationDate")
    if pub_date:
        return pub_date
    year = paper.get("year")
    if year:
        return f"{year}-01-01"
    return None


def phase3_transform(papers: dict[str, dict],
                     output_path: Path) -> list[dict]:
    """Transform raw S2 data into the target schema and write to disk."""
    print("\n" + "=" * 70)
    print("PHASE 3: Transforming to target schema")
    print("=" * 70)

    records: list[dict] = []
    for p in papers.values():
        if not p:
            continue
        jinfo = extract_journal_info(p)
        record = {
            "title":            p.get("title", ""),
            "author":           format_authors(p),
            "publication_date": format_publication_date(p),
            "journal_name":     jinfo["journal_name"],
            "volume_no":        jinfo["volume_no"],
            "issue_no":         jinfo["issue_no"],
            "citation_count":   p.get("citationCount", 0),
            "cs_subfield":      extract_subfields(p),
            "pdf_link":         make_pdf_link(p),
        }
        records.append(record)

    records.sort(key=lambda r: r["citation_count"], reverse=True)

    # ── Statistics ──────────────────────────────────────────────────────
    total = len(records)
    if total == 0:
        print("  ⚠️  No records to write.")
        return records

    with_date    = sum(1 for r in records if r["publication_date"]
                       and "-" in r["publication_date"]
                       and len(r["publication_date"]) > 4)
    with_journal = sum(1 for r in records if r["journal_name"])
    with_volume  = sum(1 for r in records if r["volume_no"])
    with_issue   = sum(1 for r in records if r["issue_no"])
    with_subfield= sum(1 for r in records if r["cs_subfield"])
    with_pdf     = sum(1 for r in records if r["pdf_link"])

    print(f"\n  Total papers:              {total}")
    print(f"  With publication date:     {with_date} ({with_date/total*100:.1f}%)")
    print(f"  With journal name:         {with_journal} ({with_journal/total*100:.1f}%)")
    print(f"  With volume number:        {with_volume} ({with_volume/total*100:.1f}%)")
    print(f"  With issue number:         {with_issue} ({with_issue/total*100:.1f}%)")
    print(f"  With CS subfield tags:     {with_subfield} ({with_subfield/total*100:.1f}%)")
    print(f"  With PDF link:             {with_pdf} ({with_pdf/total*100:.1f}%)")

    sf_counts: dict[str, int] = {}
    for r in records:
        for sf in r["cs_subfield"]:
            sf_counts[sf] = sf_counts.get(sf, 0) + 1
    print(f"\n  Top 20 subfields:")
    for sf, count in sorted(sf_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"    {sf:>35}: {count:>5}")

    print(f"\n  Top 10 by citation count:")
    for i, r in enumerate(records[:10], 1):
        print(f"    {i:>3}. {r['citation_count']:>7} │ "
              f"{r['publication_date'] or '?'} │ {r['title'][:80]}")

    # ── Write output ────────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"\n  ✅ {len(records)} records saved to {output_path}")
    return records


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch top CS papers from Semantic Scholar and produce a "
                    "JSON file with the target schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--api-key", default=None,
                        help="Semantic Scholar API key for higher rate limits")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3], default=None,
                        help="Run only the specified phase (default: all)")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help="Output JSON file path "
                             f"(default: {DEFAULT_OUTPUT})")
    parser.add_argument("--cache-dir", default=str(DEFAULT_INTERMEDIATE_DIR),
                        help="Directory for intermediate cache files "
                             f"(default: {DEFAULT_INTERMEDIATE_DIR})")
    parser.add_argument("--force", action="store_true",
                        help="Delete cached data and re-fetch everything")
    args = parser.parse_args()

    client = SemanticScholarClient(api_key=args.api_key)
    cache_dir = Path(args.cache_dir)
    output_path = Path(args.output)

    if args.force and cache_dir.exists():
        import shutil
        print(f"🗑️  Deleting cache directory: {cache_dir}")
        shutil.rmtree(cache_dir)

    start_time = time.time()

    # ── Phase 1 ─────────────────────────────────────────────────────────
    if args.phase is None or args.phase == 1:
        papers = phase1_collect(client, cache_dir)
    else:
        path = cache_dir / "phase1_raw.json"
        if not path.exists():
            print(f"❌ Phase 1 cache not found at {path}. Run Phase 1 first.")
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            papers = json.load(f)

    if not papers:
        print("❌ No papers collected. Exiting.")
        sys.exit(1)

    # ── Phase 2 ─────────────────────────────────────────────────────────
    if args.phase is None or args.phase == 2:
        papers = phase2_enrich(client, papers, cache_dir)
    else:
        path = cache_dir / "phase2_enriched.json"
        if not path.exists():
            print(f"❌ Phase 2 cache not found at {path}. Run Phase 2 first.")
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            papers = json.load(f)

    # ── Phase 3 ─────────────────────────────────────────────────────────
    if args.phase is None or args.phase == 3:
        records = phase3_transform(papers, output_path)
    else:
        if not output_path.exists():
            print(f"❌ Output file not found at {output_path}. Run Phase 3 first.")
            sys.exit(1)
        with open(output_path, encoding="utf-8") as f:
            records = json.load(f)

    elapsed = time.time() - start_time
    print(f"\n⏱️  Total elapsed: {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()