import requests
import pandas as pd
import time
from typing import Dict, List, Optional, Tuple
import json
from dataclasses import dataclass
from collections import defaultdict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# ======================
# Config & Constants
# ======================
BASE_URL = "https://api.bgm.tv"
USERNAME = ""
ACCESS_TOKEN = ""

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Subject types mapping
SUBJECT_TYPE_MAP = {
    1: "书籍",
    2: "动画", 
    3: "音乐",
    4: "游戏",
    6: "三次元"
}

# Collection types mapping  
COLLECTION_TYPE_MAP = {
    1: "想看",
    2: "看过",
    3: "在看",
    4: "搁置",
    5: "抛弃"
}

# Configuration for data collection
SUBJECT_TYPES = [1, 2, 3]         # 书籍 / 动画 / 音乐
COLLECTION_TYPES = [1, 2, 3, 4]  # 想看 / 看过 / 在看 / 搁置
LIMIT = 100

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # Exponential backoff factor
CONNECTION_TIMEOUT = 30
READ_TIMEOUT = 30

@dataclass
class CategoryStats:
    """Store statistics for each category"""
    subject_type: int
    collection_type: int
    total_items: int = 0
    fetched_items: int = 0
    pages_fetched: int = 0

# ======================
# Session Management
# ======================
def create_session() -> requests.Session:
    """
    Create a requests session with retry strategy and connection pooling
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    
    return session

# Global session object
_session = None

def get_session() -> requests.Session:
    """Get or create the global session object"""
    global _session
    if _session is None:
        _session = create_session()
    return _session

# ======================
# Core API Functions
# ======================
def fetch_category_total(subject_type: int, collection_type: int, session: requests.Session = None) -> int:
    """Get total count for a specific category with retry logic"""
    if session is None:
        session = get_session()
    
    params = {
        "subject_type": subject_type,
        "type": collection_type,
        "limit": 1,
        "offset": 0
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(
                f"{BASE_URL}/v0/users/{USERNAME}/collections",
                params=params,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT)
            )
            resp.raise_for_status()
            
            payload = resp.json()
            return payload.get("total", 0)
            
        except requests.exceptions.ConnectionError as e:
            print(f"  Connection error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF * (2 ** attempt)
                print(f"  Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {MAX_RETRIES} attempts")
                return 0
                
        except requests.exceptions.Timeout as e:
            print(f"  Timeout error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF * (2 ** attempt)
                print(f"  Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {MAX_RETRIES} attempts")
                return 0
                
        except Exception as e:
            print(f"  Error getting total for subject_type={subject_type}, collection_type={collection_type}: {e}")
            return 0
    
    return 0

def fetch_single_category(subject_type: int, collection_type: int, stats: CategoryStats, session: requests.Session = None) -> List[Dict]:
    """Fetch all items for a single category with improved error handling"""
    if session is None:
        session = get_session()
    
    all_items = []
    offset = 0
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    print(f"  Fetching {SUBJECT_TYPE_MAP.get(subject_type, f'Type{subject_type}')} - "
          f"{COLLECTION_TYPE_MAP.get(collection_type, f'Type{collection_type}')} "
          f"(Total: {stats.total_items})")
    
    while True:
        params = {
            "subject_type": subject_type,
            "type": collection_type,
            "limit": LIMIT,
            "offset": offset
        }
        
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(
                    f"{BASE_URL}/v0/users/{USERNAME}/collections",
                    params=params,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT)
                )
                
                if resp.status_code == 429:
                    print(f"    Rate limited, waiting 10 seconds...")
                    time.sleep(10)
                    continue
                    
                resp.raise_for_status()
                
                payload = resp.json()
                items = payload.get("data", [])
                
                if not items:
                    success = True
                    break
                
                all_items.extend(items)
                stats.fetched_items += len(items)
                stats.pages_fetched += 1
                consecutive_failures = 0  # Reset on success
                
                # Update progress
                if stats.total_items > 0:
                    progress = (offset + len(items)) / stats.total_items * 100
                    print(f"    Page {stats.pages_fetched}: {len(items)} items "
                          f"({min(progress, 100):.1f}%)")
                else:
                    print(f"    Page {stats.pages_fetched}: {len(items)} items")
                
                # Check if we've fetched all items
                if offset + LIMIT >= stats.total_items:
                    success = True
                    break
                    
                offset += LIMIT
                time.sleep(0.5)  # Slightly longer delay between requests
                success = True
                break
                
            except requests.exceptions.ConnectionError as e:
                print(f"    Connection error (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)[:100]}")
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF * (2 ** attempt)
                    print(f"    Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    
            except requests.exceptions.Timeout as e:
                print(f"    Timeout error (attempt {attempt + 1}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF * (2 ** attempt)
                    print(f"    Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    
            except requests.exceptions.RequestException as e:
                print(f"    Request error: {str(e)[:100]}")
                break
                
            except Exception as e:
                print(f"    Unexpected error: {str(e)[:100]}")
                break
        
        if not success:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print(f"    ⚠️ Stopping after {consecutive_failures} consecutive failures")
                break
            else:
                print(f"    Skipping this page after {MAX_RETRIES} attempts")
                offset += LIMIT
                continue
        
        if not items or offset + LIMIT >= stats.total_items:
            break
    
    return all_items

# ======================
# Data Processing Functions
# ======================
def parse_infobox(infobox: List[Dict]) -> Dict:
    """Convert Bangumi infobox to dict with better value handling"""
    result = {}
    
    if not infobox:
        return result
    
    for item in infobox:
        key = item.get("key", "").strip()
        if not key:
            continue
            
        value = item.get("value")
        
        # Handle different value types
        if isinstance(value, list):
            # Extract values from list of dicts or strings
            extracted = []
            for v in value:
                if isinstance(v, dict):
                    extracted.append(v.get("v", str(v)))
                else:
                    extracted.append(str(v))
            value = ", ".join(extracted) if extracted else None
        elif isinstance(value, dict):
            value = value.get("v", str(value))
        elif value is not None:
            value = str(value).strip()
        
        if value:
            result[key] = value
    
    return result

def extract_top_tags(tags: List[Dict], top_n: int = 5) -> Dict:
    """Extract top N tags safely with type checking"""
    output = {}
    for i in range(top_n):
        if i < len(tags) and isinstance(tags[i], dict):
            output[f"tag_{i+1}_name"] = tags[i].get("name")
            output[f"tag_{i+1}_count"] = tags[i].get("count")
        else:
            output[f"tag_{i+1}_name"] = None
            output[f"tag_{i+1}_count"] = None
    return output

def extract_common_fields(subject: Dict) -> Dict:
    """Extract common subject fields"""
    return {
        "subject_id": subject.get("id"),
        "name": subject.get("name"),
        "name_cn": subject.get("name_cn"),
        "score": subject.get("score"),
        "rank": subject.get("rank"),
        "collection_total": subject.get("collection_total"),
        "eps": subject.get("eps"),
        "volumes": subject.get("volumes"),
        "date": subject.get("date"),  # Air date
        "type": subject.get("type"),  # Subject type
        "short_summary": subject.get("short_summary", "")[:500],  # Limit length
    }

# ======================
# Main Execution Functions
# ======================
def get_all_category_totals() -> Dict[Tuple[int, int], CategoryStats]:
    """Get total counts for all category combinations"""
    print("📊 Getting category totals...")
    session = get_session()
    all_stats = {}
    
    total_combinations = len(SUBJECT_TYPES) * len(COLLECTION_TYPES)
    processed = 0
    
    for st in SUBJECT_TYPES:
        for ct in COLLECTION_TYPES:
            processed += 1
            print(f"[{processed}/{total_combinations}] Checking: "
                  f"{SUBJECT_TYPE_MAP.get(st, f'Type{st}')} - "
                  f"{COLLECTION_TYPE_MAP.get(ct, f'Type{ct}')}")
            
            total = fetch_category_total(st, ct, session)
            stats = CategoryStats(
                subject_type=st,
                collection_type=ct,
                total_items=total
            )
            all_stats[(st, ct)] = stats
            
            if total > 0:
                print(f"  Total items: {total}")
            else:
                print(f"  No items found")
            
            time.sleep(0.3)  # Be polite
    
    return all_stats

def collect_all_data(all_stats: Dict[Tuple[int, int], CategoryStats]) -> Tuple[List[Dict], List[Dict]]:
    """Collect data for all categories"""
    session = get_session()
    raw_rows = []
    analytics_rows = []
    
    print("\n" + "="*60)
    print("📥 Collecting data for all categories...")
    print("="*60)
    
    total_categories = len(all_stats)
    categories_with_data = sum(1 for stats in all_stats.values() if stats.total_items > 0)
    processed = 0
    
    for (st, ct), stats in all_stats.items():
        if stats.total_items == 0:
            continue
            
        processed += 1
        print(f"\n[{processed}/{categories_with_data}] "
              f"{SUBJECT_TYPE_MAP.get(st, f'Type{st}')} - "
              f"{COLLECTION_TYPE_MAP.get(ct, f'Type{ct}')}")
        
        collections = fetch_single_category(st, ct, stats, session)
        
        if not collections:
            print(f"  No data fetched for this category")
            continue
        
        # Process each item
        for item in collections:
            subject = item.get("subject", {})
            tags = subject.get("tags", [])
            infobox = parse_infobox(subject.get("infobox", []))
            
            # Convert to timezone-naive datetimes for Excel compatibility
            created_at = pd.to_datetime(item.get("created_at"), errors="coerce")
            if pd.notna(created_at) and hasattr(created_at, 'tz') and created_at.tz is not None:
                created_at = created_at.tz_localize(None)
            
            updated_at = pd.to_datetime(item.get("updated_at"), errors="coerce")
            if pd.notna(updated_at) and hasattr(updated_at, 'tz') and updated_at.tz is not None:
                updated_at = updated_at.tz_localize(None)
            
            # Extract common fields
            common_fields = extract_common_fields(subject)
            
            # ---------- RAW ----------
            raw_rows.append({
                "user_id": USERNAME,
                "subject_id": common_fields["subject_id"],
                "subject_type": st,
                "collection_type": ct,
                "created_at": created_at,
                "updated_at": updated_at,
                "ep_status": item.get("ep_status"),
                "vol_status": item.get("vol_status"),
                "name": common_fields["name"],
                "name_cn": common_fields["name_cn"],
                "score": common_fields["score"],
                "rank": common_fields["rank"],
                "collection_total": common_fields["collection_total"],
                "eps": common_fields["eps"],
                "volumes": common_fields["volumes"],
                "date": common_fields["date"],
                "type": common_fields["type"],
                "short_summary": common_fields["short_summary"],
                "tags": [t.get("name") for t in tags if isinstance(t, dict)],
                "tags_raw": json.dumps(tags, ensure_ascii=False) if tags else None,
                "infobox_raw": json.dumps(infobox, ensure_ascii=False) if infobox else None
            })
            
            # ---------- ANALYTICS ----------
            analytics_row = {
                "subject_id": common_fields["subject_id"],
                "subject_type": st,
                "collection_type": ct,
                "name_cn": common_fields["name_cn"] or common_fields["name"],
                "score": common_fields["score"],
                "rank": common_fields["rank"],
                "collection_total": common_fields["collection_total"],
                "created_at": created_at,
                "updated_at": updated_at,
                "eps": common_fields["eps"],
                "air_date": common_fields["date"],
                "director": infobox.get("导演") or infobox.get("監督"),
                "studio": infobox.get("动画制作") or infobox.get("アニメーション制作"),
                "country": infobox.get("国家/地区") or infobox.get("国"),
                "publisher": infobox.get("出版社") or infobox.get("発売元"),
                "author": infobox.get("作者") or infobox.get("著者"),
            }
            
            # Add tags
            tag_info = extract_top_tags(tags)
            analytics_row.update(tag_info)
            
            # Add all tags as comma-separated string
            all_tag_names = [t.get("name") for t in tags if t.get("name")]
            analytics_row["all_tags"] = ", ".join(all_tag_names) if all_tag_names else None
            
            analytics_rows.append(analytics_row)
        
        print(f"  ✅ Finished: {stats.fetched_items} items fetched from {stats.pages_fetched} pages")
    
    return raw_rows, analytics_rows

def export_data(df_raw: pd.DataFrame, df_analytics: pd.DataFrame, all_stats: Dict[Tuple[int, int], CategoryStats]):
    """Export data to files with category statistics"""
    output_file = "bangumi_collections_by_category.xlsx"
    
    print("\n" + "="*60)
    print("💾 Exporting data...")
    print("="*60)
    
    # Count categories with data for summary
    categories_with_data = sum(1 for stats in all_stats.values() if stats.total_items > 0)
    
    try:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Write raw data
            df_raw.to_excel(writer, sheet_name="raw_collections", index=False)
            
            # Write analytics data
            df_analytics.to_excel(writer, sheet_name="analytics_collections", index=False)
            
            # Create summary sheet
            summary_data = []
            for (st, ct), stats in all_stats.items():
                summary_data.append({
                    "Subject Type": SUBJECT_TYPE_MAP.get(st, f"Type{st}"),
                    "Collection Type": COLLECTION_TYPE_MAP.get(ct, f"Type{ct}"),
                    "Total Items": stats.total_items,
                    "Fetched Items": stats.fetched_items,
                    "Pages Fetched": stats.pages_fetched,
                    "Completion %": (stats.fetched_items / stats.total_items * 100) if stats.total_items > 0 else 0
                })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name="category_summary", index=False)
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                if sheet_name == "raw_collections":
                    df_to_use = df_raw
                elif sheet_name == "analytics_collections":
                    df_to_use = df_analytics
                else:
                    df_to_use = df_summary
                
                for i, col in enumerate(df_to_use.columns):
                    column_len = max(
                        df_to_use[col].astype(str).str.len().max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[worksheet.cell(1, i + 1).column_letter].width = min(column_len, 50)
        
        print(f"✅ Export finished: {output_file}")
        print(f"📊 Sheets created: raw_collections, analytics_collections, category_summary")
        
        # Save as CSV as well
        df_raw.to_csv("bangumi_raw_by_category.csv", index=False, encoding='utf-8-sig')
        df_analytics.to_csv("bangumi_analytics_by_category.csv", index=False, encoding='utf-8-sig')
        df_summary.to_csv("bangumi_category_summary.csv", index=False, encoding='utf-8-sig')
        
        print("📁 CSV files also saved with '_by_category' suffix")
        
        # Show detailed summary
        print("\n" + "="*60)
        print("📈 FINAL SUMMARY")
        print("="*60)
        print(f"Total categories checked: {len(all_stats)}")
        print(f"Categories with data: {categories_with_data}")
        print(f"Total raw items collected: {len(df_raw)}")
        print(f"Total analytics items: {len(df_analytics)}")
        
        if not df_summary.empty:
            total_fetched = df_summary["Fetched Items"].sum()
            total_expected = df_summary["Total Items"].sum()
            print(f"\n📊 Category Breakdown:")
            print(df_summary.to_string(index=False))
            print(f"\nOverall completion: {total_fetched}/{total_expected} items "
                  f"({total_fetched/total_expected*100:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error exporting to Excel: {e}")
        import traceback
        traceback.print_exc()

# ======================
# Main Function
# ======================
def main():
    """
    Main function that collects Bangumi data and returns dataframes
    Returns:
        tuple: (df_raw, df_analytics, all_stats) or (None, None, None) if no data
    """
    print("🚀 Starting Bangumi Data Collection")
    print("="*60)
    
    try:
        # Step 1: Get totals for all categories
        all_stats = get_all_category_totals()
        
        # Step 2: Collect data for all categories
        raw_rows, analytics_rows = collect_all_data(all_stats)
        
        if not raw_rows:
            print("❌ No data collected")
            return None, None, None
        
        # Step 3: Create DataFrames
        print("\n" + "="*60)
        print("📊 Building DataFrames...")
        df_raw = pd.DataFrame(raw_rows)
        df_analytics = pd.DataFrame(analytics_rows)
        
        # Step 4: Export data
        export_data(df_raw, df_analytics, all_stats)
        
        # Return dataframes for reuse in other scripts
        return df_raw, df_analytics, all_stats
        
    finally:
        # Clean up session
        global _session
        if _session:
            _session.close()
            _session = None

if __name__ == "__main__":
    main()