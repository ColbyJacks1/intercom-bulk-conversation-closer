import os
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random
from concurrent.futures import ThreadPoolExecutor
import threading

# -------------------------------------------------------------
# Intercom Bulk Conversation Closer
# -------------------------------------------------------------
# Searches for open conversations in a given Intercom team inbox and
# closes them in bulk. Multiple modes balance speed vs. safety:
# - bulk_close: sequential, conservative, rate-limit aware
# - bulk_close_hybrid: parallel with light rate-limit handling (default)
# - bulk_close_maximal: fastest; minimal safety (may see more failures)
# -------------------------------------------------------------

# Load variables from .env
load_dotenv()

ACCESS_TOKEN = os.getenv("INTERCOM_ACCESS_TOKEN")
ADMIN_ID = os.getenv("INTERCOM_ADMIN_ID")
INBOX_ID = os.getenv("INTERCOM_INBOX_ID")

print("🔧 Loading environment variables...")
if not ACCESS_TOKEN or not ADMIN_ID or not INBOX_ID:
    raise RuntimeError("Missing env vars: set INTERCOM_ACCESS_TOKEN, INTERCOM_ADMIN_ID, INTERCOM_INBOX_ID")

print(f"✅ Environment loaded - Admin ID: {ADMIN_ID}, Inbox ID: {INBOX_ID}")

BASE_URL = "https://api.intercom.io"
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def check_rate_limits(resp):
    """Check and handle rate limit headers.

    Reads Intercom's `X-RateLimit-*` response headers, logs remaining
    quota, and introduces a small randomized delay when close to the
    limit to avoid bursty traffic.
    """
    remaining = int(resp.headers.get('X-RateLimit-Remaining', 1000))
    limit = int(resp.headers.get('X-RateLimit-Limit', 10000))
    reset_time = int(resp.headers.get('X-RateLimit-Reset', 0))
    
    print(f"📊 Rate limit: {remaining}/{limit} remaining")
    
    # If we're getting close to the limit, slow down a bit
    if remaining < 50:
        wait_time = 2 + random.uniform(0, 2)  # 2-4 seconds
        print(f"⏳ Approaching rate limit, waiting {wait_time:.1f}s...")
        time.sleep(wait_time)
    
    return remaining

def search_conversations(team_id, per_page=150):
    """Yield IDs of open conversations in a team inbox.

    Paginates through Intercom's conversations search API filtering by
    `team_assignee_id` and `state=open`, yielding each conversation ID.
    """
    print(f" Searching for open conversations in team {team_id}...")
    url = f"{BASE_URL}/conversations/search"
    payload = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": team_id},
                {"field": "state", "operator": "=", "value": "open"}
            ]
        },
        "pagination": {"per_page": per_page}
    }
    
    page_count = 0
    total_conversations = 0
    
    while True:
        page_count += 1
        print(f"📄 Fetching page {page_count}...")
        
        resp = requests.post(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
        check_rate_limits(resp)
        
        data = resp.json()
        conversations = data.get("conversations", [])
        total_conversations += len(conversations)
        pages_info = data.get("pages", {})
        print(f"   Found {len(conversations)} conversations on this page")
        if page_count == 1:  # Only show totals on first page
            print(f"   Total pages: {pages_info.get('total_pages', 'unknown')}, Total conversations: {data.get('total_count', 'unknown')}")
        
        for conv in conversations:
            yield conv["id"]
            
        next_page = data.get("pages", {}).get("next")
        if not next_page:
            break
        # Update payload with pagination parameters for next page
        payload = {
            "query": {
                "operator": "AND",
                "value": [
                    {"field": "team_assignee_id", "operator": "=", "value": team_id},
                    {"field": "state", "operator": "=", "value": "open"}
                ]
            },
            "pagination": {
                "per_page": per_page,
                "starting_after": next_page.get("starting_after")
            }
        }
    
    print(f"📊 Total conversations found: {total_conversations}")

def close_conversation(conv_id):
    """Close a single conversation with robust rate-limit handling.

    Uses the parts API to add a `close` action as the admin. Retries with
    exponential backoff and honors HTTP 429 `Retry-After` headers.
    """
    url = f"{BASE_URL}/conversations/{conv_id}/parts"
    payload = {
        "message_type": "close",
        "type": "admin",
        "admin_id": ADMIN_ID
    }
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"⏳ Rate limited! Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            
            resp.raise_for_status()
            check_rate_limits(resp)
            return resp.json()
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 5)
                print(f"⚠️  Request failed (attempt {attempt + 1}), retrying in {wait_time:.1f}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to close conversation {conv_id} after {max_retries} attempts: {e}")
                raise

#Functions --- 3 different versions of the close_conversation function

def close_conversation_maximal(conv_id):
    """Maximal speed version: single quick attempt, no backoff.

    Prioritizes speed over resilience. Returns the response JSON on
    success, or None on any failure.
    """
    url = f"{BASE_URL}/conversations/{conv_id}/parts"
    payload = {
        "message_type": "close",
        "type": "admin",
        "admin_id": ADMIN_ID
    }
    
    try:
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"❌ Failed to close conversation {conv_id}: {e}")
        return None

def close_conversation_hybrid(conv_id):
    """Hybrid approach: quick retries and 429-awareness.

    Short timeouts and a few retries with minimal sleeping on 429s.
    """
    url = f"{BASE_URL}/conversations/{conv_id}/parts"
    payload = {
        "message_type": "close",
        "type": "admin",
        "admin_id": ADMIN_ID
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            
            if resp.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(resp.headers.get("Retry-After", 5))
                print(f"⏳ Rate limited on {conv_id}, waiting {retry_after}s (attempt {attempt + 1})")
                time.sleep(retry_after)
                continue
            
            resp.raise_for_status()
            return resp.json()
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 1 + attempt  # 1s, 2s, 3s
                print(f"⚠️  Retrying {conv_id} in {wait_time}s (attempt {attempt + 1}): {e}")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to close conversation {conv_id} after {max_retries} attempts: {e}")
                return None

def close_conversations_parallel(conv_ids, max_workers=10):
    """Close multiple conversations concurrently using threads.

    Uses `close_conversation_maximal` for throughput. `max_workers`
    controls the number of concurrent requests.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(close_conversation_maximal, conv_ids))
    return results

def close_conversations_parallel_hybrid(conv_ids, max_workers=10):
    """Close multiple conversations concurrently with rate-limit handling.

    Uses `close_conversation_hybrid` per task to balance speed and safety.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(close_conversation_hybrid, conv_ids))
    return results

def bulk_close_maximal(team_id, parallel_workers=20, batch_size=100, max_conversations=None):
    """Bulk close at maximum speed with parallel batches.

    Collects all matching conversation IDs first, then processes them in
    parallel batches using the maximal close function. No backoff.
    """
    print(f"🚀 Starting MAXIMAL SPEED bulk close operation...")
    print(f"📋 Settings: parallel_workers={parallel_workers}, batch_size={batch_size}")
    print(f"⚠️  NO RATE LIMITING - MAXIMUM SPEED MODE")
    if max_conversations:
        print(f"🎯 Limiting to {max_conversations} conversations")
    
    count = 0
    start_time = datetime.now()
    last_progress_time = start_time
    
    # Collect all conversation IDs first so we can batch-process
    all_conv_ids = []
    for conv_id in search_conversations(team_id):
        all_conv_ids.append(conv_id)
        if max_conversations and len(all_conv_ids) >= max_conversations:
            break
    
    print(f"📊 Processing {len(all_conv_ids)} conversations in parallel...")
    
    # Process in parallel batches for high throughput
    for i in range(0, len(all_conv_ids), batch_size):
        batch = all_conv_ids[i:i + batch_size]
        print(f"🔥 Processing batch {i//batch_size + 1}: {len(batch)} conversations")
        
        # Process batch in parallel
        results = close_conversations_parallel(batch, max_workers=parallel_workers)
        
        # Count successful closures
        successful = sum(1 for r in results if r is not None)
        count += successful
        
        # Progress updates
        current_time = datetime.now()
        if count % 50 == 0 or (current_time - last_progress_time).seconds >= 10:
            elapsed = current_time - start_time
            rate = count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
            eta_seconds = (len(all_conv_ids) - count) / rate if rate > 0 else 0
            eta = datetime.now().replace(microsecond=0) + timedelta(seconds=eta_seconds)
            
            print(f"⚡ Progress: {count:,} closed | Rate: {rate:.1f}/sec | ETA: {eta.strftime('%H:%M:%S')}")
            last_progress_time = current_time
    
    total_time = datetime.now() - start_time
    print(f"🎉 MAXIMAL SPEED COMPLETE! Closed {count:,} conversations in {total_time}")
    print(f"📊 Average rate: {count/total_time.total_seconds():.1f} conversations/second")

def bulk_close_hybrid(team_id, parallel_workers=15, batch_size=50, max_conversations=None):
    """Bulk close using parallel batches with light safety checks.

    Streams conversation IDs (no full prefetch) and processes batches in
    parallel using the hybrid close function. Suitable for long runs.
    """
    print(f"🚀 Starting HYBRID SPEED bulk close operation...")
    print(f"📋 Settings: parallel_workers={parallel_workers}, batch_size={batch_size}")
    print(f"⚠️  HYBRID MODE - Fast but with rate limit protection")
    if max_conversations:
        print(f"🎯 Limiting to {max_conversations} conversations")
    
    count = 0
    failed_count = 0
    start_time = datetime.now()
    last_progress_time = start_time
    
    # Process conversations as they're found (streaming approach)
    current_batch = []
    batch_number = 1
    
    for conv_id in search_conversations(team_id):
        current_batch.append(conv_id)
        
        # Process batch when it's full
        if len(current_batch) >= batch_size:
            print(f"🔥 Processing batch {batch_number}: {len(current_batch)} conversations")
            
            # Process batch in parallel with rate limit handling
            results = close_conversations_parallel_hybrid(current_batch, max_workers=parallel_workers)
            
            # Count successful and failed closures
            successful = sum(1 for r in results if r is not None)
            failed = sum(1 for r in results if r is None)
            count += successful
            failed_count += failed
            
            # Progress updates
            current_time = datetime.now()
            if count % 50 == 0 or (current_time - last_progress_time).seconds >= 10:
                elapsed = current_time - start_time
                rate = count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                print(f"⚡ Progress: {count:,} closed | {failed_count:,} failed | Rate: {rate:.1f}/sec")
                last_progress_time = current_time
            
            # Reset batch
            current_batch = []
            batch_number += 1
            
            # Check if we've reached the limit
            if max_conversations and count >= max_conversations:
                break
    
    # Process any remaining conversations in the final batch
    if current_batch:
        print(f"🔥 Processing final batch {batch_number}: {len(current_batch)} conversations")
        results = close_conversations_parallel_hybrid(current_batch, max_workers=parallel_workers)
        successful = sum(1 for r in results if r is not None)
        failed = sum(1 for r in results if r is None)
        count += successful
        failed_count += failed
    
    total_time = datetime.now() - start_time
    print(f"🎉 HYBRID COMPLETE! Closed {count:,} conversations, {failed_count:,} failed in {total_time}")
    print(f"📊 Average rate: {count/total_time.total_seconds():.1f} conversations/second")
    print(f"📊 Success rate: {(count/(count+failed_count)*100):.1f}%" if (count+failed_count) > 0 else "📊 Success rate: 0%")

def bulk_close(team_id, batch_size=50, delay=0.1, max_conversations=None):
    """Sequential, conservative bulk close with periodic sleeps.

    Stays well below rate limits by sending one request at a time and
    sleeping briefly every `batch_size` requests.
    """
    print(f"🚀 Starting bulk close operation...")
    print(f"📋 Settings: batch_size={batch_size}, delay={delay}s")
    print(f"⚠️  Rate limit aware: ~1,666 calls per 10 seconds")
    if max_conversations:
        print(f"🎯 Limiting to {max_conversations} conversations")
    
    count = 0
    start_time = datetime.now()
    last_progress_time = start_time
    
    for conv_id in search_conversations(team_id):
        if max_conversations and count >= max_conversations:
            print(f" Reached limit of {max_conversations} conversations")
            break
            
        close_conversation(conv_id)
        count += 1
        
        # Progress updates every 100 conversations or every 30 seconds
        current_time = datetime.now()
        if count % 100 == 0 or (current_time - last_progress_time).seconds >= 30:
            elapsed = current_time - start_time
            rate = count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
            eta_seconds = (31000 - count) / rate if rate > 0 else 0
            eta = datetime.now().replace(microsecond=0) + timedelta(seconds=eta_seconds)
            
            print(f" Progress: {count:,} closed | Rate: {rate:.1f}/sec | ETA: {eta.strftime('%H:%M:%S')}")
            last_progress_time = current_time
        
        # Rate limit friendly batching
        if count % batch_size == 0:
            time.sleep(delay)
    
    total_time = datetime.now() - start_time
    print(f"🎉 Done! Closed {count:,} conversations in {total_time}")
    print(f"📊 Average rate: {count/total_time.total_seconds():.1f} conversations/second")

if __name__ == "__main__":
    print("=" * 60)
    print("🤖 Intercom Bulk Conversation Closer - Rate Limit Aware")
    print("=" * 60)
    
    
    # Default run uses the hybrid approach for a good balance of speed
    # and reliability when processing a large volume of conversations.
    print("🚀 FULL RUN - Processing all ~31,000 conversations with hybrid approach...")
    print("⚡ Expected: ~3.3 conversations/second, ~2.6 hours total")
    bulk_close_hybrid(team_id=INBOX_ID, parallel_workers=15, batch_size=50)