"""
Intercom Bulk Updater - Reusable Framework
==========================================

A modular framework for performing bulk operations on Intercom data.
The base class handles common patterns like searching, pagination, rate limiting,
and parallel processing, while specific implementations define the search criteria
and actions to perform.

Usage:
    from intercom_bulk_updater import IntercomBulkUpdater
    from conversation_closer import ConversationCloser
    
    # Use a pre-built implementation
    closer = ConversationCloser()
    closer.bulk_close_hybrid(team_id="12345")
    
    # Or create a custom implementation
    class CustomUpdater(IntercomBulkUpdater):
        def get_search_query(self, **kwargs):
            return {"field": "custom_field", "operator": "=", "value": "custom_value"}
        
        def perform_action(self, item_id, **kwargs):
            # Your custom action here
            pass
"""

import os
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Generator, Callable


class IntercomBulkUpdater(ABC):
    """
    Base class for performing bulk operations on Intercom data.
    
    Handles common patterns like:
    - Authentication and API setup
    - Rate limiting and retry logic
    - Search and pagination
    - Parallel processing with different safety modes
    - Progress tracking and reporting
    """
    
    def __init__(self, access_token: str = None, admin_id: str = None):
        """Initialize the bulk updater with API credentials."""
        # Load from environment if not provided
        if not access_token:
            load_dotenv()
            access_token = os.getenv("INTERCOM_ACCESS_TOKEN")
        if not admin_id:
            load_dotenv()
            admin_id = os.getenv("INTERCOM_ADMIN_ID")
            
        if not access_token or not admin_id:
            raise RuntimeError("Missing credentials: set INTERCOM_ACCESS_TOKEN and INTERCOM_ADMIN_ID")
        
        self.access_token = access_token
        self.admin_id = admin_id
        self.base_url = "https://api.intercom.io"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        print(f"✅ IntercomBulkUpdater initialized - Admin ID: {admin_id}")
    
    def check_rate_limits(self, resp: requests.Response) -> int:
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
    
    @abstractmethod
    def get_search_endpoint(self) -> str:
        """Return the API endpoint for searching items."""
        pass
    
    @abstractmethod
    def get_search_query(self, **kwargs) -> Dict[str, Any]:
        """Return the search query for finding items to process."""
        pass
    
    @abstractmethod
    def get_item_id(self, item: Dict[str, Any]) -> str:
        """Extract the ID from a search result item."""
        pass
    
    @abstractmethod
    def perform_action(self, item_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Perform the action on a single item. Return response or None on failure."""
        pass
    
    def search_items(self, per_page: int = 150, **search_kwargs) -> Generator[str, None, None]:
        """Yield IDs of items matching the search criteria.
        
        Paginates through the search API and yields each item ID.
        Subclasses define the search criteria via get_search_query().
        """
        endpoint = self.get_search_endpoint()
        url = f"{self.base_url}/{endpoint}"
        query = self.get_search_query(**search_kwargs)
        
        payload = {
            "query": query,
            "pagination": {"per_page": per_page}
        }
        
        page_count = 0
        total_items = 0
        
        print(f"🔍 Searching for items with query: {query}")
        
        while True:
            page_count += 1
            print(f"📄 Fetching page {page_count}...")
            
            resp = requests.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            self.check_rate_limits(resp)
            
            data = resp.json()
            items = data.get("conversations", []) or data.get("items", []) or data.get("data", [])
            total_items += len(items)
            pages_info = data.get("pages", {})
            
            print(f"   Found {len(items)} items on this page")
            if page_count == 1:  # Only show totals on first page
                print(f"   Total pages: {pages_info.get('total_pages', 'unknown')}, Total items: {data.get('total_count', 'unknown')}")
            
            for item in items:
                yield self.get_item_id(item)
            
            next_page = data.get("pages", {}).get("next")
            if not next_page:
                break
                
            # Update payload with pagination parameters for next page
            payload = {
                "query": query,
                "pagination": {
                    "per_page": per_page,
                    "starting_after": next_page.get("starting_after")
                }
            }
        
        print(f"📊 Total items found: {total_items}")
    
    def perform_action_with_retry(self, item_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Perform action with hybrid retry logic - fast but with 429 handling."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.perform_action(item_id, **kwargs)
                if result is not None:
                    return result
                    
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 5))
                    print(f"⏳ Rate limited on {item_id}, waiting {retry_after}s (attempt {attempt + 1})")
                    time.sleep(retry_after)
                    continue
                
                if attempt < max_retries - 1:
                    wait_time = 1 + attempt  # 1s, 2s, 3s
                    print(f"⚠️  Retrying {item_id} in {wait_time}s (attempt {attempt + 1}): {e}")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Failed to process item {item_id} after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def process_items_parallel(self, item_ids: List[str], action_func: Callable, max_workers: int = 10) -> List[Optional[Dict[str, Any]]]:
        """Process multiple items concurrently using threads."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(action_func, item_ids))
        return results
    
    def bulk_process(self, parallel_workers: int = 15, batch_size: int = 50, max_items: int = None, **search_kwargs) -> Dict[str, int]:
        """Bulk processing with parallel batches and rate limit safety."""
        print(f"🚀 Starting bulk processing...")
        print(f"📋 Settings: parallel_workers={parallel_workers}, batch_size={batch_size}")
        print(f"⚠️  HYBRID MODE - Fast but with rate limit protection")
        
        count = 0
        failed_count = 0
        start_time = datetime.now()
        last_progress_time = start_time
        
        # Process items as they're found (streaming approach)
        current_batch = []
        batch_number = 1
        
        for item_id in self.search_items(**search_kwargs):
            current_batch.append(item_id)
            
            # Process batch when it's full
            if len(current_batch) >= batch_size:
                print(f"🔥 Processing batch {batch_number}: {len(current_batch)} items")
                
                # Process batch in parallel with rate limit handling
                results = self.process_items_parallel(current_batch, self.perform_action_with_retry, max_workers=parallel_workers)
                
                # Count successful and failed operations
                successful = sum(1 for r in results if r is not None)
                failed = sum(1 for r in results if r is None)
                count += successful
                failed_count += failed
                
                # Progress updates
                current_time = datetime.now()
                if count % 50 == 0 or (current_time - last_progress_time).seconds >= 10:
                    elapsed = current_time - start_time
                    rate = count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    print(f"⚡ Progress: {count:,} processed | {failed_count:,} failed | Rate: {rate:.1f}/sec")
                    last_progress_time = current_time
                
                # Reset batch
                current_batch = []
                batch_number += 1
                
                # Check if we've reached the limit
                if max_items and count >= max_items:
                    break
        
        # Process any remaining items in the final batch
        if current_batch:
            print(f"🔥 Processing final batch {batch_number}: {len(current_batch)} items")
            results = self.process_items_parallel(current_batch, self.perform_action_with_retry, max_workers=parallel_workers)
            successful = sum(1 for r in results if r is not None)
            failed = sum(1 for r in results if r is None)
            count += successful
            failed_count += failed
        
        total_time = datetime.now() - start_time
        print(f"🎉 Bulk processing complete! Processed {count:,} items, {failed_count:,} failed in {total_time}")
        print(f"📊 Average rate: {count/total_time.total_seconds():.1f} items/second")
        print(f"📊 Success rate: {(count/(count+failed_count)*100):.1f}%" if (count+failed_count) > 0 else "📊 Success rate: 0%")
        
        return {"success": count, "failed": failed_count, "total_time": total_time}


