"""
Conversation Closer - Specific Implementation
============================================

A specific implementation of IntercomBulkUpdater for closing conversations.
This demonstrates how to use the base class for a specific use case.
"""

import requests
from intercom_bulk_updater import IntercomBulkUpdater
from typing import Dict, Any, Optional


class ConversationCloser(IntercomBulkUpdater):
    """
    Bulk conversation closer for Intercom.
    
    Searches for open conversations in a team inbox and closes them.
    Supports multiple processing modes: sequential, hybrid, and maximal.
    """
    
    def get_search_endpoint(self) -> str:
        """Return the conversations search endpoint."""
        return "conversations/search"
    
    def get_search_query(self, team_id: str = None, state: str = "open", **kwargs) -> Dict[str, Any]:
        """Return the search query for finding open conversations in a team."""
        if not team_id:
            raise ValueError("team_id is required for conversation search")
        
        return {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": team_id},
                {"field": "state", "operator": "=", "value": state}
            ]
        }
    
    def get_item_id(self, item: Dict[str, Any]) -> str:
        """Extract conversation ID from search result."""
        return item["id"]
    
    def perform_action(self, conversation_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Close a single conversation.
        
        Uses the parts API to add a 'close' action as the admin.
        """
        url = f"{self.base_url}/conversations/{conversation_id}/parts"
        payload = {
            "message_type": "close",
            "type": "admin",
            "admin_id": self.admin_id
        }
        
        resp = self._make_request(url, payload)
        return resp.json() if resp else None
    
    def _make_request(self, url: str, payload: Dict[str, Any], timeout: int = 30) -> Optional[Any]:
        """Make a single API request with basic error handling."""
        try:
            resp = requests.post(url, headers=self.headers, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return None
    
    def bulk_close(self, team_id: str, parallel_workers: int = 15, batch_size: int = 50, max_conversations: int = None, **kwargs) -> Dict[str, int]:
        """Close conversations using parallel processing with rate limit safety."""
        print(f"🚀 Starting conversation closing...")
        return self.bulk_process(
            parallel_workers=parallel_workers,
            batch_size=batch_size,
            max_items=max_conversations,
            team_id=team_id,
            **kwargs
        )


# Convenience functions for backward compatibility
def bulk_close(team_id: str, parallel_workers: int = 15, batch_size: int = 50, max_conversations: int = None) -> Dict[str, int]:
    """Close conversations using parallel processing with rate limit safety."""
    closer = ConversationCloser()
    return closer.bulk_close(
        team_id=team_id,
        parallel_workers=parallel_workers,
        batch_size=batch_size,
        max_conversations=max_conversations
    )


# Alias for backward compatibility
def bulk_close_hybrid(team_id: str, parallel_workers: int = 15, batch_size: int = 50, max_conversations: int = None) -> Dict[str, int]:
    """Alias for bulk_close - maintains backward compatibility."""
    return bulk_close(team_id, parallel_workers, batch_size, max_conversations)


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    inbox_id = os.getenv("INTERCOM_INBOX_ID")
    
    if not inbox_id:
        print("❌ INTERCOM_INBOX_ID not found in environment variables")
        exit(1)
    
    print("=" * 60)
    print("🤖 Intercom Bulk Conversation Closer - Modular Version")
    print("=" * 60)
    
    # Example usage
    closer = ConversationCloser()
    
    print("🚀 FULL RUN - Processing all conversations...")
    print("⚡ Expected: ~3.3 conversations/second, ~2.6 hours total")
    
    result = closer.bulk_close(team_id=inbox_id, parallel_workers=15, batch_size=50)
    
    print(f"\n📊 Final Results:")
    print(f"   Successfully closed: {result['success']:,}")
    print(f"   Failed: {result['failed']:,}")
    print(f"   Total time: {result['total_time']}")
