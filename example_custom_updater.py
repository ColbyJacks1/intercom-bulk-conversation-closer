"""
Example Custom Updater - How to Create New Bulk Operations
=========================================================

This file demonstrates how to create custom bulk operations by extending
the IntercomBulkUpdater base class. You can use this as a template for
creating new bulk update operations.

Examples included:
1. Tag Assignment Updater
2. Conversation State Changer
3. Custom Field Updater
"""

import requests
from intercom_bulk_updater import IntercomBulkUpdater
from typing import Dict, Any, Optional


class TagAssignmentUpdater(IntercomBulkUpdater):
    """
    Example: Bulk assign tags to conversations.
    
    This shows how to create a custom updater that assigns tags to
    conversations matching certain criteria.
    """
    
    def get_search_endpoint(self) -> str:
        return "conversations/search"
    
    def get_search_query(self, team_id: str = None, state: str = "open", **kwargs) -> Dict[str, Any]:
        """Search for conversations to tag."""
        if not team_id:
            raise ValueError("team_id is required")
        
        return {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": team_id},
                {"field": "state", "operator": "=", "value": state}
            ]
        }
    
    def get_item_id(self, item: Dict[str, Any]) -> str:
        return item["id"]
    
    def perform_action(self, conversation_id: str, tags: list = None, **kwargs) -> Optional[Dict[str, Any]]:
        """Assign tags to a conversation."""
        if not tags:
            print(f"⚠️  No tags provided for conversation {conversation_id}")
            return None
        
        url = f"{self.base_url}/conversations/{conversation_id}/tags"
        payload = {
            "id": conversation_id,
            "tags": [{"id": tag_id} for tag_id in tags]
        }
        
        try:
            resp = requests.post(url, headers=self.headers, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Failed to tag conversation {conversation_id}: {e}")
            return None


class ConversationStateChanger(IntercomBulkUpdater):
    """
    Example: Bulk change conversation states.
    
    This shows how to create a custom updater that changes conversation
    states (e.g., from open to snoozed, or from snoozed to closed).
    """
    
    def get_search_endpoint(self) -> str:
        return "conversations/search"
    
    def get_search_query(self, team_id: str = None, current_state: str = "open", **kwargs) -> Dict[str, Any]:
        """Search for conversations in a specific state."""
        if not team_id:
            raise ValueError("team_id is required")
        
        return {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": team_id},
                {"field": "state", "operator": "=", "value": current_state}
            ]
        }
    
    def get_item_id(self, item: Dict[str, Any]) -> str:
        return item["id"]
    
    def perform_action(self, conversation_id: str, new_state: str = "closed", **kwargs) -> Optional[Dict[str, Any]]:
        """Change conversation state."""
        url = f"{self.base_url}/conversations/{conversation_id}"
        payload = {
            "id": conversation_id,
            "state": new_state
        }
        
        try:
            resp = requests.put(url, headers=self.headers, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Failed to change state for conversation {conversation_id}: {e}")
            return None


class CustomFieldUpdater(IntercomBulkUpdater):
    """
    Example: Bulk update custom fields on conversations.
    
    This shows how to create a custom updater that updates custom fields
    on conversations based on certain criteria.
    """
    
    def get_search_endpoint(self) -> str:
        return "conversations/search"
    
    def get_search_query(self, team_id: str = None, **kwargs) -> Dict[str, Any]:
        """Search for conversations to update."""
        if not team_id:
            raise ValueError("team_id is required")
        
        return {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": team_id}
            ]
        }
    
    def get_item_id(self, item: Dict[str, Any]) -> str:
        return item["id"]
    
    def perform_action(self, conversation_id: str, custom_fields: Dict[str, Any] = None, **kwargs) -> Optional[Dict[str, Any]]:
        """Update custom fields on a conversation."""
        if not custom_fields:
            print(f"⚠️  No custom fields provided for conversation {conversation_id}")
            return None
        
        url = f"{self.base_url}/conversations/{conversation_id}"
        payload = {
            "id": conversation_id,
            "custom_attributes": custom_fields
        }
        
        try:
            resp = requests.put(url, headers=self.headers, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Failed to update custom fields for conversation {conversation_id}: {e}")
            return None


# Example usage functions
def example_tag_assignment():
    """Example: Assign tags to all open conversations in a team."""
    updater = TagAssignmentUpdater()
    
    # Assign tags to conversations
    result = updater.bulk_process(
        team_id="your_team_id_here",
        parallel_workers=10,
        batch_size=25,
        tags=["urgent", "follow-up"]  # These will be passed to perform_action
    )
    
    print(f"Tag assignment complete: {result}")


def example_state_change():
    """Example: Change conversation states from open to snoozed."""
    updater = ConversationStateChanger()
    
    # Change states
    result = updater.bulk_process(
        team_id="your_team_id_here",
        parallel_workers=10,
        batch_size=25,
        current_state="open",
        new_state="snoozed"  # These will be passed to perform_action
    )
    
    print(f"State change complete: {result}")


def example_custom_field_update():
    """Example: Update custom fields on conversations."""
    updater = CustomFieldUpdater()
    
    # Update custom fields
    result = updater.bulk_process(
        team_id="your_team_id_here",
        parallel_workers=10,
        batch_size=25,
        custom_fields={
            "priority": "high",
            "category": "support",
            "last_reviewed": "2024-01-15"
        }
    )
    
    print(f"Custom field update complete: {result}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 Example Custom Updaters")
    print("=" * 60)
    print()
    print("This file contains examples of how to create custom bulk updaters.")
    print("To use them, uncomment and modify the example functions below.")
    print()
    print("Available examples:")
    print("1. TagAssignmentUpdater - Assign tags to conversations")
    print("2. ConversationStateChanger - Change conversation states")
    print("3. CustomFieldUpdater - Update custom fields")
    print()
    print("To run an example, uncomment the corresponding function call below:")
    print()
    print("# example_tag_assignment()")
    print("# example_state_change()")
    print("# example_custom_field_update()")


