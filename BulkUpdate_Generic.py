"""
Intercom Bulk Update Generic - Modular Version
==============================================

This file demonstrates how to use the modular IntercomBulkUpdater framework
for different types of bulk operations. It shows both the new modular approach
and provides backward compatibility with the original functions.

The modular approach separates:
- Search logic (what to find)
- Action logic (what to do with each item)
- Processing logic (how to handle batches and rate limits)

This makes it easy to create new bulk operations by extending the base class.
"""

import os
from dotenv import load_dotenv
from conversation_closer import ConversationCloser, bulk_close, bulk_close_hybrid

# Load environment variables
load_dotenv()

INBOX_ID = os.getenv("INTERCOM_INBOX_ID")

if not INBOX_ID:
    raise RuntimeError("Missing env var: set INTERCOM_INBOX_ID")

print(f"✅ Environment loaded - Inbox ID: {INBOX_ID}")


# =============================================================================
# MODULAR APPROACH - Using the new framework
# =============================================================================

def demonstrate_modular_approach():
    """Demonstrate the new modular approach using the ConversationCloser class."""
    print("🔧 Using the new modular approach...")
    
    # Create a conversation closer instance
    closer = ConversationCloser()
    
    # Example: Bulk processing with parallel batches
    print("\n1. Bulk Processing (Parallel with Rate Limit Safety):")
    result = closer.bulk_close(
        team_id=INBOX_ID, 
        parallel_workers=15, 
        batch_size=50, 
        max_conversations=100
    )
    print(f"   Result: {result}")


def demonstrate_backward_compatibility():
    """Demonstrate backward compatibility with original function names."""
    print("🔧 Using backward-compatible function names...")
    
    # These functions work exactly like the original script
    print("\n1. Using bulk_close_hybrid function:")
    result = bulk_close_hybrid(
        team_id=INBOX_ID,
        parallel_workers=15,
        batch_size=50,
        max_conversations=100
    )
    print(f"   Result: {result}")


# =============================================================================
# BACKWARD COMPATIBILITY - Original function names still work
# =============================================================================

# The original function names are still available for backward compatibility
# They now use the modular framework under the hood but maintain the same API


if __name__ == "__main__":
    print("=" * 60)
    print("🤖 Intercom Bulk Update Generic - Modular Version")
    print("=" * 60)
    print()
    print("This file demonstrates the simplified modular approach for bulk operations.")
    print("All operations now use parallel processing with rate limit safety.")
    print()
    print("Choose your approach:")
    print()
    print("1. Modular approach (recommended for new code)")
    print("2. Backward compatibility (for existing code)")
    print()
    
    # Uncomment one of these to run:
    
    # Option 1: Demonstrate the new modular approach
    # demonstrate_modular_approach()
    
    # Option 2: Demonstrate backward compatibility
    # demonstrate_backward_compatibility()
    
    # Option 3: Run the bulk processing (like the original script)
    print("🚀 Running bulk processing...")
    print("⚡ Processing conversations with parallel batches and rate limit safety...")
    result = bulk_close_hybrid(team_id=INBOX_ID, parallel_workers=15, batch_size=50)
    print(f"📊 Final result: {result}")
