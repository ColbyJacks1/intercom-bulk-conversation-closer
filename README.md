# Intercom Bulk Updater - Modular Framework

A reusable framework for performing bulk operations on Intercom data. The framework separates search logic, action logic, and processing logic to make it easy to create new bulk operations.

## 🏗️ Architecture

### Core Components

1. **`intercom_bulk_updater.py`** - Base class with reusable search and batching logic
2. **`conversation_closer.py`** - Specific implementation for closing conversations
3. **`example_custom_updater.py`** - Examples of how to create custom bulk operations
4. **`BulkUpdate_Generic.py`** - Updated generic file using the modular approach

### Key Features

- **Modular Design**: Separate search, action, and processing logic
- **Optimized Processing**: Parallel processing with rate limit safety
- **Rate Limiting**: Built-in rate limit handling and retry logic
- **Parallel Processing**: Thread-based parallel processing for high throughput
- **Backward Compatibility**: Original function names still work
- **Extensible**: Easy to create new bulk operations by extending the base class

## 🚀 Quick Start

### Using Pre-built Implementations

```python
from conversation_closer import ConversationCloser

# Create a conversation closer
closer = ConversationCloser()

# Close conversations using parallel processing with rate limit safety
result = closer.bulk_close(
    team_id="your_team_id",
    parallel_workers=15,
    batch_size=50,
    max_conversations=1000
)
```

### Using Backward Compatible Functions

```python
from conversation_closer import bulk_close_hybrid

# Works exactly like the original script
result = bulk_close_hybrid(
    team_id="your_team_id",
    parallel_workers=15,
    batch_size=50
)
```

## 🔧 Creating Custom Bulk Operations

### Step 1: Extend the Base Class

```python
from intercom_bulk_updater import IntercomBulkUpdater
from typing import Dict, Any, Optional

class CustomUpdater(IntercomBulkUpdater):
    def get_search_endpoint(self) -> str:
        return "conversations/search"  # or "contacts/search", etc.
    
    def get_search_query(self, **kwargs) -> Dict[str, Any]:
        return {
            "operator": "AND",
            "value": [
                {"field": "team_assignee_id", "operator": "=", "value": kwargs["team_id"]},
                {"field": "state", "operator": "=", "value": "open"}
            ]
        }
    
    def get_item_id(self, item: Dict[str, Any]) -> str:
        return item["id"]
    
    def perform_action(self, item_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        # Your custom action here
        url = f"{self.base_url}/conversations/{item_id}/parts"
        payload = {
            "message_type": "close",
            "type": "admin",
            "admin_id": self.admin_id
        }
        
        resp = requests.post(url, headers=self.headers, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()
```

### Step 2: Use Your Custom Updater

```python
updater = CustomUpdater()

# Use the optimized processing mode
result = updater.bulk_process(
    team_id="your_team_id",
    parallel_workers=10,
    batch_size=25
)
```

## 📊 Processing Mode

### Optimized Processing (Single Mode)
- **Parallel processing** with rate limit safety
- **Automatic retry logic** with exponential backoff
- **429 response handling** with proper backoff
- **Streaming approach** - processes items as they're found
- **Progress tracking** with success/failure rates
- **Configurable parameters** for different scenarios

This single mode provides the best balance of speed and safety for most use cases.

## 🔍 Search Capabilities

The framework supports searching any Intercom API endpoint that supports pagination:

- **Conversations**: Search by team, state, assignee, etc.
- **Contacts**: Search by custom attributes, tags, etc.
- **Companies**: Search by custom attributes, tags, etc.
- **Any other paginated endpoint**

## ⚙️ Configuration

### Environment Variables

```bash
INTERCOM_ACCESS_TOKEN=your_access_token
INTERCOM_ADMIN_ID=your_admin_id
INTERCOM_INBOX_ID=your_inbox_id  # Optional, for conversation operations
```

### Processing Parameters

- **`parallel_workers`**: Number of concurrent threads (default: 15)
- **`batch_size`**: Items per batch (default: 50)
- **`max_items`**: Maximum items to process (optional)
- **`timeout`**: Request timeout in seconds (default: 30)

## 📝 Examples

See `example_custom_updater.py` for complete examples of:

- Tag assignment updater
- Conversation state changer
- Custom field updater

## 🔄 Migration from Original Script

The original script functions are still available for backward compatibility:

```python
# Old way (still works)
from conversation_closer import bulk_close_hybrid
result = bulk_close_hybrid(team_id="12345")

# New way (recommended)
from conversation_closer import ConversationCloser
closer = ConversationCloser()
result = closer.bulk_close(team_id="12345")
```

## 🛠️ Development

### Adding New Operations

1. Create a new class extending `IntercomBulkUpdater`
2. Implement the four required methods:
   - `get_search_endpoint()`
   - `get_search_query()`
   - `get_item_id()`
   - `perform_action()`
3. Use the optimized processing mode

### Testing

```python
# Test with small batches first
result = updater.bulk_process(
    team_id="test_team",
    parallel_workers=2,
    batch_size=5,
    max_items=10
)
```

## 📈 Performance Tips

1. **Start Small**: Test with small batches and limits first
2. **Monitor Rate Limits**: Watch for 429 responses and adjust accordingly
3. **Tune Parameters**: Adjust `parallel_workers` and `batch_size` based on your needs
4. **Handle Failures**: The framework handles retries, but monitor success rates
5. **Streaming Processing**: The framework processes items as they're found, reducing memory usage

## 🚨 Important Notes

- Always test with small batches first
- Monitor Intercom's rate limits
- The framework handles retries automatically
- Failed items are logged but don't stop processing
- Use appropriate timeouts for your network conditions
