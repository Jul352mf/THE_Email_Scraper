# Google Trusted IP Automation

## Problem
Google Custom Search API requires trusted IPs to be registered in the Google Cloud Console. When running from different environments (home, VPS, cloud instances), the IP needs to be updated manually.

## Solutions

### 1. Google Cloud Shell Automation (Recommended)
Google Cloud Shell provides a stable environment with pre-configured authentication:

```bash
# In Google Cloud Shell
gcloud config set project YOUR_PROJECT_ID

# Get current external IP
CURRENT_IP=$(curl -s https://api.ipify.org)
echo "Current IP: $CURRENT_IP"

# Update API key restrictions (requires gcloud CLI with proper permissions)
gcloud services api-keys update YOUR_API_KEY_ID \
  --allowed-ips="$CURRENT_IP"
```

### 2. Programmatic API Key Management
Use Google Cloud API Key Management API:

```python
from google.cloud import apikeys_v2
from google.oauth2 import service_account
import requests

def update_api_key_ip_restrictions(project_id, key_id, new_ip):
    """Update API key IP restrictions programmatically."""
    
    # Initialize the API Key service client
    client = apikeys_v2.ApiKeysClient()
    
    # Get current key
    key_name = f"projects/{project_id}/locations/global/keys/{key_id}"
    key = client.get_key(name=key_name)
    
    # Update IP restrictions
    key.restrictions.api_targets[0].allowed_ips = [new_ip]
    
    # Update the key
    operation = client.update_key(key=key)
    result = operation.result()
    
    return result

# Usage
current_ip = requests.get('https://api.ipify.org').text
update_api_key_ip_restrictions('your-project-id', 'your-key-id', current_ip)
```

### 3. Multiple API Keys Strategy
Create multiple API keys for different environments:

```python
# In config.py
GOOGLE_API_KEYS = {
    'home': 'AIza...',  # Home IP restricted
    'vps': 'AIza...',   # VPS IP restricted  
    'cloud': 'AIza...', # Cloud IP restricted
}

def get_api_key_for_current_ip():
    """Get appropriate API key based on current IP."""
    current_ip = requests.get('https://api.ipify.org').text
    
    # IP to environment mapping
    ip_mappings = {
        '192.168.1.100': 'home',
        '203.0.113.50': 'vps',
        '198.51.100.25': 'cloud'
    }
    
    env = ip_mappings.get(current_ip, 'default')
    return GOOGLE_API_KEYS.get(env, GOOGLE_API_KEYS['home'])
```

### 4. Proxy/VPN Solution
Use a consistent exit IP through proxy or VPN:

```bash
# Using SSH tunnel as SOCKS proxy
ssh -D 8080 -C -q -N user@your-vps.com

# Configure requests to use proxy
import requests

proxies = {
    'http': 'socks5://localhost:8080',
    'https': 'socks5://localhost:8080'
}

response = requests.get(url, proxies=proxies)
```

### 5. IP Whitelist Expansion
For development, use broader IP ranges:

```
# Instead of specific IP: 203.0.113.50
# Use subnet: 203.0.113.0/24
# Or provider range: 203.0.0.0/16
```

## Implementation Priority

1. **Immediate**: Use multiple API keys for known environments
2. **Short-term**: Implement Google Cloud Shell automation
3. **Long-term**: Set up programmatic API key management

## Security Considerations

- Never commit API keys to version control
- Use environment variables or secure key management
- Regularly rotate API keys
- Monitor API usage for unauthorized access
- Consider using service accounts with domain-wide delegation

## Cost Optimization

- Each API key has separate quotas
- Monitor usage across all keys
- Implement caching to reduce API calls
- Use batch requests where possible

## Error Handling

```python
def handle_google_api_error(error):
    """Handle Google API authentication errors."""
    if 'IP address blocked' in str(error):
        log.error("Current IP not authorized. Please update API key restrictions.")
        current_ip = requests.get('https://api.ipify.org').text
        log.info(f"Current IP: {current_ip}")
        log.info("Update API key at: https://console.cloud.google.com/apis/credentials")
        return False
    return True
```

This approach provides multiple fallback options while maintaining security.