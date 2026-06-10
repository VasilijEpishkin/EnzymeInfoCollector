# fatsecret-mcp — Setup Guide

## Install & build

```bash
cd fatsecret-mcp
npm install
npm run build
```

## Claude Desktop config

File: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "fatsecret": {
      "command": "node",
      "args": ["/ABSOLUTE/PATH/TO/fatsecret-mcp/dist/index.js"],
      "env": {
        "CLIENT_ID": "your_fatsecret_client_id",
        "CLIENT_SECRET": "your_fatsecret_client_secret"
      }
    }
  }
}
```

Restart Claude Desktop after saving.

## Available tools (16 total)

### OAuth 1.0a tools (existing)
- `set_credentials` — set API credentials
- `start_oauth_flow` / `complete_oauth_flow` — 3-legged OAuth for user auth
- `check_auth_status`
- `search_foods`, `get_food`
- `search_recipes`, `get_recipe`
- `get_user_profile`, `get_user_food_entries`, `add_food_entry`
- `get_weight_month`

### OAuth 2.0 v2 tools (new, require premier scope)
- `search_foods_v2` — `foods.search.v3`, supports language/region filters
- `get_food_v2` — `food.get.v4`, includes vitamins & minerals
- `get_food_by_barcode` — `food.find_id_for_barcode`, EAN-13/UPC-A
- `search_exercises` — `exercises.get`, calorie estimates

OAuth 2.0 tokens are fetched automatically with `scope=basic premier`
and cached in `~/.fatsecret-mcp-config.json`.
