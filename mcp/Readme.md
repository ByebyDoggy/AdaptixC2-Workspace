# adaptix-mcp

MCP (Model Context Protocol) server for [AdaptixC2](https://github.com/Adaptix-Framework/AdaptixC2) — control your C2 infrastructure from Claude Code or any MCP-compatible client.

> Built for AdaptixC2 v1.2

https://github.com/user-attachments/assets/1b55434f-efd6-47e7-94a2-3eec493b73c9

## Features

- **50 MCP tools** covering the full AdaptixC2 API
- **Agent interaction** via raw console commands (works with any agent type — beacon, gopher, future agents)
- **Dynamic command discovery** — available commands fetched from the server, no hardcoded tool definitions
- **Real-time session tracking** via WebSocket with OTP authentication
- **Team chat** — read and send messages to other operators

### Tool Categories

| Category | Tools | Description |
|----------|-------|-------------|
| Sessions | `get_live_beacons`, `send_command`, `list_commands` | Track agents, execute commands, discover available commands |
| Listeners | `listener_list/create/edit/pause/resume/remove` | Full listener lifecycle management |
| Payloads | `agent_generate` | Build beacon/gopher agent binaries |
| Credentials | `creds_list/add/edit/remove/set_tag` | Credential store management |
| Targets | `targets_list/add/edit/remove/set_tag` | Target host tracking |
| Downloads | `downloads_list`, `download_sync`, `downloads_delete` | File exfiltration management |
| Screenshots | `screenshots_list/save/set_note/remove` | Screenshot capture management |
| Tunnels | `tunnel_start_socks5/socks4/lportfwd/rportfwd`, `tunnel_stop/set_info`, `tunnels_list` | SOCKS proxies and port forwarding |
| Tasks | `tasks_list/cancel/delete`, `task_hook/save` | Task history and management |
| Agent Mgmt | `agent_remove/set_tag/set_mark/set_color/update_data/console_remove` | Agent metadata and cleanup |
| Services | `service_list/call` | Plugin service interface |
| Chat | `chat_get_all/get_new/send` | Team communication |

## Installation

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager.

```bash
uv venv
uv pip install -r requirements.txt
```

## Configuration

Set credentials via environment variables or edit defaults in the script:

| Variable | Default | Description |
|----------|---------|-------------|
| `ADAPTIX_USERNAME` | `claude` | Operator username |
| `ADAPTIX_PASSWORD` | `claude` | Operator password |
| `ADAPTIX_ENDPOINT` | `127.0.0.1:4321/endpoint` | AdaptixC2 server endpoint |
| `ADAPTIX_PAYLOADS_DIR` | `./payloads` | Directory for generated payloads and downloads |

## Usage with Claude Code

Add to your project's `.mcp.json`:

```json
{
  "mcpServers": {
    "adaptix-c2": {
      "command": "uv",
      "args": ["--directory", "/path/to/adaptix-mcp", "run", "AdaptixC2_mcp.py"],
      "env": {
        "ADAPTIX_USERNAME": "your-operator",
        "ADAPTIX_PASSWORD": "your-password",
        "ADAPTIX_ENDPOINT": "127.0.0.1:4321/endpoint"
      }
    }
  }
}
```

## Credits

Forked from [0xb11a1/AdaptixC2_mcp](https://github.com/0xb11a1/AdaptixC2_mcp)

Inspired by:
- [TrustedSec: Teaching a New Dog Old Tricks](https://trustedsec.com/blog/teaching-a-new-dog-old-tricks-phishing-with-mcp)
- [@_xpn_](https://x.com/_xpn_/status/1902848551463399504)
