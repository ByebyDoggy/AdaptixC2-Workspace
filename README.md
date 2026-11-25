# AdaptixC2 Workspace

Umbrella repository for the AdaptixC2 fork and the merged AdaptixC2 MCP server.

## Layout

| Path | What |
|---|---|
| `AdaptixC2/` | Submodule — [ByebyDoggy/AdaptixC2](https://github.com/ByebyDoggy/AdaptixC2): teamserver, client, extenders, contracts |
| `mcp/` | Merged single-file MCP server (85 tools) — see `mcp/README.md` |

## MCP server

The production deployment uses symlinks so there is exactly one copy of the
code and one launch entry point:

```
/opt/adaptix-mcp/start-mcp.sh  ->  <repo>/mcp/start-mcp.sh
/opt/adaptix-mcp/src           ->  <repo>/mcp
```

MCP client configuration:

```json
{
  "mcpServers": {
    "adaptix-c2": {
      "command": "/opt/adaptix-mcp/start-mcp.sh",
      "args": []
    }
  }
}
```

Operator credentials are read from `/opt/adaptix-mcp/mcp.env` (chmod 600,
never committed); `ADAPTIX_USERNAME=auto` allocates a free `mcpNNN` slot.
