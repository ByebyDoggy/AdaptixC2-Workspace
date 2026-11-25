# AdaptixC2 MCP Server (merged)

Single-file MCP server for [AdaptixC2](https://github.com/Adaptix-Framework/AdaptixC2),
based on the production `AdaptixC2_mcp.py` (52 tools) with the Windows/BOF tool
groups merged from [Faceless0x7/AdaptixC2-MCP-Server](https://github.com/Faceless0x7/AdaptixC2-MCP-Server).

## Tool inventory (85)

| Group | Count | Notes |
|---|---|---|
| Sessions, listeners, payloads, creds, targets, downloads, screenshots, tunnels, tasks, chat, services, scripts | 52 | original production toolset |
| `bof_*` groups (AD, ADCS, Creds, Elevation, Execution, Injection, Kerbeus, LDAP, Lateral, MSSQL, PostEx, Process, RelayInformer, SAL, SAR) | 15 | Windows beacon + Extension-Kit axscripts |
| `bof_check` | 1 | verifies Extension-Kit commands are registered |
| Windows filesystem (`list_directory` … `list_disks`) | 10 | beacon only |
| Windows execution (`execute_powershell`, `set_agent_sleep`, `jobs_list`, `jobs_kill`) | 4 | `set_agent_sleep` works on any agent |
| Reporting (`log_finding`, `save_writeup`, `view_session_notes`) | 3 | local notes on the MCP host |

## Single entry point

```
/opt/adaptix-mcp/start-mcp.sh  ->  <repo>/mcp/start-mcp.sh
/opt/adaptix-mcp/src           ->  <repo>/mcp
```

`start-mcp.sh` resolves its own real path, so symlinks are safe. It sources
`/opt/adaptix-mcp/mcp.env` (chmod 600, not committed) and execs the server with
the operator credentials.

MCP client config:

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

## Operator slots

`ADAPTIX_USERNAME=auto` uses `flock` on `$ADAPTIX_SLOT_DIR/slotNNN.lock` to hand
each MCP process a distinct operator account (`mcp001` … `mcp128`), avoiding the
single-session-per-operator conflict.

## Command output routing

Console output is captured per agent (`a_id`) with one lock per agent, so
concurrent tool calls and background agent traffic can never be mixed into
another command's result.

## Tests

```bash
python3 -m py_compile AdaptixC2_mcp.py
python3 tests/test_inventory.py
python3 tests/smoke_mcp.py            # needs a running teamserver + launcher
```

## Provenance

- Base: production single-file server (fork lineage of `0xb11a1/AdaptixC2_mcp`).
- Windows/BOF tool descriptions and command formats: `Faceless0x7/AdaptixC2-MCP-Server`
  (BSD-3-Clause, see upstream repository).
