#!/usr/bin/env python3
"""Stdio smoke test for the merged MCP server.

Requires a running Adaptix teamserver and the production venv (mcp package).

    python3 tests/smoke_mcp.py
    python3 tests/smoke_mcp.py --call get_live_beacons --agent <agent_id>
"""
import argparse
import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

DEFAULT_LAUNCHER = "/opt/adaptix-mcp/start-mcp.sh"

PROBES = [
    "get_live_beacons",
    "bof_check",
    "list_directory",
    "execute_powershell",
    "log_finding",
]


async def run(launcher, calls, agent_id):
    params = StdioServerParameters(command=launcher, args=[], env=os.environ.copy())
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = sorted(t.name for t in tools.tools)
            print(f"tools: {len(names)}")
            for probe in PROBES:
                print(("  ok   " if probe in names else "  MISS "), probe)

            for name in calls:
                args = {"agent_id": agent_id} if (agent_id and name != "get_live_beacons") else {}
                result = await session.call_tool(name, args)
                text = "".join(getattr(item, "text", "") for item in result.content)
                print(f"\n[{name}] {text[:500]}")

            if agent_id:
                result = await session.call_tool("bof_check", {"agent_id": agent_id})
                text = "".join(getattr(item, "text", "") for item in result.content)
                print(f"\n[bof_check {agent_id}] {text[:500]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--launcher", default=DEFAULT_LAUNCHER)
    parser.add_argument("--call", action="append", default=[])
    parser.add_argument("--agent", default="")
    options = parser.parse_args()

    calls = options.call or ["get_live_beacons"]
    try:
        asyncio.run(run(options.launcher, calls, options.agent))
    except Exception as exc:
        print(f"SMOKE FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
