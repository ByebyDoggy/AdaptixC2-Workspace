#!/bin/bash
# AdaptixC2 MCP server — single entry point.
#
# MCP clients (Claude Code and any other MCP client) launch this script over
# stdio. It resolves its own real path, so symlinking the script elsewhere is
# safe: /opt/adaptix-mcp/start-mcp.sh -> <repo>/mcp/start-mcp.sh
set -euo pipefail

SELF="$(readlink -f "$0")"
MCP_DIR="$(dirname "$SELF")"

ENV_FILE="${ADAPTIX_MCP_ENV:-/opt/adaptix-mcp/mcp.env}"
if [ -f "$ENV_FILE" ]; then
    # set -a exports every assignment from the env file; without it the
    # credentials stay shell-local and the python child sees an empty password.
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

: "${ADAPTIX_PASSWORD:?ADAPTIX_PASSWORD is not set (write it to $ENV_FILE, chmod 600)}"

export ADAPTIX_USERNAME="${ADAPTIX_USERNAME:-auto}"
export ADAPTIX_ENDPOINT="${ADAPTIX_ENDPOINT:-127.0.0.1:4321/endpoint}"
export ADAPTIX_PAYLOADS_DIR="${ADAPTIX_PAYLOADS_DIR:-/opt/projects/AdaptixC2-Workspace/src/teamserver/payloads}"
export ADAPTIX_SLOT_DIR="${ADAPTIX_SLOT_DIR:-/opt/adaptix-mcp/slots}"
export ADAPTIX_SLOT_COUNT="${ADAPTIX_SLOT_COUNT:-128}"
export PYTHONWARNINGS=ignore

MCP_PYTHON="${ADAPTIX_MCP_PYTHON:-/opt/adaptix-mcp/venv/bin/python}"
exec "$MCP_PYTHON" "$MCP_DIR/AdaptixC2_mcp.py"
