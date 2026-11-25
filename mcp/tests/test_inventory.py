#!/usr/bin/env python3
"""Tool inventory assertions for the merged single-file MCP server."""
import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "AdaptixC2_mcp.py"

BOF_TOOLS = [
    "bof_ad", "bof_adcs", "bof_creds", "bof_elevation", "bof_execution",
    "bof_injection", "bof_kerbeus", "bof_ldap", "bof_lateral", "bof_mssql",
    "bof_postex", "bof_process", "bof_relayinformer", "bof_sal", "bof_sar",
    "bof_check",
]
FS_TOOLS = [
    "list_directory", "get_working_directory", "read_file", "change_directory",
    "copy_file", "move_file", "make_directory", "remove_file", "download_file",
    "list_disks",
]
EXEC_TOOLS = ["execute_powershell", "set_agent_sleep", "jobs_list", "jobs_kill"]
REPORT_TOOLS = ["log_finding", "save_writeup", "view_session_notes"]

EXPECTED_TOTAL = 85


def tool_names(tree):
    names = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for deco in node.decorator_list:
                if "tool" in ast.unparse(deco).lower():
                    names.append(node.name)
    return names


def main():
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    names = tool_names(tree)
    failures = []

    if len(names) != EXPECTED_TOTAL:
        failures.append(f"expected {EXPECTED_TOTAL} tools, found {len(names)}")

    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        failures.append(f"duplicate tool names: {dupes}")

    for group, expected in (
        ("bof", BOF_TOOLS),
        ("filesystem", FS_TOOLS),
        ("execution", EXEC_TOOLS),
        ("reporting", REPORT_TOOLS),
    ):
        missing = [n for n in expected if n not in names]
        if missing:
            failures.append(f"missing {group} tools: {missing}")

    if failures:
        print("FAIL")
        for item in failures:
            print(" -", item)
        return 1

    print(f"OK: {len(names)} tools, {len(BOF_TOOLS)} BOF, "
          f"{len(FS_TOOLS)} filesystem, {len(EXEC_TOOLS)} execution, "
          f"{len(REPORT_TOOLS)} reporting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
