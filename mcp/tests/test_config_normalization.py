#!/usr/bin/env python3
"""Pure unit tests for config parsing/coercion. No server, no network."""
import json
import os
import pathlib
import sys

os.environ.setdefault("ADAPTIX_USERNAME", "claude")
os.environ.setdefault("ADAPTIX_PASSWORD", "unused")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import AdaptixC2_mcp as m  # noqa: E402

FAILS = []


def check(name, cond, detail=""):
    if cond:
        print("  ok   %s" % name)
    else:
        FAILS.append(name)
        print("  FAIL %s %s" % (name, detail))


def http_cfg():
    return {
        "host_bind": "0.0.0.0",
        "port_bind": 8080,
        "callback_addresses": ["c2.example.com:80"],
        "encrypt_key": "00112233445566778899aabbccddeeff",
        "http_method": "POST",
        "uri": ["/api"],
        "hb_header": "X-Beacon-Id",
        "user_agent": ["Mozilla/5.0"],
        "page-payload": "<html><<<PAYLOAD_DATA>>></html>",
    }


def norm(ltype, cfg, **kwargs):
    return m._normalize_listener_config(ltype, cfg, **kwargs)


def test_parsing():
    print("[parsing]")
    data, _, err, _ = norm("BeaconHTTP", http_cfg())
    check("dict config", not err and data["port_bind"] == 8080, str(err))
    data, _, err, _ = norm("BeaconHTTP", json.dumps(http_cfg()))
    check("json string config", not err and data["uri"] == ["/api"], str(err))
    data, w, err, _ = norm("BeaconHTTP", json.dumps(json.dumps(http_cfg())))
    check("double-encoded config", not err and any("decoded" in x for x in w), str(err))
    data, w, err, _ = norm("BeaconHTTP", {"config": http_cfg()})
    check("wrapper config", not err and any("unwrapped" in x for x in w), str(err))
    data, w, err, _ = norm("BeaconHTTP", str(http_cfg()))
    check("python literal config", not err, str(err))
    _, _, err, _ = norm("BeaconHTTP", "{not json")
    check("invalid json rejected", bool(err) and "not valid JSON" in err[0], str(err))
    _, _, err, _ = norm("BeaconHTTP", '["a"]')
    check("array rejected", bool(err), str(err))
    _, _, err, _ = norm("BeaconHTTP", "")
    check("empty rejected", bool(err), str(err))


def test_coercion():
    print("[coercion]")
    cfg = http_cfg()
    cfg["uri"] = "/scalar"
    cfg["user_agent"] = "UA"
    data, _, err, _ = norm("BeaconHTTP", cfg)
    check("scalar -> list", not err and data["uri"] == ["/scalar"] and data["user_agent"] == ["UA"], str(err))

    cfg = http_cfg()
    cfg["port_bind"] = "8443"
    cfg["ssl"] = "false"
    data, _, err, _ = norm("BeaconHTTP", cfg)
    check("int/bool strings coerced", not err and data["port_bind"] == 8443 and data["ssl"] is False, str(err))

    cfg = http_cfg()
    cfg["encrypt_key"] = cfg["encrypt_key"].upper()
    data, w, err, _ = norm("BeaconHTTP", cfg)
    check("uppercase key lowered", not err and data["encrypt_key"].islower(), str(err))

    cfg = http_cfg()
    cfg["uris"] = ["/typo"]
    data, w, err, _ = norm("BeaconHTTP", cfg)
    check("unknown field warns + passthrough", not err and "uris" in data and any("did you mean 'uri'" in x for x in w), str(err))

    cfg = http_cfg()
    cfg["callback_servers"] = ["c2.example.com:80"]
    del cfg["callback_addresses"]
    data, w, err, _ = norm("BeaconHTTP", cfg)
    check("callback_servers alias", not err and data["callback_addresses"] == ["c2.example.com:80"], str(err))


def test_callbacks():
    print("[callbacks]")
    cfg = {"host_bind": "0.0.0.0", "port_bind": 9091, "encrypt_key": "0" * 32, "timeout": 10}
    _, _, err, _ = norm("GopherTCP", cfg)
    check("missing callback errors", bool(err) and "callback_addresses" in err[0] and "callback_from" in err[0], str(err))

    def lookup(name, kind):
        return (["c2.example.com:80"] if kind == "strlist" else "c2.example.com:80"), "stub", None

    data, w, err, _ = norm("GopherTCP", cfg, callback_from="gopher1", lookup=lookup)
    check("callback_from inheritance", not err and data["callback_addresses"] == "c2.example.com:80", str(err))

    cfg2 = dict(cfg)
    cfg2["callback_addresses"] = "gopher1"
    data, w, err, _ = norm("GopherTCP", cfg2, lookup=lookup)
    check("bare listener name inheritance", not err and data["callback_addresses"] == "c2.example.com:80", str(err))

    cfg3 = {"host_bind": "0.0.0.0", "port_bind": 9091, "encrypt_key": "0" * 32,
            "callback_addresses": ["a:1", "b:2"], "timeout": 10}
    data, _, err, _ = norm("GopherTCP", cfg3)
    check("gopher list -> newline text", not err and data["callback_addresses"] == "a:1\nb:2", str(err))

    base = http_cfg()
    data, _, err, _ = norm("BeaconHTTP", {"uri": ["/new"]}, merge_base=base, source="edit")
    check("edit patch keeps other fields",
          not err and data["uri"] == ["/new"] and data["user_agent"] == ["Mozilla/5.0"] and data["page-payload"], str(err))

    _, _, err, _ = norm("BeaconHTTP", {"callback_addresses": ""}, merge_base=base, source="edit")
    check("edit cannot clear callback", bool(err) and "cannot clear" in err[0], str(err))


def test_examples():
    print("[registry examples]")
    for ltype, schema in m.LISTENER_SCHEMAS.items():
        _, _, err, _ = norm(ltype, schema["example"])
        check("%s example valid" % ltype, not err, str(err))
    for atype, schema in m.AGENT_SCHEMAS.items():
        _, _, err, _ = m._normalize_agent_config(atype, schema["example"])
        check("%s agent example valid" % atype, not err, str(err))


def test_agent():
    print("[agent config]")
    data, w, err, _ = m._normalize_agent_config("beacon", {
        "os": "Windows", "arch": "x64", "format": "exe", "sleep": 5, "jitter": "20",
        "kill_date": "2027-01-01", "kill_time": "12:00",
    })
    check("agent coercion", not err and data["os"] == "windows" and data["format"] == "Exe"
          and data["sleep"] == "5s" and data["jitter"] == 20, str(err))
    check("kill date converted", data.get("kill_date") == "01.01.2027", str(data.get("kill_date")))
    check("kill time converted", data.get("kill_time") == "12:00:00", str(data.get("kill_time")))

    _, _, err, _ = m._normalize_agent_config("gopher", {"os": "linux"})
    check("agent required arch", bool(err) and "arch" in err[0], str(err))


def test_listener_names():
    print("[listener names]")
    check("comma names", m._normalize_listener_names("a,b, a")[0] == ["a", "b"])
    check("json array names", m._normalize_listener_names('["a","b"]')[0] == ["a", "b"])
    check("list names", m._normalize_listener_names(["a", "b"])[0] == ["a", "b"])
    records = [{"l_name": "g1", "l_reg_name": "GopherTCP"}, {"l_name": "h1", "l_reg_name": "BeaconHTTP"}]
    errs, _ = m._check_agent_listener_compatibility("beacon", records, ["g1"])
    check("beacon rejects gopher listener", bool(errs), str(errs))
    errs, _ = m._check_agent_listener_compatibility("beacon", records, ["h1"])
    check("beacon accepts http listener", not errs, str(errs))
    errs, _ = m._check_agent_listener_compatibility("beacon", records, ["missing"])
    check("missing listener rejected", bool(errs), str(errs))


def test_help_and_errors():
    print("[help/errors]")
    help_text = m._render_listener_help("create")
    check("help lists required callback", "callback_addresses (strlist, required)" in help_text)
    check("help lists page-payload", "page-payload" in help_text)
    check("help shows example", '"host_bind": "0.0.0.0"' in help_text)
    check("agent help compatibility", "gopher -> GopherTCP" in m._render_agent_help())

    msg = m._translate_backend_error("cannot unmarshal JSON string into TransportConfig.uri of Go type []string")
    check("unmarshal translated", "field 'uri' expects a list of strings" in msg, msg[:120])
    msg = m._translate_backend_error("callback_servers is required")
    check("callback_servers translated", "callback_addresses is required" in msg, msg[:120])

    fmt = m._format_config_errors("BeaconHTTP", ["missing required field 'uri'"], [], m._minimal_example("BeaconHTTP"))
    check("error report has example", "Minimal working example" in fmt and '"host_bind"' in fmt)


def main():
    test_parsing()
    test_coercion()
    test_callbacks()
    test_examples()
    test_agent()
    test_listener_names()
    test_help_and_errors()
    print()
    if FAILS:
        print("FAILED: %d" % len(FAILS))
        for item in FAILS:
            print(" -", item)
        return 1
    print("OK: all normalization checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
