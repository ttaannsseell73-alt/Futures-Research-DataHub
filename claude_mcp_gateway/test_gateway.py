import contextlib
import io
import json
import unittest

import httpx
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

import server

TOKEN = "tok-abcdef-1234567890"
ANON = "anon-jwt-secret-value"
SECRET = "bridge-secret-value"
ENV = {
    "CLAUDE_MCP_TOKEN": TOKEN,
    "SUPABASE_URL": "https://example.supabase.co",
    "SUPABASE_ANON_JWT": ANON,
    "V4_BRIDGE_SECRET": SECRET,
}
FULL = {"GATEWAY_MODE": "full", "MUTATION_GUARD": server.MUTATION_GUARD}
MUTATION_TOOLS = {
    "action_run", "project_run", "mkdir", "write_text", "patch_text", "copy_path", "move_path",
    "trash_path", "git_commit", "git_restore_file", "job_stop", "stop_all_motors", "conference_run",
}
DONE = {"completed": True, "command_id": "cmd-1", "result": {"ok": True}}


async def no_sleep(_seconds):
    return None


def make(handler, clock=None, **env):
    """Build a gateway whose upstream is an in-memory httpx.MockTransport."""
    calls = []

    def record(request):
        calls.append(request)
        return handler(request, len(calls))

    client = httpx.AsyncClient(transport=httpx.MockTransport(record))
    kwargs = {"clock": clock} if clock else {}
    app, gw = server.build({**ENV, **env}, client=client, sleep=no_sleep, **kwargs)
    return app, gw, calls


def body(request):
    return json.loads(request.content)


async def asgi(app, path, headers=(), query=b"", method="POST"):
    sent = []
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method,
        "scheme": "https", "path": path, "raw_path": path.encode(), "query_string": query,
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers],
        "server": ("test", 443), "client": ("client", 1),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    payload = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, payload


async def inner_ok(scope, receive, send):
    await JSONResponse({"inner": True})(scope, receive, send)


class AuthTests(unittest.IsolatedAsyncioTestCase):
    def app(self, **env):
        return server.GatewayASGI(inner_ok, server.Config.from_env({**ENV, **env}))

    async def test_missing_wrong_and_valid_token(self):
        app = self.app()
        self.assertEqual((await asgi(app, "/mcp"))[0], 401)
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", "Bearer wrong")]))[0], 401)
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", TOKEN)]))[0], 401)
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", "bearer " + TOKEN)]))[0], 401)
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", "Bearer " + TOKEN + " ")]))[0], 401)
        status, payload = await asgi(app, "/mcp", [("Authorization", "Bearer " + TOKEN)])
        self.assertEqual((status, json.loads(payload)), (200, {"inner": True}))

    async def test_query_string_credentials_rejected(self):
        app = self.app()
        self.assertNotEqual((await asgi(app, "/mcp", query=("token=" + TOKEN).encode()))[0], 200)
        status, _ = await asgi(app, "/mcp", [("Authorization", "Bearer " + TOKEN)], query=("access_token=" + TOKEN).encode())
        self.assertEqual(status, 400)

    async def test_unset_token_rejects_everything(self):
        app = self.app(CLAUDE_MCP_TOKEN="")
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", "Bearer ")]))[0], 401)

    async def test_healthz_is_open_and_reveals_no_secrets(self):
        status, payload = await asgi(self.app(), "/healthz", method="GET")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload), {"ok": True, "version": "1.0.0", "mode": "readonly", "upstream_configured": True})
        for secret in (TOKEN, ANON, SECRET):
            self.assertNotIn(secret.encode(), payload)

    async def test_off_mode_returns_503(self):
        app = self.app(GATEWAY_MODE="off")
        self.assertEqual((await asgi(app, "/mcp", [("Authorization", "Bearer " + TOKEN)]))[0], 503)
        self.assertEqual(json.loads((await asgi(app, "/healthz", method="GET"))[1])["mode"], "off")


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_main_omits_target_device(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json=DONE))
        out = await gw.tools["pc_status"]()
        self.assertEqual(out["status"], "completed")
        self.assertEqual(len(calls), 1)
        sent = body(calls[0])
        self.assertEqual(sent, {"action": "invoke", "command_type": "status", "payload": {}, "wait_seconds": 22})
        self.assertNotIn("x-device-name", calls[0].headers)
        self.assertNotIn("TANSEL", calls[0].content.decode())
        self.assertEqual(str(calls[0].url), "https://example.supabase.co/functions/v1/pc-bridge")
        self.assertEqual(calls[0].headers["authorization"], "Bearer " + ANON)
        self.assertEqual(calls[0].headers["apikey"], ANON)
        self.assertEqual(calls[0].headers["x-bridge-secret"], SECRET)
        self.assertEqual(calls[0].headers["content-type"], "application/json")

    async def test_second_maps_only_to_second_pc(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json=DONE))
        await gw.tools["file_read"](project="p", relative_path="a.txt", device="second")
        sent = body(calls[0])
        self.assertEqual(sent["target_device"], "SECOND-PC")
        self.assertEqual(sent["command_type"], "read")
        self.assertEqual(sent["payload"], {"project": "p", "relative_path": "a.txt", "max_bytes": 65536})

    async def test_unknown_device_rejected_without_upstream_call(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json=DONE))
        for device in ("TANSEL", "SECOND-PC", "", "Main"):
            out = await gw.tools["pc_status"](device=device)
            self.assertEqual(out["status"], "invalid_arguments")
        self.assertEqual(calls, [])

    async def test_forbidden_actions_never_sent(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json=DONE))
        for action in ("poll", "complete", "anything"):
            with self.assertRaises(server.UpstreamError):
                await gw.bridge.call(action, {})
        self.assertEqual(calls, [])


class ReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_invoke_202_polls_same_command_id_without_reenqueue(self):
        def handler(request, n):
            if n == 1:
                return httpx.Response(202, json={"completed": False, "command_id": "cmd-1"})
            if n == 2:
                return httpx.Response(200, json={"completed": False, "command_id": "cmd-1"})
            return httpx.Response(200, json=DONE)

        _, gw, calls = make(handler)
        out = await gw.tools["system_status"]()
        self.assertEqual(out["status"], "completed")
        self.assertEqual(out["command_id"], "cmd-1")
        self.assertEqual([body(c)["action"] for c in calls], ["invoke", "result", "result"])
        for call in calls[1:]:
            self.assertEqual(body(call), {"action": "result", "command_id": "cmd-1"})

    async def test_deadline_returns_pending_with_command_id(self):
        ticks = iter(range(0, 100000, 10))
        _, gw, calls = make(lambda request, n: httpx.Response(202, json={"completed": False, "command_id": "cmd-9"}),
                            clock=lambda: float(next(ticks)))
        out = await gw.tools["pc_status"]()
        self.assertEqual(out["status"], "pending")
        self.assertEqual(out["command_id"], "cmd-9")
        self.assertIs(out["retry"], False)
        actions = [body(c)["action"] for c in calls]
        self.assertEqual(actions.count("invoke"), 1)
        self.assertTrue(len(actions) > 1 and set(actions[1:]) == {"result"})

    async def test_command_result_single_result_call(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"completed": False, "command_id": "cmd-1"}))
        out = await gw.tools["command_result"](command_id="cmd-1")
        self.assertEqual(out["status"], "pending")
        self.assertEqual([body(c) for c in calls], [{"action": "result", "command_id": "cmd-1"}])
        bad = await gw.tools["command_result"](command_id="../x")
        self.assertEqual(bad["status"], "invalid_arguments")
        self.assertEqual(len(calls), 1)

    async def test_health_uses_health_action(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"ok": True}))
        out = await gw.tools["gateway_health"]()
        self.assertEqual(out["status"], "ok")
        self.assertEqual(body(calls[0]), {"action": "health"})

    async def test_response_truncation(self):
        big = {"completed": True, "command_id": "cmd-1", "result": "x" * (600 * 1024)}
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=big))
        out = await gw.tools["file_read"](project="p", relative_path="big.bin")
        self.assertEqual(out["status"], "completed")
        result = out["result"]
        self.assertIs(result["truncated"], True)
        self.assertGreater(result["original_bytes"], server.MAX_RESPONSE_BYTES)
        self.assertEqual(len(result["preview"]), server.PREVIEW_BYTES)
        self.assertLess(len(json.dumps(out)), server.MAX_RESPONSE_BYTES)

    async def test_upstream_echo_of_secret_is_redacted(self):
        leaky = {"completed": True, "command_id": "cmd-1", "result": "seen " + SECRET + " and " + ANON}
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=leaky))
        text = json.dumps(await gw.tools["pc_status"]())
        self.assertNotIn(SECRET, text)
        self.assertNotIn(ANON, text)

    async def test_upstream_error_status_and_bad_json(self):
        _, gw, _ = make(lambda request, n: httpx.Response(403, json={"error": "nope"}))
        out = await gw.tools["pc_status"]()
        self.assertEqual((out["status"], out["error"]), ("error", "upstream_http_403"))
        _, gw, _ = make(lambda request, n: httpx.Response(200, content=b"not json"))
        out = await gw.tools["pc_status"]()
        self.assertEqual((out["status"], out["error"]), ("error", "upstream_invalid_json"))

    async def test_rate_limit_burst(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"ok": True}), clock=lambda: 0.0)
        outcomes = [(await gw.tools["gateway_health"]())["status"] for _ in range(server.RATE_BURST + 1)]
        self.assertEqual(outcomes[:-1], ["ok"] * server.RATE_BURST)
        self.assertEqual(outcomes[-1], "rate_limited")
        self.assertEqual(len(calls), server.RATE_BURST)


class MutationTests(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_returns_queued_immediately(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"command_id": "cmd-7"}), **FULL)
        out = await gw.tools["write_text"](project="p", relative_path="a.txt", content="hello")
        self.assertEqual((out["status"], out["command_id"], out["retry"]), ("queued", "cmd-7", False))
        self.assertEqual(len(calls), 1)
        sent = body(calls[0])
        self.assertEqual(sent["action"], "enqueue")
        self.assertEqual(sent["command_type"], "write_text")
        self.assertNotIn("target_device", sent)
        self.assertNotIn("wait_seconds", sent)

    async def test_timeout_is_unknown_outcome_and_never_retried(self):
        def handler(request, n):
            raise httpx.ReadTimeout("timed out", request=request)

        _, gw, calls = make(handler, **FULL)
        out = await gw.tools["git_commit"](project="p", message="msg")
        self.assertEqual(out["status"], "UNKNOWN_OUTCOME")
        self.assertIs(out["retry"], False)
        self.assertIsNone(out["command_id"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(body(calls[0])["action"], "enqueue")

    async def test_5xx_is_unknown_and_4xx_is_definite_error(self):
        _, gw, calls = make(lambda request, n: httpx.Response(502, json={}), **FULL)
        self.assertEqual((await gw.tools["stop_all_motors"]())["status"], "UNKNOWN_OUTCOME")
        self.assertEqual(len(calls), 1)
        _, gw, calls = make(lambda request, n: httpx.Response(400, json={}), **FULL)
        self.assertEqual((await gw.tools["stop_all_motors"]())["status"], "error")
        self.assertEqual(len(calls), 1)

    async def test_connect_error_is_definite_error_without_retry(self):
        def handler(request, n):
            raise httpx.ConnectError("refused", request=request)

        _, gw, calls = make(handler, **FULL)
        out = await gw.tools["job_stop"](job_id="j1")
        self.assertEqual((out["status"], out["error"], out["retry"]), ("error", "upstream_unreachable", False))
        self.assertEqual(len(calls), 1)

    async def test_oversized_content_rejected_before_upstream(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"command_id": "cmd-7"}), **FULL)
        out = await gw.tools["write_text"](project="p", relative_path="a.txt", content="x" * (256 * 1024 + 1))
        self.assertEqual(out["status"], "invalid_arguments")
        self.assertEqual(calls, [])

    async def test_second_device_and_conference_payload(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"command_id": "cmd-7"}), **FULL)
        await gw.tools["conference_run"](payload={"topic": "t"}, device="second")
        sent = body(calls[0])
        self.assertEqual(sent, {"action": "enqueue", "command_type": "conference_run",
                                "payload": {"topic": "t"}, "target_device": "SECOND-PC"})

    async def test_mutate_refused_outside_full_mode(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json={"command_id": "cmd-7"}))
        out = await gw.mutate("mkdir", "mkdir", "main", {"project": "p", "relative_path": "d"})
        self.assertEqual(out["error"], "mutations_disabled")
        self.assertEqual(calls, [])


class ModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_readonly_excludes_mutation_tools(self):
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=DONE))
        self.assertEqual(gw.cfg.mode, "readonly")
        self.assertFalse(MUTATION_TOOLS & set(gw.tools))
        self.assertEqual(len(gw.tools), 21)
        self.assertIn("command_result", gw.tools)

    async def test_full_with_guard_includes_mutation_tools(self):
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=DONE), **FULL)
        self.assertEqual(gw.cfg.mode, "full")
        self.assertTrue(MUTATION_TOOLS <= set(gw.tools))
        self.assertEqual(len(gw.tools), 34)

    async def test_full_without_guard_fails_closed_to_readonly(self):
        for guard in ({}, {"MUTATION_GUARD": "yes"}, {"MUTATION_GUARD": ""}):
            _, gw, _ = make(lambda request, n: httpx.Response(200, json=DONE), GATEWAY_MODE="full", **guard)
            self.assertEqual(gw.cfg.mode, "readonly")
            self.assertFalse(MUTATION_TOOLS & set(gw.tools))

    async def test_off_and_unrecognised_mode_register_nothing(self):
        for mode in ("off", "bogus"):
            _, gw, _ = make(lambda request, n: httpx.Response(200, json=DONE), GATEWAY_MODE=mode)
            self.assertEqual(gw.cfg.mode, "off")
            self.assertEqual(gw.tools, {})

    async def test_unconfigured_upstream(self):
        _, gw, calls = make(lambda request, n: httpx.Response(200, json=DONE), V4_BRIDGE_SECRET="")
        self.assertFalse(gw.cfg.upstream_configured)
        out = await gw.tools["pc_status"]()
        self.assertEqual(out["error"], "upstream_not_configured")
        self.assertEqual(calls, [])


class AuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_audit_line_has_no_secrets_or_bodies(self):
        upstream = {"command_id": "cmd-7", "result": "RESULT-BODY-MARKER"}
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=upstream), **FULL)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            await gw.tools["write_text"](project="PROJECT-MARKER", relative_path="PATH-MARKER.txt", content="CONTENT-MARKER")
            await gw.tools["pc_status"](device="second")
        lines = buf.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        records = [json.loads(line) for line in lines]
        self.assertEqual(set(records[0]), {"timestamp", "request_id", "client", "mode", "tool", "device",
                                           "args_sha256", "command_id", "outcome", "latency_ms"})
        self.assertEqual((records[0]["tool"], records[0]["device"], records[0]["outcome"], records[0]["command_id"]),
                         ("write_text", "main", "queued", "cmd-7"))
        self.assertEqual((records[0]["client"], records[0]["mode"]), ("claude", "full"))
        self.assertEqual(len(records[0]["args_sha256"]), 64)
        self.assertEqual((records[1]["tool"], records[1]["device"]), ("pc_status", "second"))
        for marker in (TOKEN, ANON, SECRET, "PROJECT-MARKER", "PATH-MARKER", "CONTENT-MARKER", "RESULT-BODY-MARKER", "Bearer"):
            self.assertNotIn(marker, buf.getvalue())

    async def test_invalid_call_is_audited_without_value(self):
        _, gw, _ = make(lambda request, n: httpx.Response(200, json=DONE))
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            await gw.tools["pc_status"](device="BAD-DEVICE-MARKER")
        record = json.loads(buf.getvalue())
        self.assertEqual((record["outcome"], record["device"]), ("invalid_arguments", None))
        self.assertNotIn("BAD-DEVICE-MARKER", buf.getvalue())


class McpSmokeTests(unittest.TestCase):
    """End-to-end over the real MCP app (in-memory ASGI, no network). Validates the SDK wiring."""

    def test_mcp_initialize_and_list(self):
        app, _ = server.build(ENV)
        headers = {"Authorization": "Bearer " + TOKEN, "Accept": "application/json, text/event-stream",
                   "Content-Type": "application/json", "MCP-Protocol-Version": "2025-06-18"}
        init = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}}
        with TestClient(app) as client:
            self.assertEqual(client.post("/mcp", json=init).status_code, 401)
            response = client.post("/mcp", headers=headers, json=init)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["result"]["serverInfo"]["name"], "CLAUDE-MCP-GATEWAY")
            response = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            self.assertEqual(response.status_code, 200)
            names = {tool["name"] for tool in response.json()["result"]["tools"]}
            self.assertIn("pc_status", names)
            self.assertIn("command_result", names)
            self.assertFalse(MUTATION_TOOLS & names)


if __name__ == "__main__":
    unittest.main()
