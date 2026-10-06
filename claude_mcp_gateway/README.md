# CLAUDE-MCP-GATEWAY 1.0.0

Remote MCP gateway for the Claude custom connector.

    Claude custom connector -> CLAUDE-MCP-GATEWAY (Render) -> Supabase Edge Function pc-bridge v3 -> TANSEL V4 queue/RPC -> Windows bridge

The gateway only adds typed tools, auth, limits and audit in front of pc-bridge. Existing V4 restrictions stay authoritative. **Never modify the existing V4 / Supabase components for this gateway.**

## VERIFICATION STATUS - READ FIRST

This code was written without being executed: the authoring environment did not allow running commands, and the only SDK available to read was mcp 1.30.0, not 2.2.0. Before deploying:

1. Run `pip install -r requirements.txt` on Python 3.12. If pip reports a conflict, the `starlette` / `uvicorn` / `httpx` pins (taken from a working mcp 1.30.0 environment) need adjusting to what mcp 2.2.0 requires.
2. Run `python -m unittest -v test_gateway`. `test_mcp_initialize_and_list` is the check that the three SDK calls used here match mcp 2.2.0: `from mcp.server import MCPServer`, `mcp.tool()`, and `mcp.streamable_http_app(streamable_http_path=, json_response=, stateless_http=, transport_security=)`.
3. Confirm the pc-bridge wire format assumptions below against the pc-bridge v3 source.

### Assumed pc-bridge request/response shape

The field names come from the task description; the envelope itself was not available to check.

- Request body: `{"action": "invoke"|"enqueue", "command_type": ..., "payload": {...}, "wait_seconds": 22 (invoke only), "target_device": "SECOND-PC" (second only)}`
- Result request: `{"action": "result", "command_id": ...}`; health request: `{"action": "health"}`
- Response: JSON object with a top-level `command_id` (or `id`) and `completed`. A command is treated as finished when HTTP is 200, `completed` is not false, and `status` is not one of queued/pending/claimed/running/processing.
- Payload keys are the tool argument names (`project`, `relative_path`, `job_id`, `lines`, ...). `conference_run` sends its `payload` dict as the command payload.
- Mutation `command_type` names are assumed equal to the tool names (`action_run`, `project_run`, `mkdir`, `write_text`, `patch_text`, `copy_path`, `move_path`, `trash_path`, `git_commit`, `git_restore_file`, `job_stop`, `stop_all_motors`, `conference_run`).

If any of these differ, change `command_body`, `_cid`, `_done` or the tool bodies in `server.py`.

## Render

Environment variables:

| Name | Value |
| --- | --- |
| `CLAUDE_MCP_TOKEN` | long random secret, shared only with the Claude connector |
| `SUPABASE_URL` | `https://weudparfakeqixwteuvx.supabase.co` |
| `SUPABASE_ANON_JWT` | Supabase anon JWT |
| `V4_BRIDGE_SECRET` | V4 bridge secret |
| `GATEWAY_MODE` | `readonly` |
| `MUTATION_GUARD` | leave unset initially |

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT`
- Python: 3.12

## Claude custom connector

- Connector URL: `https://<service>.onrender.com/mcp`
- Request header: `Authorization` = `Bearer <CLAUDE_MCP_TOKEN>`

`/mcp` requires exactly one `Authorization: Bearer <token>` header (constant-time compare). Requests carrying credential-like query parameters (`token`, `access_token`, `apikey`, `key`, ...) are rejected with 400. If `CLAUDE_MCP_TOKEN` is unset every `/mcp` request gets 401.

## Modes

`GATEWAY_MODE` is read once at startup; there is no runtime toggle.

| Mode | Behaviour |
| --- | --- |
| `readonly` (default) | read-only tools only |
| `full` | read-only plus mutation tools; requires `MUTATION_GUARD=I_UNDERSTAND_DUPLICATE_RISK`, otherwise the gateway starts as `readonly` |
| `off` | `/mcp` returns 503, no tools registered |

An unrecognised `GATEWAY_MODE` value starts the gateway as `off`.

**FULL mode is NOT recommended until durable dedupe/audit is added.** pc-bridge has no client idempotency key, and V1 keeps no durable audit or dedupe state, so the gateway cannot prove whether a mutation whose HTTP outcome was ambiguous was queued. The gateway never retries an enqueue, but a client that repeats the call can still produce a duplicate.

## Tools

Every tool returns a JSON object with `status` and `request_id`. `device` is `main` (default) or `second`; anything else is rejected. `main` sends no device field at all, so `target_device` stays null; `second` sends `target_device="SECOND-PC"`. Clients cannot pass a raw `command_type` or `target_device`.

Read-only: `gateway_health`, `command_result`, `pc_status`, `system_status`, `capabilities`, `action_list`, `process_status`, `motor_status`, `docker_status`, `ollama_status`, `job_status`, `job_tail`, `file_list`, `file_read`, `file_tail`, `search_files`, `search_text`, `git_status`, `git_diff`, `git_log`, `bank_status`.

Mutations (full mode only): `action_run`, `project_run`, `mkdir`, `write_text`, `patch_text`, `copy_path`, `move_path`, `trash_path`, `git_commit`, `git_restore_file`, `job_stop`, `stop_all_motors`, `conference_run`.

### Read behaviour

`action=invoke` with `wait_seconds=22`. If the command is not complete, the gateway keeps the same `command_id` and polls `action=result` every 1 s until 45 s total, then returns `status="pending"` with the `command_id`. Use `command_result(command_id)` afterwards. Nothing is ever re-enqueued.

### Mutation behaviour

`action=enqueue`, one attempt, one mutation at a time per process. Statuses:

- `queued` with `command_id`: poll with `command_result`.
- `UNKNOWN_OUTCOME`, `retry=false`: timeout, transport error, HTTP 5xx, or an unreadable 2xx response. The command may or may not be queued. Do not repeat it; check state with read-only tools.
- `error`, `retry=false`: definitely not queued (connection never established, HTTP 4xx, validation).

### Limits

- Upstream timeouts: connect 5 s, read 30 s, write 10 s, pool 5 s.
- Rate limit: 60 calls/min, burst 20, per process (`status="rate_limited"`).
- Concurrency: 4 reads, 1 mutation.
- `content`, `old`, `new`, `conference_run.payload`: 256 KiB each; upstream request body: 1 MiB.
- Upstream responses over 512 KiB serialized are replaced by `{truncated, original_bytes, limit_bytes, sha256, preview}`.
- Other argument bounds are in `VALIDATORS` in `server.py` (`lines` 1-5000, `max_bytes` 1-262144, `max_results` 1-1000, `max_count` 1-500).

The limits are per process, so run a single instance and a single uvicorn worker.

## Audit

One JSON line on stdout per tool call: `timestamp`, `request_id`, `client`, `mode`, `tool`, `device`, `args_sha256`, `command_id`, `outcome`, `latency_ms`. Argument bodies, headers, tokens, file contents and result bodies are never logged. The log is not durable beyond Render's log retention.

## Health

`GET /healthz` is unauthenticated, does not call upstream, and returns only:

    {"ok": true, "version": "1.0.0", "mode": "readonly|full|off", "upstream_configured": true|false}

## Initial acceptance gates

Run in order with `GATEWAY_MODE=readonly` and `MUTATION_GUARD` unset:

1. `GET /healthz` returns `ok=true`, `mode=readonly`, `upstream_configured=true`.
2. Authenticated MCP initialize, discovery and tool listing succeed from the Claude connector; no mutation tool is listed. The same request without the header returns 401.
3. `gateway_health` returns `status="ok"`.
4. `pc_status` returns `status="completed"`.
5. `system_status` returns `status="completed"`.

## Tests

    python -m unittest -v test_gateway

No real network, Supabase or Claude is needed; upstream is replaced with `httpx.MockTransport`.
