"""CLAUDE-MCP-GATEWAY: Claude custom connector -> pc-bridge v3 (Supabase Edge) -> TANSEL V4 queue."""
import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal, Mapping
from urllib.parse import parse_qsl

import httpx
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

VERSION = "1.0.0"
MODES = ("readonly", "full", "off")
MUTATION_GUARD = "I_UNDERSTAND_DUPLICATE_RISK"
# poll and complete belong to the Windows bridge and are never called from here.
ALLOWED_ACTIONS = frozenset({"health", "enqueue", "result", "invoke"})
SECOND_PC = "SECOND-PC"
READ_COMMANDS = frozenset({
    "status", "system_status", "capabilities", "action_list", "process_status", "motor_status",
    "docker_status", "ollama_status", "job_status", "job_tail", "list", "read", "tail",
    "search_files", "search_text", "git_status", "git_diff", "git_log", "bank_status",
})
MUTATION_COMMANDS = frozenset({
    "action_run", "project_run", "mkdir", "write_text", "patch_text", "copy_path", "move_path",
    "trash_path", "git_commit", "git_restore_file", "job_stop", "stop_all_motors", "conference_run",
})
INVOKE_WAIT_SECONDS = 22  # pc-bridge maximum is 25
READ_DEADLINE_SECONDS = 45.0
POLL_INTERVAL_SECONDS = 1.0
MAX_TEXT_BYTES = 256 * 1024
MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 512 * 1024
PREVIEW_BYTES = 64 * 1024
RATE_PER_MINUTE = 60
RATE_BURST = 20
READ_CONCURRENCY = 4
PENDING_STATES = frozenset({"queued", "pending", "claimed", "running", "processing"})
CREDENTIAL_QUERY_KEYS = frozenset({
    "token", "access_token", "authorization", "auth", "bearer", "apikey", "api_key", "key", "secret",
})
COMMAND_ID_RE = re.compile("[A-Za-z0-9_-]{1,128}")
Device = Literal["main", "second"]


class Invalid(ValueError):
    """Argument validation failure; the message names the field only, never its value."""


class UpstreamError(Exception):
    def __init__(self, code: str, ambiguous: bool = False):
        super().__init__(code)
        self.code = code
        self.ambiguous = ambiguous  # True when the request may have reached pc-bridge


@dataclass(frozen=True)
class Config:
    mode: str
    token: str = field(repr=False)
    supabase_url: str
    anon_jwt: str = field(repr=False)
    bridge_secret: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Config":
        raw = (env.get("GATEWAY_MODE") or "readonly").strip().lower()
        mode = raw if raw in MODES else "off"  # unrecognised value fails closed
        if mode == "full" and env.get("MUTATION_GUARD") != MUTATION_GUARD:
            mode = "readonly"
        url = (env.get("SUPABASE_URL") or "").strip().rstrip("/")
        return cls(
            mode=mode,
            token=(env.get("CLAUDE_MCP_TOKEN") or "").strip(),
            supabase_url=url if url.startswith("https://") else "",
            anon_jwt=(env.get("SUPABASE_ANON_JWT") or "").strip(),
            bridge_secret=(env.get("V4_BRIDGE_SECRET") or "").strip(),
        )

    @property
    def upstream_configured(self) -> bool:
        return bool(self.supabase_url and self.anon_jwt and self.bridge_secret)

    @property
    def endpoint(self) -> str:
        return self.supabase_url + "/functions/v1/pc-bridge"


class TokenBucket:
    def __init__(self, per_minute: int, burst: int, clock: Callable[[], float]):
        self.rate = per_minute / 60.0
        self.burst = float(burst)
        self.tokens = float(burst)
        self.clock = clock
        self.last = clock()

    def take(self) -> bool:
        now = self.clock()
        self.tokens = min(self.burst, self.tokens + max(0.0, now - self.last) * self.rate)
        self.last = now
        if self.tokens < 1.0:
            return False
        self.tokens -= 1.0
        return True


def _text(max_bytes: int, empty: bool = True):
    def check(name: str, value: Any) -> str:
        if not isinstance(value, str) or chr(0) in value:
            raise Invalid(f"{name} must be a string without NUL")
        if len(value.encode("utf-8", "replace")) > max_bytes:
            raise Invalid(f"{name} exceeds {max_bytes} bytes")
        if not empty and not value.strip():
            raise Invalid(f"{name} must not be empty")
        return value
    return check


def _int(low: int, high: int):
    def check(name: str, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise Invalid(f"{name} must be an integer in [{low}, {high}]")
        return value
    return check


def _json(max_bytes: int, *kinds: type):
    def check(name: str, value: Any) -> Any:
        if not isinstance(value, kinds):
            raise Invalid(f"{name} has the wrong type")
        if isinstance(value, list) and not all(isinstance(item, str) for item in value):
            raise Invalid(f"{name} must contain only strings")
        try:
            size = len(json.dumps(value, ensure_ascii=False).encode("utf-8", "replace"))
        except (TypeError, ValueError):
            raise Invalid(f"{name} must be JSON-serializable") from None
        if size > max_bytes:
            raise Invalid(f"{name} exceeds {max_bytes} bytes")
        return value
    return check


def _command_id(name: str, value: Any) -> str:
    if not isinstance(value, str) or not COMMAND_ID_RE.fullmatch(value):
        raise Invalid(f"{name} is not a valid command id")
    return value


VALIDATORS: dict[str, Callable[[str, Any], Any]] = {
    "project": _text(128, empty=False),
    "relative_path": _text(1024),
    "source": _text(1024),
    "destination": _text(1024),
    "job_id": _text(128, empty=False),
    "action_id": _text(128, empty=False),
    "runtime": _text(64, empty=False),
    "script": _text(1024, empty=False),
    "pattern": _text(1024),
    "term": _text(1024),
    "message": _text(8192, empty=False),
    "content": _text(MAX_TEXT_BYTES),
    "old": _text(MAX_TEXT_BYTES),
    "new": _text(MAX_TEXT_BYTES),
    "lines": _int(1, 5000),
    "max_bytes": _int(1, MAX_TEXT_BYTES),
    "max_results": _int(1, 1000),
    "max_count": _int(1, 500),
    "args": _json(64 * 1024, dict, list),
    "payload": _json(MAX_TEXT_BYTES, dict),
    "command_id": _command_id,
}


def validate(args: Mapping[str, Any], require: tuple[str, ...] = ()) -> dict[str, Any]:
    out = {name: VALIDATORS[name](name, value) for name, value in args.items() if value is not None}
    for name in require:
        if not out.get(name):
            raise Invalid(f"{name} is required")
    return out


def command_body(command_type: str, device: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Build the pc-bridge command. Main PC: no device field at all, so target_device stays null."""
    if command_type not in READ_COMMANDS and command_type not in MUTATION_COMMANDS:
        raise Invalid("unsupported command")
    body: dict[str, Any] = {"command_type": command_type, "payload": payload}
    if device == "second":
        body["target_device"] = SECOND_PC
    elif device != "main":
        raise Invalid("device must be 'main' or 'second'")
    return body


def _cid(data: Mapping[str, Any]) -> str | None:
    value = data.get("command_id") or data.get("id")
    return value if isinstance(value, str) and COMMAND_ID_RE.fullmatch(value) else None


def _done(status: int, data: Mapping[str, Any]) -> bool:
    if status != 200 or data.get("completed") is False:
        return False
    return str(data.get("status", "")).lower() not in PENDING_STATES


class Bridge:
    """The only code path that talks to pc-bridge. Secrets stay in this class."""

    def __init__(self, cfg: Config, client: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self._client = client

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0))
        return self._client

    async def call(self, action: str, body: dict[str, Any], read_timeout: float = 30.0) -> tuple[int, dict[str, Any]]:
        if action not in ALLOWED_ACTIONS:
            raise UpstreamError("forbidden_action")
        if not self.cfg.upstream_configured:
            raise UpstreamError("upstream_not_configured")
        try:
            raw = json.dumps({**body, "action": action}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError):
            raise UpstreamError("request_not_serializable") from None
        if len(raw) > MAX_REQUEST_BYTES:
            raise UpstreamError("request_too_large")
        headers = {
            "Authorization": "Bearer " + self.cfg.anon_jwt,
            "apikey": self.cfg.anon_jwt,
            "x-bridge-secret": self.cfg.bridge_secret,
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(connect=5.0, read=max(1.0, min(30.0, read_timeout)), write=10.0, pool=5.0)
        try:
            resp = await self._http().post(self.cfg.endpoint, content=raw, headers=headers, timeout=timeout)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            raise UpstreamError("upstream_unreachable") from None  # nothing was sent
        except httpx.HTTPError:
            raise UpstreamError("upstream_transport_error", ambiguous=True) from None
        if resp.status_code not in (200, 202):
            raise UpstreamError(f"upstream_http_{resp.status_code}", ambiguous=resp.status_code >= 500)
        try:
            data = resp.json()
        except ValueError:
            data = None
        if not isinstance(data, dict):
            raise UpstreamError("upstream_invalid_json", ambiguous=True)
        return resp.status_code, data

    def shield(self, obj: Any) -> Any:
        """Redact gateway secrets and cap the size of anything returned to the MCP client."""
        text = json.dumps(obj, ensure_ascii=True, separators=(",", ":"), default=str)
        redacted = False
        for secret in (self.cfg.anon_jwt, self.cfg.bridge_secret, self.cfg.token):
            if len(secret) >= 8 and secret in text:
                text = text.replace(secret, "[REDACTED]")
                redacted = True
        if len(text) > MAX_RESPONSE_BYTES:
            return {
                "truncated": True,
                "original_bytes": len(text),
                "limit_bytes": MAX_RESPONSE_BYTES,
                "sha256": hashlib.sha256(text.encode("ascii")).hexdigest(),
                "preview": text[:PREVIEW_BYTES],
            }
        return json.loads(text) if redacted else obj


class Gateway:
    def __init__(self, cfg: Config, client: httpx.AsyncClient | None = None,
                 sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.cfg = cfg
        self.bridge = Bridge(cfg, client)
        self.tools: dict[str, Callable[..., Awaitable[dict[str, Any]]]] = {}
        self._sleep = sleep
        self._clock = clock
        self._bucket = TokenBucket(RATE_PER_MINUTE, RATE_BURST, clock)
        self._read_sem = asyncio.Semaphore(READ_CONCURRENCY)
        self._mutation_lock = asyncio.Lock()

    def _audit(self, request_id: str, tool: str, device: str | None, args: Mapping[str, Any],
               command_id: Any, outcome: Any, started: float) -> None:
        digest = hashlib.sha256(json.dumps(args, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8", "replace"))
        print(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "request_id": request_id,
            "client": "claude",
            "mode": self.cfg.mode,
            "tool": tool,
            "device": device,
            "args_sha256": digest.hexdigest(),
            "command_id": command_id if isinstance(command_id, str) else None,
            "outcome": str(outcome),
            "latency_ms": int((time.monotonic() - started) * 1000),
        }, separators=(",", ":")), flush=True)

    async def _audited(self, tool: str, device: str | None, args: Mapping[str, Any],
                       run: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        started = time.monotonic()
        try:
            if device not in (None, "main", "second"):
                raise Invalid("device must be 'main' or 'second'")
            if not self._bucket.take():
                out: dict[str, Any] = {"status": "rate_limited", "retry_after_seconds": 1}
            else:
                out = await run()
        except Invalid as exc:
            out = {"status": "invalid_arguments", "error": str(exc)}
        except Exception:
            out = {"status": "error", "error": "internal_error", "retry": False}
        self._audit(request_id, tool, device if device in ("main", "second") else None, args,
                    out.get("command_id"), out.get("status"), started)
        return {**out, "request_id": request_id}

    def _completed(self, command_id: str | None, data: dict[str, Any]) -> dict[str, Any]:
        return {"status": "completed", "command_id": command_id, "result": self.bridge.shield(data)}

    @staticmethod
    def _pending(command_id: str) -> dict[str, Any]:
        return {"status": "pending", "command_id": command_id, "retry": False,
                "next": "call command_result with this command_id; do not repeat the original call"}

    async def _poll(self, command_id: str, deadline: float) -> dict[str, Any]:
        while (remaining := deadline - self._clock()) > POLL_INTERVAL_SECONDS:
            await self._sleep(POLL_INTERVAL_SECONDS)
            try:
                status, data = await self.bridge.call("result", {"command_id": command_id}, read_timeout=remaining)
            except UpstreamError as exc:
                if exc.ambiguous:
                    continue  # reading a result is side-effect free
                return {"status": "error", "error": exc.code, "command_id": command_id, "retry": False}
            if _done(status, data):
                return self._completed(command_id, data)
        return self._pending(command_id)

    async def health(self) -> dict[str, Any]:
        info = {"version": VERSION, "mode": self.cfg.mode, "upstream_configured": self.cfg.upstream_configured}

        async def run() -> dict[str, Any]:
            try:
                _, data = await self.bridge.call("health", {})
            except UpstreamError as exc:
                return {"status": "error", "error": exc.code, "gateway": info}
            return {"status": "ok", "gateway": info, "upstream": self.bridge.shield(data)}
        return await self._audited("gateway_health", None, {}, run)

    async def result(self, command_id: str) -> dict[str, Any]:
        async def run() -> dict[str, Any]:
            cid = validate({"command_id": command_id})["command_id"]
            async with self._read_sem:
                try:
                    status, data = await self.bridge.call("result", {"command_id": cid})
                except UpstreamError as exc:
                    return {"status": "error", "error": exc.code, "command_id": cid, "retry": False}
            return self._completed(cid, data) if _done(status, data) else self._pending(cid)
        return await self._audited("command_result", None, {"command_id": command_id}, run)

    async def read(self, tool: str, command_type: str, device: str, args: dict[str, Any],
                   require: tuple[str, ...] = ()) -> dict[str, Any]:
        async def run() -> dict[str, Any]:
            if command_type not in READ_COMMANDS:
                raise Invalid("unsupported command")
            body = command_body(command_type, device, validate(args, require))
            body["wait_seconds"] = INVOKE_WAIT_SECONDS
            async with self._read_sem:
                deadline = self._clock() + READ_DEADLINE_SECONDS
                try:
                    status, data = await self.bridge.call("invoke", body)
                except UpstreamError as exc:
                    return {"status": "error", "error": exc.code}
                cid = _cid(data)
                if _done(status, data):
                    return self._completed(cid, data)
                if cid is None:
                    return {"status": "error", "error": "upstream_missing_command_id"}
                return await self._poll(cid, deadline)  # same command_id, never re-enqueued
        return await self._audited(tool, device, {**args, "device": device}, run)

    async def mutate(self, tool: str, command_type: str, device: str, args: dict[str, Any],
                     require: tuple[str, ...] = (), unwrap: str | None = None) -> dict[str, Any]:
        async def run() -> dict[str, Any]:
            if self.cfg.mode != "full" or command_type not in MUTATION_COMMANDS:
                return {"status": "error", "error": "mutations_disabled", "retry": False}
            payload = validate(args, require)
            body = command_body(command_type, device, payload[unwrap] if unwrap else payload)
            unknown = {"status": "UNKNOWN_OUTCOME", "command_id": None, "retry": False,
                       "message": "The command may or may not have been queued. Do not repeat it; inspect state with read-only tools first."}
            async with self._mutation_lock:
                try:
                    _, data = await self.bridge.call("enqueue", body)  # exactly one attempt, never retried
                except UpstreamError as exc:
                    if exc.ambiguous:
                        return {**unknown, "error": exc.code}
                    return {"status": "error", "error": exc.code, "retry": False}
            cid = _cid(data)
            if cid is None:
                return {**unknown, "error": "upstream_missing_command_id"}
            return {"status": "queued", "command_id": cid, "retry": False,
                    "next": "call command_result with this command_id"}
        return await self._audited(tool, device, {**args, "device": device}, run)


def register_tools(mcp: MCPServer, gw: Gateway) -> None:
    def expose(fn):
        gw.tools[fn.__name__] = fn
        mcp.tool()(fn)
        return fn

    def skip(fn):
        return fn

    ro = expose if gw.cfg.mode in ("readonly", "full") else skip
    mut = expose if gw.cfg.mode == "full" else skip

    @ro
    async def gateway_health() -> dict[str, Any]:
        """Gateway version/mode plus pc-bridge health."""
        return await gw.health()

    @ro
    async def command_result(command_id: str) -> dict[str, Any]:
        """Fetch the result of a previously issued command by command_id."""
        return await gw.result(command_id)

    @ro
    async def pc_status(device: Device = "main") -> dict[str, Any]:
        """Bridge status of the PC."""
        return await gw.read("pc_status", "status", device, {})

    @ro
    async def system_status(device: Device = "main") -> dict[str, Any]:
        """System status (CPU, memory, disk)."""
        return await gw.read("system_status", "system_status", device, {})

    @ro
    async def capabilities(device: Device = "main") -> dict[str, Any]:
        """Capabilities reported by V4."""
        return await gw.read("capabilities", "capabilities", device, {})

    @ro
    async def action_list(device: Device = "main") -> dict[str, Any]:
        """List the allow-listed V4 actions."""
        return await gw.read("action_list", "action_list", device, {})

    @ro
    async def process_status(project: str | None = None, device: Device = "main") -> dict[str, Any]:
        """Process status, optionally for one project."""
        return await gw.read("process_status", "process_status", device, {"project": project})

    @ro
    async def motor_status(device: Device = "main") -> dict[str, Any]:
        """Motor status."""
        return await gw.read("motor_status", "motor_status", device, {})

    @ro
    async def docker_status(device: Device = "main") -> dict[str, Any]:
        """Docker status."""
        return await gw.read("docker_status", "docker_status", device, {})

    @ro
    async def ollama_status(device: Device = "main") -> dict[str, Any]:
        """Ollama status."""
        return await gw.read("ollama_status", "ollama_status", device, {})

    @ro
    async def job_status(job_id: str, device: Device = "main") -> dict[str, Any]:
        """Status of one job."""
        return await gw.read("job_status", "job_status", device, {"job_id": job_id})

    @ro
    async def job_tail(job_id: str, lines: int = 200, device: Device = "main") -> dict[str, Any]:
        """Last lines of a job's output."""
        return await gw.read("job_tail", "job_tail", device, {"job_id": job_id, "lines": lines})

    @ro
    async def file_list(project: str, relative_path: str = "", device: Device = "main") -> dict[str, Any]:
        """List a directory inside a project."""
        return await gw.read("file_list", "list", device, {"project": project, "relative_path": relative_path})

    @ro
    async def file_read(project: str, relative_path: str, max_bytes: int = 65536, device: Device = "main") -> dict[str, Any]:
        """Read a file inside a project."""
        return await gw.read("file_read", "read", device,
                             {"project": project, "relative_path": relative_path, "max_bytes": max_bytes},
                             require=("relative_path",))

    @ro
    async def file_tail(project: str, relative_path: str, lines: int = 200, device: Device = "main") -> dict[str, Any]:
        """Last lines of a file inside a project."""
        return await gw.read("file_tail", "tail", device,
                             {"project": project, "relative_path": relative_path, "lines": lines},
                             require=("relative_path",))

    @ro
    async def search_files(project: str, relative_path: str = "", pattern: str = "", max_results: int = 100,
                           device: Device = "main") -> dict[str, Any]:
        """Find files by name pattern inside a project."""
        return await gw.read("search_files", "search_files", device,
                             {"project": project, "relative_path": relative_path, "pattern": pattern, "max_results": max_results})

    @ro
    async def search_text(project: str, relative_path: str = "", term: str = "", max_results: int = 100,
                          device: Device = "main") -> dict[str, Any]:
        """Search file contents inside a project."""
        return await gw.read("search_text", "search_text", device,
                             {"project": project, "relative_path": relative_path, "term": term, "max_results": max_results})

    @ro
    async def git_status(project: str, device: Device = "main") -> dict[str, Any]:
        """git status of a project."""
        return await gw.read("git_status", "git_status", device, {"project": project})

    @ro
    async def git_diff(project: str, relative_path: str = "", device: Device = "main") -> dict[str, Any]:
        """git diff of a project or one path."""
        return await gw.read("git_diff", "git_diff", device, {"project": project, "relative_path": relative_path})

    @ro
    async def git_log(project: str, max_count: int = 20, device: Device = "main") -> dict[str, Any]:
        """Recent git commits of a project."""
        return await gw.read("git_log", "git_log", device, {"project": project, "max_count": max_count})

    @ro
    async def bank_status(device: Device = "main") -> dict[str, Any]:
        """Bank status."""
        return await gw.read("bank_status", "bank_status", device, {})

    @mut
    async def action_run(action_id: str, args: dict[str, Any], device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue an allow-listed V4 action. Returns queued + command_id; never repeat on UNKNOWN_OUTCOME."""
        return await gw.mutate("action_run", "action_run", device, {"action_id": action_id, "args": args})

    @mut
    async def project_run(project: str, runtime: str, script: str, args: list[str], device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a project script run."""
        return await gw.mutate("project_run", "project_run", device,
                               {"project": project, "runtime": runtime, "script": script, "args": args})

    @mut
    async def mkdir(project: str, relative_path: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue directory creation."""
        return await gw.mutate("mkdir", "mkdir", device, {"project": project, "relative_path": relative_path},
                               require=("relative_path",))

    @mut
    async def write_text(project: str, relative_path: str, content: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a text file write (content max 256 KiB)."""
        return await gw.mutate("write_text", "write_text", device,
                               {"project": project, "relative_path": relative_path, "content": content},
                               require=("relative_path",))

    @mut
    async def patch_text(project: str, relative_path: str, old: str, new: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a text replacement in a file."""
        return await gw.mutate("patch_text", "patch_text", device,
                               {"project": project, "relative_path": relative_path, "old": old, "new": new},
                               require=("relative_path", "old"))

    @mut
    async def copy_path(project: str, source: str, destination: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a copy inside a project."""
        return await gw.mutate("copy_path", "copy_path", device,
                               {"project": project, "source": source, "destination": destination},
                               require=("source", "destination"))

    @mut
    async def move_path(project: str, source: str, destination: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a move inside a project."""
        return await gw.mutate("move_path", "move_path", device,
                               {"project": project, "source": source, "destination": destination},
                               require=("source", "destination"))

    @mut
    async def trash_path(project: str, relative_path: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue moving a path to trash."""
        return await gw.mutate("trash_path", "trash_path", device, {"project": project, "relative_path": relative_path},
                               require=("relative_path",))

    @mut
    async def git_commit(project: str, message: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a git commit."""
        return await gw.mutate("git_commit", "git_commit", device, {"project": project, "message": message})

    @mut
    async def git_restore_file(project: str, relative_path: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a git restore of one file."""
        return await gw.mutate("git_restore_file", "git_restore_file", device,
                               {"project": project, "relative_path": relative_path}, require=("relative_path",))

    @mut
    async def job_stop(job_id: str, device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue stopping a job."""
        return await gw.mutate("job_stop", "job_stop", device, {"job_id": job_id})

    @mut
    async def stop_all_motors(device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue stopping all motors."""
        return await gw.mutate("stop_all_motors", "stop_all_motors", device, {})

    @mut
    async def conference_run(payload: dict[str, Any], device: Device = "main") -> dict[str, Any]:
        """MUTATION. Queue a conference run with the given payload."""
        return await gw.mutate("conference_run", "conference_run", device, {"payload": payload}, unwrap="payload")


class GatewayASGI:
    """Outer ASGI app: /healthz, mode gate and bearer auth in front of the MCP app."""

    def __init__(self, inner, cfg: Config):
        self.inner = inner
        self.cfg = cfg

    def _authorized(self, scope) -> bool:
        values = [value for key, value in scope.get("headers", []) if key == b"authorization"]
        if len(values) != 1 or not self.cfg.token:
            return False
        return hmac.compare_digest(values[0], b"Bearer " + self.cfg.token.encode("utf-8"))

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            return await self.inner(scope, receive, send)
        if scope["type"] != "http":
            return

        async def reply(status: int, body: dict[str, Any], headers: dict[str, str] | None = None):
            await JSONResponse(body, status_code=status, headers=headers)(scope, receive, send)

        path = scope.get("path", "")
        if path == "/healthz":
            return await reply(200, {"ok": True, "version": VERSION, "mode": self.cfg.mode,
                                     "upstream_configured": self.cfg.upstream_configured})
        if path != "/mcp":
            return await reply(404, {"error": "not_found"})
        if self.cfg.mode == "off":
            return await reply(503, {"error": "gateway_off"})
        query = scope.get("query_string", b"").decode("latin-1")
        if {key.lower() for key, _ in parse_qsl(query, keep_blank_values=True)} & CREDENTIAL_QUERY_KEYS:
            return await reply(400, {"error": "query_string_credentials_rejected"})
        if not self._authorized(scope):
            return await reply(401, {"error": "unauthorized"}, {"WWW-Authenticate": "Bearer"})
        await self.inner(scope, receive, send)


def build(env: Mapping[str, str], client: httpx.AsyncClient | None = None,
          sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
          clock: Callable[[], float] = time.monotonic) -> tuple[GatewayASGI, Gateway]:
    cfg = Config.from_env(env)  # mode is fixed here; there is no runtime toggle
    gw = Gateway(cfg, client, sleep, clock)
    mcp = MCPServer("CLAUDE-MCP-GATEWAY")
    register_tools(mcp, gw)
    inner = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        # Public hostname on Render; access is gated by the bearer token above.
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    return GatewayASGI(inner, cfg), gw


app, gateway = build(os.environ)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
