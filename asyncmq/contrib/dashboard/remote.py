from __future__ import annotations

import datetime as dt
import inspect
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, cast

import anyio
from lilya.apps import Lilya
from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import JSONResponse
from lilya.routing import RoutePath
from lilya.types import ASGIApp

from asyncmq import monkay
from asyncmq.core.inspection import JobInspectionPage

REMOTE_TOKEN_ENV = "ASYNCMQ_REMOTE_TOKEN"
REMOTE_URL_ENV = "ASYNCMQ_REMOTE_BACKEND_URL"
REMOTE_BACKEND_METHODS: frozenset[str] = frozenset(
    {
        "cancel_job",
        "clean_jobs",
        "count",
        "create_repeatable",
        "drain_queue",
        "enqueue_repeatable",
        "get_job",
        "get_job_count_by_types",
        "get_job_counts",
        "get_job_result",
        "get_job_state",
        "get_jobs",
        "health_check",
        "inspect_jobs",
        "is_queue_paused",
        "list_jobs",
        "list_queues",
        "list_repeatables",
        "list_workers",
        "obliterate_queue",
        "pause_queue",
        "pause_repeatable",
        "purge",
        "queue_stats",
        "remove_job",
        "remove_repeatable",
        "resume_queue",
        "resume_repeatable",
        "retry_job",
        "upsert_repeatable",
    }
)

TransportResponse = tuple[int, Any]
Transport = Callable[
    [str, str, dict[str, Any] | None, dict[str, str], dict[str, str] | None],
    TransportResponse | Awaitable[TransportResponse],
]


class RemoteBackendError(RuntimeError):
    """
    Raised when the remote AsyncMQ backend control plane rejects a call.

    The dashboard client uses this error for HTTP failures, remote exceptions,
    malformed response payloads, and missing client configuration.
    """


def _json_default(value: Any) -> Any:
    """
    Convert common AsyncMQ runtime objects into JSON-compatible values.

    The remote backend endpoint intentionally serializes only data returned by
    allowlisted administrative methods, but those values may include dataclasses,
    enums, sets, tuples, or timestamp objects.
    """
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (set, tuple)):
        return list(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump()

    legacy_dict = getattr(value, "dict", None)
    if callable(legacy_dict):
        return legacy_dict()

    return str(value)


def _to_jsonable(value: Any) -> Any:
    """
    Normalize a value through the standard JSON encoder and decoder.

    This keeps the response envelope predictable for both real HTTP clients and
    the in-process transport used by tests or custom deployments.
    """
    return json.loads(json.dumps(value, default=_json_default))


def _success(data: Any = None) -> JSONResponse:
    """
    Build a successful remote backend response envelope.

    Returning a consistent envelope lets the client distinguish remote backend
    results from transport metadata such as the HTTP status code.
    """
    return JSONResponse({"ok": True, "data": _to_jsonable(data)})


def _error(exc: Exception, *, status_code: int, method: str | None = None) -> JSONResponse:
    """
    Build an error response envelope for remote backend failures.

    The message is intentionally concise because the endpoint is a private
    control plane and callers should not parse tracebacks from responses.
    """
    return JSONResponse(
        {
            "ok": False,
            "error": {
                "type": exc.__class__.__name__,
                "message": str(exc),
                "method": method,
            },
        },
        status_code=status_code,
    )


async def _json_body(request: Request) -> dict[str, Any]:
    """
    Decode a request body into a dictionary payload.

    Empty request bodies are accepted as an empty dictionary so health-style
    calls and future lightweight endpoints can share this helper.
    """
    try:
        body = await request.json()
    except json.JSONDecodeError as exc:
        raise ValueError("Remote backend calls require a valid JSON request body.") from exc
    except Exception as exc:
        text = await request.body()
        if not text:
            return {}
        raise ValueError("Remote backend calls require a valid JSON request body.") from exc

    if body is None:
        return {}
    if not isinstance(body, dict):
        raise ValueError("Remote backend calls require a JSON object request body.")
    return cast(dict[str, Any], body)


def _resolve_expected_token(token: str | None) -> str | None:
    """
    Resolve the server-side bearer token for remote control access.

    The explicit argument wins, with ``ASYNCMQ_REMOTE_TOKEN`` as the deployment
    fallback for containerized worker services.
    """
    return token or os.getenv(REMOTE_TOKEN_ENV)


def _check_token(request: Request, token: str | None) -> None:
    """
    Validate the remote-control token when one is configured.

    AsyncMQ accepts the standard ``Authorization: Bearer`` shape and the
    ``X-AsyncMQ-Remote-Token`` header for service meshes that normalize auth.
    """
    expected = _resolve_expected_token(token)
    if not expected:
        return

    authorization = request.headers.get("authorization", "")
    header_token = request.headers.get("x-asyncmq-remote-token", "")
    if authorization == f"Bearer {expected}" or header_token == expected:
        return
    raise PermissionError("Invalid AsyncMQ remote backend token.")


def _resolve_backend(backend: Any | None) -> Any:
    """
    Return the backend that owns real AsyncMQ runtime state.

    Worker/control services may pass an explicit backend, while applications
    using settings can let the endpoint read ``monkay.settings.backend``.
    """
    return backend if backend is not None else monkay.settings.backend


async def _call_backend_method(backend: Any, method: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
    """
    Execute one allowlisted backend method and await it when necessary.

    This helper keeps the route handler small and supports custom backends that
    expose synchronous administrative helpers.
    """
    handler = getattr(backend, method, None)
    if not callable(handler):
        raise AttributeError(f"Backend does not provide remote method '{method}'.")

    result = handler(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def create_remote_backend_app(backend: Any | None = None, *, token: str | None = None) -> ASGIApp:
    """
    Create the worker-side AsyncMQ backend control application.

    Mount this app only inside a trusted network or service boundary. The
    dashboard process can then use ``RemoteBackendClient`` as its configured
    backend without holding direct Redis, database, or broker credentials.
    """

    class RemoteHealthController(Controller):
        """
        Serve remote backend liveness and optional backend readiness.

        The endpoint validates the same token as mutating calls so private
        health checks do not become a public backend discovery surface.
        """

        async def get(self, request: Request) -> JSONResponse:
            """
            Handle remote backend health checks.

            When the real backend exposes ``health_check`` this endpoint calls
            it, otherwise a configured endpoint is considered alive.
            """
            try:
                _check_token(request, token)
                current_backend = _resolve_backend(backend)
                health_check = getattr(current_backend, "health_check", None)
                if callable(health_check):
                    result = health_check()
                    if inspect.isawaitable(result):
                        await result
            except PermissionError as exc:
                return _error(exc, status_code=401, method="health_check")
            except Exception as exc:
                return _error(exc, status_code=503, method="health_check")
            return _success({"status": "ok"})

    class RemoteBackendCallController(Controller):
        """
        Dispatch allowlisted backend inspection and control methods.

        The method allowlist prevents the endpoint from becoming an arbitrary
        object invocation gateway into the worker process.
        """

        async def post(self, request: Request) -> JSONResponse:
            """
            Execute one allowlisted backend call from the dashboard client.

            The request payload uses ``{"args": [...], "kwargs": {...}}`` so
            backend method signatures stay backend-native.
            """
            method = str(request.path_params["method"])
            try:
                _check_token(request, token)
            except PermissionError as exc:
                return _error(exc, status_code=401, method=method)

            if method not in REMOTE_BACKEND_METHODS:
                return _error(ValueError(f"Remote backend method '{method}' is not allowed."), status_code=404)

            try:
                body = await _json_body(request)
                args = body.get("args", [])
                kwargs = body.get("kwargs", {})
                if not isinstance(args, list):
                    raise ValueError("Remote backend call args must be a list.")
                if not isinstance(kwargs, dict):
                    raise ValueError("Remote backend call kwargs must be an object.")

                result = await _call_backend_method(
                    _resolve_backend(backend), method, args, cast(dict[str, Any], kwargs)
                )
            except ValueError as exc:
                return _error(exc, status_code=400, method=method)
            except AttributeError as exc:
                return _error(exc, status_code=404, method=method)
            except Exception as exc:
                return _error(exc, status_code=500, method=method)

            return _success(result)

    app = Lilya(
        routes=[
            RoutePath("/health", RemoteHealthController, methods=["GET"], name="asyncmq-remote-health"),
            RoutePath("/call/{method}", RemoteBackendCallController, methods=["POST"], name="asyncmq-remote-call"),
        ]
    )
    return cast(ASGIApp, app)


class RemoteBackendClient:
    """
    Backend-shaped AsyncMQ dashboard client for remote worker/control services.

    Configure this object as ``monkay.settings.backend`` in a dashboard-only
    process when the real backend connection lives in another internal service.
    """

    is_remote = True

    def __init__(
        self,
        base_url: str | None = None,
        *,
        token: str | None = None,
        transport: Transport | None = None,
    ) -> None:
        """
        Initialize the remote backend client.

        ``base_url`` can be omitted when ``ASYNCMQ_REMOTE_BACKEND_URL`` is set
        or when a custom in-process transport is supplied for tests.
        """
        self.base_url = (base_url or os.getenv(REMOTE_URL_ENV) or "").rstrip("/")
        self.token = token or os.getenv(REMOTE_TOKEN_ENV)
        self.transport = transport

        if not self.base_url and self.transport is None:
            raise ValueError(f"RemoteBackendClient requires a base_url, a transport, or {REMOTE_URL_ENV}.")

    def _headers(self) -> dict[str, str]:
        """
        Build HTTP headers for a remote backend request.

        The bearer token is optional for development or test transports, but
        production deployments should always configure it.
        """
        headers = {"accept": "application/json", "content-type": "application/json"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        return headers

    def _url(self, path: str, query: dict[str, str] | None = None) -> str:
        """
        Build a full request URL from the configured base URL.

        Query parameters are kept separate from JSON bodies so non-POST helper
        endpoints remain possible without changing the transport contract.
        """
        if not self.base_url:
            raise RemoteBackendError("RemoteBackendClient has no base_url configured.")
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        return url

    def _urllib_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
        query: dict[str, str] | None,
    ) -> TransportResponse:
        """
        Send one blocking standard-library HTTP request.

        The async client calls this method in a worker thread so dashboard
        request handlers do not block the event loop.
        """
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(self._url(path, query), data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = int(response.status)
                response_body = response.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            response_body = exc.read()
        except urllib.error.URLError as exc:
            raise RemoteBackendError(f"Remote AsyncMQ backend request failed: {exc}") from exc

        if not response_body:
            return status, {}

        try:
            return status, json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RemoteBackendError("Remote AsyncMQ backend returned invalid JSON.") from exc

    async def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        """
        Execute a remote request through the configured transport.

        Custom transports make tests and non-HTTP embeddings possible while the
        default path uses standard HTTP against ``base_url``.
        """
        headers = self._headers()
        if self.transport is not None:
            response = self.transport(method, path, payload, headers, query)
            if inspect.isawaitable(response):
                status, body = await cast(Awaitable[TransportResponse], response)
            else:
                status, body = cast(TransportResponse, response)
        else:
            status, body = await anyio.to_thread.run_sync(
                self._urllib_request,
                method,
                path,
                payload,
                headers,
                query,
            )

        return self._decode_response(status, body)

    def _decode_response(self, status: int, body: Any) -> Any:
        """
        Decode one remote response envelope into backend return data.

        HTTP and envelope failures are both converted into ``RemoteBackendError``
        so dashboard controllers can use their existing backend error handling.
        """
        if not isinstance(body, dict):
            raise RemoteBackendError("Remote AsyncMQ backend returned a non-object response.")

        if status >= 400 or body.get("ok") is False:
            error = body.get("error", {})
            if isinstance(error, dict):
                error_type = error.get("type") or "RemoteBackendError"
                message = error.get("message") or f"Remote AsyncMQ backend request failed with HTTP {status}."
                method = error.get("method")
                suffix = f" during {method}" if method else ""
                raise RemoteBackendError(f"{error_type}{suffix}: {message}")
            raise RemoteBackendError(f"Remote AsyncMQ backend request failed with HTTP {status}.")

        if body.get("ok") is not True:
            raise RemoteBackendError("Remote AsyncMQ backend returned an invalid response envelope.")
        return body.get("data")

    async def _call_backend(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """
        Call one backend method through the remote control endpoint.

        Method names are percent-encoded before they become path segments even
        though the public client only calls trusted constant names.
        """
        path = f"/call/{urllib.parse.quote(method, safe='')}"
        return await self._request("POST", path, {"args": list(args), "kwargs": kwargs})

    async def health_check(self) -> None:
        """
        Check whether the worker-side remote backend endpoint is reachable.

        The dashboard readiness controller calls this method just like it would
        call a direct backend health check.
        """
        await self._request("GET", "/health")
        return None

    async def list_queues(self) -> list[str]:
        """
        Return queues visible to the worker-side backend.

        The dashboard uses this for overview, metrics, navigation, and queue
        detail pages.
        """
        return list(await self._call_backend("list_queues"))

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Return per-state queue counters from the remote backend.

        Backends may return only the counters they can compute efficiently; the
        dashboard keeps its existing fallback behavior.
        """
        return cast(dict[str, int], await self._call_backend("queue_stats", queue_name))

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """
        Return jobs for one queue and state from the remote backend.

        This is the fallback inspection path when a backend does not provide a
        paged ``inspect_jobs`` implementation.
        """
        return cast(list[dict[str, Any]], await self._call_backend("list_jobs", queue, state))

    async def inspect_jobs(
        self,
        queue_name: str,
        state: str,
        *,
        page: int = 1,
        size: int = 20,
        q: str = "",
        task: str = "",
        job_id: str = "",
        sort: str = "newest",
    ) -> JobInspectionPage:
        """
        Return a paged job inspection result from the remote backend.

        The JSON response is reconstructed into ``JobInspectionPage`` so the
        existing dashboard controllers keep their current contract.
        """
        data = await self._call_backend(
            "inspect_jobs",
            queue_name,
            state,
            page=page,
            size=size,
            q=q,
            task=task,
            job_id=job_id,
            sort=sort,
        )
        if not isinstance(data, dict):
            raise RemoteBackendError("Remote inspect_jobs returned an invalid payload.")
        return JobInspectionPage(**data)

    async def get_jobs(
        self,
        queue_name: str,
        types: list[str] | tuple[str, ...] | None = None,
        *,
        start: int = 0,
        end: int = -1,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Return jobs from one or more inspection buckets.

        This mirrors the backend getter used by Queue-oriented administration
        helpers and keeps BullMQ-style dashboard extensions remote-capable.
        """
        return cast(
            list[dict[str, Any]],
            await self._call_backend("get_jobs", queue_name, types, start=start, end=end, asc=asc),
        )

    async def get_job_counts(self, queue_name: str, *types: str) -> dict[str, int]:
        """
        Return per-bucket job counts for a queue.

        The variadic ``types`` arguments are preserved exactly so the remote
        call behaves like the direct backend method.
        """
        return cast(dict[str, int], await self._call_backend("get_job_counts", queue_name, *types))

    async def get_job_count_by_types(self, queue_name: str, *types: str) -> int:
        """
        Return the total count for selected job buckets.

        This keeps queue-level count helpers available when the dashboard uses
        a remote backend client.
        """
        return int(await self._call_backend("get_job_count_by_types", queue_name, *types))

    async def count(self, queue_name: str) -> int:
        """
        Return the queued workload count for a queue.

        The method is included for Queue API parity and future dashboard
        controls that use backend-shaped count operations.
        """
        return int(await self._call_backend("count", queue_name))

    async def get_job(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Return one stored job payload by identifier.

        Job detail pages use this direct lookup before falling back to state
        bucket scans.
        """
        return cast(dict[str, Any] | None, await self._call_backend("get_job", queue_name, job_id))

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Return the backend state for one job.

        The job detail page uses this when state is available outside the job
        payload itself.
        """
        return cast(str | None, await self._call_backend("get_job_state", queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Any:
        """
        Return a persisted job result from the remote backend.

        Result payloads stay JSON-normalized across the remote boundary.
        """
        return await self._call_backend("get_job_result", queue_name, job_id)

    async def retry_job(self, queue_name: str, job_id: str) -> Any:
        """
        Retry a failed job through the worker-side backend.

        The return value is backend-specific and is passed through unchanged
        after JSON normalization.
        """
        return await self._call_backend("retry_job", queue_name, job_id)

    async def remove_job(self, queue_name: str, job_id: str) -> Any:
        """
        Remove a job through the worker-side backend.

        Dashboard controllers keep their existing success and audit behavior
        around this backend-shaped method.
        """
        return await self._call_backend("remove_job", queue_name, job_id)

    async def cancel_job(self, queue_name: str, job_id: str) -> Any:
        """
        Cancel a job through the worker-side backend.

        This is exposed for the existing job action controller and mirrors the
        direct backend administrative surface.
        """
        return await self._call_backend("cancel_job", queue_name, job_id)

    async def pause_queue(self, queue_name: str) -> Any:
        """
        Pause a queue through the remote backend.

        The worker/control service owns the real backend mutation and therefore
        remains the only process that needs direct backend credentials.
        """
        return await self._call_backend("pause_queue", queue_name)

    async def resume_queue(self, queue_name: str) -> Any:
        """
        Resume a queue through the remote backend.

        This complements ``pause_queue`` for dashboard queue detail and list
        controls.
        """
        return await self._call_backend("resume_queue", queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Return whether a queue is paused.

        The result is coerced to ``bool`` because backend implementations may
        return truthy JSON values from different storage systems.
        """
        return bool(await self._call_backend("is_queue_paused", queue_name))

    async def list_workers(self) -> list[Any]:
        """
        Return worker heartbeat records visible to the remote backend.

        The dashboard accepts dictionaries and WorkerInfo-like shapes, so JSON
        dictionaries are sufficient across the remote boundary.
        """
        return cast(list[Any], await self._call_backend("list_workers"))

    async def list_repeatables(self, queue_name: str) -> list[Any]:
        """
        Return repeatable job definitions for a queue.

        Dataclass records are serialized into dictionaries by the server and
        normalized by the existing repeatables controller.
        """
        return cast(list[Any], await self._call_backend("list_repeatables", queue_name))

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Create or update a durable repeatable job definition.

        This supports queue-level repeatable helpers when a dashboard process
        talks to AsyncMQ through a remote backend client.
        """
        return await self._call_backend("upsert_repeatable", queue_name, job_def)

    async def create_repeatable(self, job_def: dict[str, Any]) -> Any:
        """
        Create a repeatable job through legacy backend contracts.

        Some dashboard fallback paths still use this shape for custom backend
        compatibility.
        """
        return await self._call_backend("create_repeatable", job_def)

    async def enqueue_repeatable(self, queue_name: str, payload: dict[str, Any], run_at: float) -> Any:
        """
        Enqueue a repeatable job through legacy backend contracts.

        The remote client exposes this for compatibility with dashboard create
        fallbacks that predate durable upsert support.
        """
        return await self._call_backend("enqueue_repeatable", queue_name, payload, run_at)

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Pause a repeatable job definition.

        The worker-side backend receives the same action definition that the
        direct dashboard backend would receive.
        """
        return await self._call_backend("pause_repeatable", queue_name, job_def)

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Resume a repeatable job definition.

        Backend-specific return values such as the next run timestamp are
        passed through to callers after JSON normalization.
        """
        return await self._call_backend("resume_repeatable", queue_name, job_def)

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> Any:
        """
        Remove a repeatable job definition.

        The second argument may be a full definition or a backend-specific
        repeatable identifier because existing backends support both shapes.
        """
        return await self._call_backend("remove_repeatable", queue_name, job_def)

    async def clean_jobs(
        self,
        queue_name: str,
        *,
        state: str = "completed",
        grace: float = 0,
        limit: int = 0,
    ) -> list[str]:
        """
        Clean jobs from a queue through the remote backend.

        This method is included for queue administration parity beyond the
        currently visible dashboard controls.
        """
        return cast(
            list[str],
            await self._call_backend("clean_jobs", queue_name, state=state, grace=grace, limit=limit),
        )

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> Any:
        """
        Purge jobs through backend-specific purge contracts.

        The return value is intentionally backend-specific for compatibility
        with existing direct backend implementations.
        """
        return await self._call_backend("purge", queue_name, state, older_than)

    async def drain_queue(self, queue_name: str, *, include_delayed: bool = False) -> list[str]:
        """
        Drain queued jobs through the remote backend.

        This mirrors the backend administration method and preserves the
        ``include_delayed`` keyword-only shape.
        """
        return cast(list[str], await self._call_backend("drain_queue", queue_name, include_delayed=include_delayed))

    async def obliterate_queue(self, queue_name: str, *, force: bool = False) -> list[str]:
        """
        Obliterate queue data through the remote backend.

        The ``force`` flag is passed to the worker-side backend where active
        job checks and storage deletion are actually performed.
        """
        return cast(list[str], await self._call_backend("obliterate_queue", queue_name, force=force))
