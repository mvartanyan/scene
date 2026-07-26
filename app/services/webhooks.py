from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import logging
import os
import random
import socket
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

from app.services.storage import SceneRepository

LOGGER = logging.getLogger("scene.webhooks")

SCENE_WEBHOOK_ENABLED_ENV = "SCENE_WEBHOOK_ENABLED"
SCENE_WEBHOOK_ENDPOINT_ID_ENV = "SCENE_WEBHOOK_ENDPOINT_ID"
SCENE_WEBHOOK_URL_ENV = "SCENE_WEBHOOK_URL"
SCENE_WEBHOOK_SECRET_ENV = "SCENE_WEBHOOK_SECRET"
SCENE_WEBHOOK_TIMEOUT_SECONDS_ENV = "SCENE_WEBHOOK_TIMEOUT_SECONDS"
SCENE_WEBHOOK_MAX_ATTEMPTS_ENV = "SCENE_WEBHOOK_MAX_ATTEMPTS"
SCENE_WEBHOOK_MAX_AGE_SECONDS_ENV = "SCENE_WEBHOOK_MAX_AGE_SECONDS"
SCENE_WEBHOOK_ALLOW_PRIVATE_URLS_ENV = "SCENE_WEBHOOK_ALLOW_PRIVATE_URLS"
SCENE_WEBHOOK_ALLOWED_HOSTS_ENV = "SCENE_WEBHOOK_ALLOWED_HOSTS"
SCENE_WEBHOOK_POLL_SECONDS_ENV = "SCENE_WEBHOOK_POLL_SECONDS"

RETRYABLE_STATUS_CODES = {408, 409, 425, 429}

Resolver = Callable[..., Sequence[Tuple[object, ...]]]
RequestSender = Callable[[str, bytes, Dict[str, str], float], int]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


@dataclass(frozen=True)
class WebhookConfig:
    enabled: bool
    endpoint_id: str = "spm"
    endpoint_url: str = ""
    secret: str = field(default="", repr=False)
    timeout_seconds: float = 10.0
    max_attempts: int = 8
    max_age_seconds: int = 86_400
    allow_private_urls: bool = False
    allowed_hosts: Tuple[str, ...] = ()
    poll_seconds: float = 2.0


@dataclass(frozen=True)
class DeliveryAttempt:
    outcome: str
    response_status: Optional[int]
    duration_seconds: float
    error_code: Optional[str]


class WebhookConfigurationError(ValueError):
    pass


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


def load_webhook_config() -> WebhookConfig:
    enabled = _env_bool(SCENE_WEBHOOK_ENABLED_ENV)
    allow_private = _env_bool(SCENE_WEBHOOK_ALLOW_PRIVATE_URLS_ENV)
    endpoint_id = os.environ.get(SCENE_WEBHOOK_ENDPOINT_ID_ENV, "spm").strip()
    endpoint_url = os.environ.get(SCENE_WEBHOOK_URL_ENV, "").strip()
    secret = os.environ.get(SCENE_WEBHOOK_SECRET_ENV, "")
    allowed_hosts = tuple(
        dict.fromkeys(
            host.strip().rstrip(".").lower()
            for host in os.environ.get(SCENE_WEBHOOK_ALLOWED_HOSTS_ENV, "").split(",")
            if host.strip()
        )
    )
    config = WebhookConfig(
        enabled=enabled,
        endpoint_id=endpoint_id,
        endpoint_url=endpoint_url,
        secret=secret,
        timeout_seconds=float(
            _env_int(
                SCENE_WEBHOOK_TIMEOUT_SECONDS_ENV,
                10,
                minimum=1,
                maximum=60,
            )
        ),
        max_attempts=_env_int(
            SCENE_WEBHOOK_MAX_ATTEMPTS_ENV,
            8,
            minimum=1,
            maximum=20,
        ),
        max_age_seconds=_env_int(
            SCENE_WEBHOOK_MAX_AGE_SECONDS_ENV,
            86_400,
            minimum=60,
            maximum=604_800,
        ),
        allow_private_urls=allow_private,
        allowed_hosts=allowed_hosts,
        poll_seconds=float(
            _env_int(
                SCENE_WEBHOOK_POLL_SECONDS_ENV,
                2,
                minimum=1,
                maximum=60,
            )
        ),
    )
    if not enabled:
        return config
    if (
        not endpoint_id
        or len(endpoint_id) > 100
        or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
            for character in endpoint_id
        )
    ):
        raise WebhookConfigurationError("webhook_endpoint_id_invalid")
    if not endpoint_url:
        raise WebhookConfigurationError("webhook_url_missing")
    if not secret.strip():
        raise WebhookConfigurationError("webhook_secret_missing")
    validate_webhook_url(
        endpoint_url,
        allow_private=allow_private,
        allowed_hosts=allowed_hosts,
    )
    return config


def validate_webhook_url(
    value: str,
    *,
    allow_private: bool = False,
    allowed_hosts: Sequence[str] = (),
    resolver: Resolver = socket.getaddrinfo,
) -> str:
    parsed = urlsplit(value.strip())
    allowed_schemes = {"https", "http"} if allow_private else {"https"}
    if parsed.scheme.lower() not in allowed_schemes:
        raise WebhookConfigurationError("webhook_url_scheme_invalid")
    if not parsed.hostname or parsed.username or parsed.password:
        raise WebhookConfigurationError("webhook_url_authority_invalid")
    hostname = parsed.hostname.rstrip(".").lower()
    normalized_allowed_hosts = {
        host.strip().rstrip(".").lower() for host in allowed_hosts if host.strip()
    }
    if normalized_allowed_hosts and hostname not in normalized_allowed_hosts:
        raise WebhookConfigurationError("webhook_url_host_forbidden")
    if parsed.query or parsed.fragment:
        raise WebhookConfigurationError("webhook_url_query_or_fragment_forbidden")
    try:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    except ValueError:
        raise WebhookConfigurationError("webhook_url_port_invalid") from None
    if not allow_private and port != 443:
        raise WebhookConfigurationError("webhook_url_port_invalid")
    try:
        addresses = resolver(
            hostname,
            port,
            type=socket.SOCK_STREAM,
        )
    except OSError:
        raise WebhookConfigurationError("webhook_url_dns_unavailable") from None
    resolved = {str(item[4][0]) for item in addresses if len(item) > 4 and item[4]}
    if not resolved:
        raise WebhookConfigurationError("webhook_url_dns_unavailable")
    if not allow_private:
        try:
            parsed_addresses = [ipaddress.ip_address(address) for address in resolved]
        except ValueError:
            raise WebhookConfigurationError("webhook_url_address_invalid") from None
        if any(not address.is_global for address in parsed_addresses):
            raise WebhookConfigurationError("webhook_url_address_forbidden")
    normalized_netloc = (
        f"[{hostname}]" if ":" in hostname else hostname
    )
    if parsed.port:
        normalized_netloc = f"{normalized_netloc}:{parsed.port}"
    return urlunsplit(
        (
            parsed.scheme.lower(),
            normalized_netloc,
            parsed.path or "/",
            "",
            "",
        )
    )


def canonical_event_bytes(marker: Dict[str, object]) -> bytes:
    occurred_at = str(marker["occurred_at"])
    event_id = str(marker["event_id"])
    envelope = {
        "schema_version": 1,
        "event_id": event_id,
        "event_type": str(marker["event_type"]),
        "occurred_at": occurred_at,
        "created_at": occurred_at,
        "delivery_id": event_id,
        "scene": {
            "environment": str(marker["environment"]).lower(),
            "run_id": str(marker["run_id"]),
            "batch_id": str(marker["batch_id"]),
        },
        "spm": {
            "ticket": str(marker["ticket"]),
            "criterion_id": str(marker["criterion_id"]),
            "invocation_id": str(marker["invocation_id"]),
        },
    }
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def signature_headers(
    *,
    event_id: str,
    raw_body: bytes,
    secret: str,
    timestamp: int,
) -> Dict[str, str]:
    timestamp_value = str(int(timestamp))
    signature = hmac.new(
        secret.encode("utf-8"),
        timestamp_value.encode("ascii") + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Scene-Event-Id": event_id,
        "X-Scene-Timestamp": timestamp_value,
        "X-Scene-Signature-Version": "1",
        "X-Scene-Signature": f"sha256={signature}",
    }


def materialize_webhook_events(
    repo: SceneRepository,
    config: WebhookConfig,
) -> int:
    materialized = 0
    for run in repo.list_runs_with_webhook_outbox():
        refreshed = repo.ensure_run_webhook_markers(str(run["id"])) or run
        markers = refreshed.get("webhook_outbox")
        if not isinstance(markers, list):
            continue
        for marker in markers:
            if not isinstance(marker, dict):
                continue
            raw_body = canonical_event_bytes(marker)
            event_id = str(marker["event_id"])
            event = repo.ensure_webhook_event(
                {
                    "id": event_id,
                    "event_type": marker["event_type"],
                    "occurred_at": marker["occurred_at"],
                    "run_id": marker["run_id"],
                    "batch_id": marker["batch_id"],
                    "body": raw_body.decode("ascii"),
                    "body_sha256": hashlib.sha256(raw_body).hexdigest(),
                    "created_at": _iso(_utcnow()),
                }
            )
            if config.enabled:
                repo.ensure_webhook_delivery(
                    event,
                    endpoint_id=config.endpoint_id,
                    endpoint_url=config.endpoint_url,
                )
            materialized += 1
    return materialized


def _stdlib_request_sender(
    url: str,
    body: bytes,
    headers: Dict[str, str],
    timeout_seconds: float,
) -> int:
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method="POST",
    )
    opener = urllib.request.build_opener(_NoRedirectHandler())
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def attempt_webhook_delivery(
    *,
    endpoint_url: str,
    event_id: str,
    raw_body: bytes,
    secret: str,
    timeout_seconds: float,
    allow_private_urls: bool = False,
    allowed_hosts: Sequence[str] = (),
    now: Optional[datetime] = None,
    resolver: Resolver = socket.getaddrinfo,
    sender: RequestSender = _stdlib_request_sender,
) -> DeliveryAttempt:
    started_at = time.monotonic()
    try:
        normalized_url = validate_webhook_url(
            endpoint_url,
            allow_private=allow_private_urls,
            allowed_hosts=allowed_hosts,
            resolver=resolver,
        )
        effective_now = now or _utcnow()
        status = sender(
            normalized_url,
            raw_body,
            signature_headers(
                event_id=event_id,
                raw_body=raw_body,
                secret=secret,
                timestamp=int(effective_now.timestamp()),
            ),
            timeout_seconds,
        )
    except WebhookConfigurationError as exc:
        return DeliveryAttempt(
            outcome="permanent_failure",
            response_status=None,
            duration_seconds=max(0.0, time.monotonic() - started_at),
            error_code=str(exc),
        )
    except (OSError, TimeoutError, urllib.error.URLError):
        return DeliveryAttempt(
            outcome="retry",
            response_status=None,
            duration_seconds=max(0.0, time.monotonic() - started_at),
            error_code="network_error",
        )
    if 200 <= status < 300:
        outcome = "succeeded"
        error_code = None
    elif status in RETRYABLE_STATUS_CODES or status >= 500:
        outcome = "retry"
        error_code = f"http_{status}"
    else:
        outcome = "permanent_failure"
        error_code = f"http_{status}"
    return DeliveryAttempt(
        outcome=outcome,
        response_status=status,
        duration_seconds=max(0.0, time.monotonic() - started_at),
        error_code=error_code,
    )


def retry_at(
    attempt_number: int,
    *,
    now: Optional[datetime] = None,
    random_value: Optional[float] = None,
) -> str:
    base = min(300.0, 5.0 * (2 ** min(max(attempt_number - 1, 0), 6)))
    jitter = (
        max(0.0, min(float(random_value), 1.0))
        if random_value is not None
        else random.random()
    )
    return _iso((now or _utcnow()) + timedelta(seconds=base + base * 0.2 * jitter))


class WebhookDeliveryWorker:
    def __init__(
        self,
        repo: SceneRepository,
        config: WebhookConfig,
        *,
        owner: Optional[str] = None,
        sender: RequestSender = _stdlib_request_sender,
        resolver: Resolver = socket.getaddrinfo,
    ) -> None:
        self.repo = repo
        self.config = config
        self.owner = owner or f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
        self.sender = sender
        self.resolver = resolver

    def run_cycle(self, *, now: Optional[datetime] = None) -> int:
        if not self.repo.acquire_webhook_worker_lease(self.owner, lease_seconds=30):
            return 0
        self.repo.report_webhook_worker_status(
            self.owner,
            enabled=self.config.enabled,
            configured_ok=True,
        )
        materialize_webhook_events(self.repo, self.config)
        if not self.repo.acquire_webhook_worker_lease(self.owner, lease_seconds=30):
            return 0
        processed = 0
        if self.config.enabled:
            for candidate in self.repo.list_due_webhook_deliveries(limit=20):
                if not self.repo.acquire_webhook_worker_lease(
                    self.owner,
                    lease_seconds=30,
                ):
                    break
                claimed = self.repo.claim_webhook_delivery(
                    str(candidate["id"]),
                    owner=self.owner,
                    lease_seconds=max(30, int(self.config.timeout_seconds) + 10),
                )
                if not claimed:
                    continue
                event = self.repo.get_webhook_event(str(claimed["event_id"]))
                if not event:
                    self.repo.complete_webhook_delivery_attempt(
                        str(claimed["id"]),
                        owner=self.owner,
                        outcome="permanent_failure",
                        response_status=None,
                        duration_seconds=0.0,
                        error="event_missing",
                        next_attempt_at=None,
                    )
                    self.repo.record_operational_counters(
                        {
                            "webhook_attempt_total": 1,
                            "webhook_failure_total": 1,
                        }
                    )
                    processed += 1
                    continue
                raw_body = str(event["body"]).encode("ascii")
                if not hmac.compare_digest(
                    hashlib.sha256(raw_body).hexdigest(),
                    str(event.get("body_sha256") or ""),
                ):
                    self.repo.complete_webhook_delivery_attempt(
                        str(claimed["id"]),
                        owner=self.owner,
                        outcome="permanent_failure",
                        response_status=None,
                        duration_seconds=0.0,
                        error="event_body_invalid",
                        next_attempt_at=None,
                    )
                    self.repo.record_operational_counters(
                        {
                            "webhook_attempt_total": 1,
                            "webhook_failure_total": 1,
                        }
                    )
                    processed += 1
                    continue
                attempt_now = now or _utcnow()
                result = attempt_webhook_delivery(
                    endpoint_url=str(claimed["endpoint_url"]),
                    event_id=str(event["id"]),
                    raw_body=raw_body,
                    secret=self.config.secret,
                    timeout_seconds=self.config.timeout_seconds,
                    allow_private_urls=self.config.allow_private_urls,
                    allowed_hosts=self.config.allowed_hosts,
                    now=attempt_now,
                    resolver=self.resolver,
                    sender=self.sender,
                )
                attempt_number = (
                    int(claimed.get("generation_attempt_count") or 0) + 1
                )
                created_at = datetime.fromisoformat(
                    str(
                        claimed.get("generation_started_at")
                        or claimed["created_at"]
                    ).replace("Z", "+00:00")
                )
                age_seconds = max(
                    0.0,
                    (attempt_now - created_at.astimezone(timezone.utc)).total_seconds(),
                )
                outcome = result.outcome
                error_code = result.error_code
                next_attempt_at = None
                if outcome == "retry":
                    if (
                        attempt_number >= self.config.max_attempts
                        or age_seconds >= self.config.max_age_seconds
                    ):
                        outcome = "permanent_failure"
                        error_code = "retry_policy_exhausted"
                    else:
                        next_attempt_at = retry_at(attempt_number, now=attempt_now)
                self.repo.complete_webhook_delivery_attempt(
                    str(claimed["id"]),
                    owner=self.owner,
                    outcome=outcome,
                    response_status=result.response_status,
                    duration_seconds=result.duration_seconds,
                    error=error_code,
                    next_attempt_at=next_attempt_at,
                )
                counters = {"webhook_attempt_total": 1}
                if outcome == "succeeded":
                    counters["webhook_success_total"] = 1
                else:
                    counters["webhook_failure_total"] = 1
                self.repo.record_operational_counters(counters)
                self.repo.acquire_webhook_worker_lease(
                    self.owner,
                    lease_seconds=30,
                )
                processed += 1
        self.repo.report_webhook_worker_status(
            self.owner,
            enabled=self.config.enabled,
            configured_ok=True,
        )
        return processed


def safe_config_error_code(exc: Exception) -> str:
    if isinstance(exc, WebhookConfigurationError):
        return str(exc)
    return "webhook_configuration_invalid"


def run_worker_forever(
    *,
    repo: Optional[SceneRepository] = None,
) -> None:
    from app.services.storage import get_repository

    repository = repo or get_repository()
    owner = f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
    while True:
        try:
            config = load_webhook_config()
            worker = WebhookDeliveryWorker(
                repository,
                config,
                owner=owner,
            )
            worker.run_cycle()
            delay = config.poll_seconds
        except Exception as exc:  # noqa: BLE001 - worker must remain observable.
            code = safe_config_error_code(exc)
            LOGGER.error("webhook_worker_cycle_failed code=%s", code)
            if repository.acquire_webhook_worker_lease(owner, lease_seconds=30):
                repository.report_webhook_worker_status(
                    owner,
                    enabled=_env_bool(SCENE_WEBHOOK_ENABLED_ENV),
                    configured_ok=False,
                    error_code=code,
                )
            delay = 5.0
        time.sleep(delay)
