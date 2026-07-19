from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


class FakeResponse:
    def __init__(self, etag: str) -> None:
        self.headers = {"ETag": etag}
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def runner_script(monkeypatch: pytest.MonkeyPatch):
    playwright_package = types.ModuleType("playwright")
    sync_api = types.ModuleType("playwright.sync_api")
    sync_api.sync_playwright = lambda: None
    playwright_package.sync_api = sync_api
    monkeypatch.setitem(sys.modules, "playwright", playwright_package)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)

    path = Path(__file__).parents[1] / "app" / "services" / "runner_script.py"
    spec = importlib.util.spec_from_file_location("scene_runner_script_transfer_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _output(kind: str, filename: str, content_type: str) -> dict[str, object]:
    return {
        "method": "PUT",
        "key": f"scene/staging/executions/execution-1/{kind}/{filename}",
        "filename": filename,
        "content_type": content_type,
        "url": (
            f"https://s3.test/private/{kind}/{filename}"
            f"?X-Amz-Credential=runner-secret&X-Amz-Signature={kind}-secret"
        ),
    }


@pytest.mark.unit
def test_presigned_outputs_upload_before_result_and_receipts_reach_callback(
    runner_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)

    observed = b"observed-png"
    trace = b"playwright-trace"
    video = b"session-video"
    (tmp_path / "observed.png").write_bytes(observed)
    (tmp_path / "trace.zip").write_bytes(trace)
    (tmp_path / "videos").mkdir()
    (tmp_path / "videos" / "generated.webm").write_bytes(video)

    outputs = {
        "observed": _output("observed", "observed.png", "image/png"),
        "reference": _output("reference", "reference.png", "image/png"),
        "trace": _output("trace", "trace.zip", "application/zip"),
        "video": _output("video", "video.webm", "video/webm"),
        "result": _output("result", "result.json", "application/json"),
    }
    outputs["observed"]["kind"] = "ignored-entry-kind"
    outputs["observed"]["headers"] = {"Content-Type": "application/octet-stream"}
    config = {"artifact_transfer": {"version": 1, "outputs": outputs}}
    result = {
        "status": "ok",
        "screenshot": "observed.png",
        "reference": None,
        "trace": "trace.zip",
        "video": "videos/generated.webm",
        "error": None,
    }

    requests = []

    def fake_urlopen(request, timeout: int):
        data = request.data.read() if hasattr(request.data, "read") else request.data
        response = FakeResponse(f'"etag-{len(requests) + 1}"')
        requests.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "headers": {name.lower(): value for name, value in request.header_items()},
                "data": data,
                "timeout": timeout,
                "response": response,
            }
        )
        return response

    monkeypatch.setattr(runner_script.urllib.request, "urlopen", fake_urlopen)

    callback_result = runner_script._finalize_result(config, tmp_path, result)

    assert [request["method"] for request in requests] == ["PUT"] * 4
    assert [request["data"] for request in requests[:3]] == [observed, trace, video]
    assert [request["headers"]["content-type"] for request in requests] == [
        "image/png",
        "application/zip",
        "video/webm",
        "application/json",
    ]
    assert [request["headers"]["content-length"] for request in requests[:3]] == [
        str(len(observed)),
        str(len(trace)),
        str(len(video)),
    ]

    receipts = callback_result["uploads"]
    assert list(receipts) == ["observed", "trace", "video", "result"]
    assert receipts["observed"] == {
        "key": outputs["observed"]["key"],
        "etag": "etag-1",
        "size_bytes": len(observed),
        "sha256": hashlib.sha256(observed).hexdigest(),
    }

    uploaded_result = requests[-1]["data"]
    uploaded_payload = json.loads(uploaded_result)
    assert list(uploaded_payload["uploads"]) == ["observed", "trace", "video"]
    assert "result" not in uploaded_payload["uploads"]
    assert (tmp_path / "result.json").read_bytes() == uploaded_result
    assert receipts["result"]["size_bytes"] == len(uploaded_result)
    assert receipts["result"]["sha256"] == hashlib.sha256(uploaded_result).hexdigest()

    monkeypatch.setattr(runner_script, "CALLBACK_URL", "https://scene.test/executions/1/complete")
    monkeypatch.setattr(runner_script, "CALLBACK_TOKEN", "callback-token")
    assert runner_script._post_result(callback_result)
    callback_request = requests[-1]
    assert callback_request["method"] == "POST"
    callback_payload = json.loads(callback_request["data"])
    assert callback_payload == {"token": "callback-token", "result": callback_result}
    assert "https://" not in callback_request["data"].decode("utf-8")

    output = capsys.readouterr().out
    assert "runner-secret" not in output
    assert "X-Amz-" not in output
    assert "Signature=" not in output


@pytest.mark.unit
def test_no_transfer_manifest_preserves_local_result_without_network(
    runner_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def unexpected_urlopen(*args, **kwargs):
        raise AssertionError("no artifact upload should be attempted")

    monkeypatch.setattr(runner_script.urllib.request, "urlopen", unexpected_urlopen)
    result = {"status": "ok", "screenshot": "observed.png", "error": None}

    callback_result = runner_script._finalize_result({}, tmp_path, result)

    assert callback_result == result
    assert "uploads" not in callback_result
    assert json.loads((tmp_path / "result.json").read_text(encoding="utf-8")) == result


@pytest.mark.unit
def test_upload_failure_is_diagnosable_without_exposing_presigned_query(
    runner_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "observed.png").write_bytes(b"observed")
    output = _output("observed", "observed.png", "image/png")
    config = {"artifact_transfer": {"outputs": {"observed": output}}}
    result = {"status": "ok", "screenshot": "observed.png", "error": None}

    def failing_urlopen(request, timeout: int):
        raise RuntimeError(f"PUT failed for {request.full_url}")

    monkeypatch.setattr(runner_script.urllib.request, "urlopen", failing_urlopen)

    callback_result = runner_script._finalize_result(config, tmp_path, result)

    assert callback_result["status"] == "error"
    assert callback_result["uploads"] == {}
    assert callback_result["artifact_transfer_errors"][0]["kind"] == "observed"
    diagnostic = json.dumps(callback_result)
    output_text = capsys.readouterr().out
    for rendered in (diagnostic, output_text):
        assert "runner-secret" not in rendered
        assert "observed-secret" not in rendered
        assert "X-Amz-Credential" not in rendered
        assert "X-Amz-Signature" not in rendered
    assert "RuntimeError" in callback_result["error"]
    assert "<redacted-url>" in callback_result["error"]


@pytest.mark.unit
def test_callback_payload_strips_urls_from_diagnostics(
    runner_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    callback_bodies = []

    def fake_urlopen(request, timeout: int):
        callback_bodies.append(request.data)
        return FakeResponse('"callback-etag"')

    monkeypatch.setattr(runner_script.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(runner_script, "CALLBACK_URL", "https://scene.test/executions/1/complete")
    monkeypatch.setattr(runner_script, "CALLBACK_TOKEN", "callback-token")
    result = {
        "status": "error",
        "error": "PUT failed for https://s3.test/private/result?X-Amz-Signature=secret",
        "uploads": {},
    }

    assert runner_script._post_result(result)

    rendered = callback_bodies[0].decode("utf-8")
    callback = json.loads(rendered)
    assert "https://" not in rendered
    assert "X-Amz-Signature" not in rendered
    assert callback["result"]["error"] == "PUT failed for <redacted-url>"


@pytest.mark.unit
def test_manifest_path_cannot_escape_artifact_directory(
    runner_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / "outside.png"
    outside.write_bytes(b"private")
    output = _output("observed", "observed.png", "image/png")
    output["path"] = "../outside.png"
    config = {"artifact_transfer": {"outputs": {"observed": output}}}
    result = {"status": "ok", "screenshot": "observed.png", "error": None}

    def unexpected_urlopen(*args, **kwargs):
        raise AssertionError("unsafe artifact must not be uploaded")

    monkeypatch.setattr(runner_script.urllib.request, "urlopen", unexpected_urlopen)

    callback_result = runner_script._finalize_result(config, tmp_path, result)

    assert callback_result["status"] == "error"
    assert "leaves the artifact directory" in callback_result["error"]
