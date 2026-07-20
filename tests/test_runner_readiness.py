from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
RUNNER_SCRIPT = ROOT / "app" / "services" / "runner_script.py"
READINESS_SCRIPT = ROOT / "scripts" / "runner_readiness.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_fake_playwright(monkeypatch: pytest.MonkeyPatch, launches: list[tuple[str, dict]]) -> None:
    class FakeLocator:
        def inner_text(self) -> str:
            return "ready"

    class FakePage:
        def set_content(self, content: str) -> None:
            assert "SCENE readiness" in content

        def title(self) -> str:
            return "SCENE readiness"

        def locator(self, selector: str) -> FakeLocator:
            assert selector == "h1"
            return FakeLocator()

        def screenshot(self, *, path: str) -> None:
            Path(path).write_bytes(b"png")

    class FakeBrowser:
        def new_page(self, *, viewport: dict) -> FakePage:
            assert viewport == {"width": 1280, "height": 720}
            return FakePage()

        def close(self) -> None:
            return None

    class FakeBrowserType:
        def __init__(self, name: str) -> None:
            self.name = name

        def launch(self, **kwargs) -> FakeBrowser:
            launches.append((self.name, kwargs))
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeBrowserType("chromium")
        firefox = FakeBrowserType("firefox")

    class FakePlaywrightContext:
        def __enter__(self) -> FakePlaywright:
            return FakePlaywright()

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

    playwright_package = types.ModuleType("playwright")
    sync_api = types.ModuleType("playwright.sync_api")
    sync_api.sync_playwright = FakePlaywrightContext
    playwright_package.sync_api = sync_api
    monkeypatch.setitem(sys.modules, "playwright", playwright_package)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)


@pytest.mark.unit
def test_chromium_uses_mounted_shm_and_explicit_unsandboxed_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_playwright(monkeypatch, [])
    runner = _load_module("scene_runner_launch_contract_test", RUNNER_SCRIPT)

    assert runner.browser_launch_kwargs("chromium") == {
        "headless": True,
        "chromium_sandbox": False,
        "ignore_default_args": ["--disable-dev-shm-usage"],
    }
    assert runner.browser_launch_kwargs("firefox") == {"headless": True}

    source = RUNNER_SCRIPT.read_text(encoding="utf-8")
    assert source.count("browser_type.launch(") == 1
    assert source.count("launch_browser(browser_type, browser_name)") == 2


@pytest.mark.unit
def test_readiness_loads_the_production_browser_launch_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    launches: list[tuple[str, dict]] = []
    _install_fake_playwright(monkeypatch, launches)
    readiness = _load_module("scene_runner_readiness_test", READINESS_SCRIPT)

    result = readiness.check_browser_launches(
        str(tmp_path),
        runner_script=str(RUNNER_SCRIPT),
    )

    assert result["ok"] is True
    assert launches == [
        (
            "chromium",
            {
                "headless": True,
                "chromium_sandbox": False,
                "ignore_default_args": ["--disable-dev-shm-usage"],
            },
        ),
        ("firefox", {"headless": True}),
    ]
    assert not list(tmp_path.glob(".*-readiness-*.png"))


@pytest.mark.unit
def test_k3s_readiness_invokes_shared_browser_smoke() -> None:
    manifest = (ROOT / "deploy" / "k3s" / "runner-readiness-job.yaml").read_text(
        encoding="utf-8"
    )

    assert "--browser-smoke" in manifest
    assert "--runner-script /opt/scene/runner.py" in manifest
    assert "getattr(playwright, browser_name).launch" not in manifest
