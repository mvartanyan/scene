import json
import os
import sys
import traceback
import urllib.error
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright


CALLBACK_URL = os.environ.get("SCENE_CALLBACK_URL")
CALLBACK_TOKEN = os.environ.get("SCENE_CALLBACK_TOKEN")
EXECUTION_ID = os.environ.get("SCENE_EXECUTION_ID")


def _log(message: str) -> None:
    print(f"[scene-runner] {message}", flush=True)


def _post_result(payload: dict) -> bool:
    if not CALLBACK_URL or not CALLBACK_TOKEN:
        return False
    try:
        data = json.dumps({"token": CALLBACK_TOKEN, "result": payload}).encode("utf-8")
        req = urllib.request.Request(
            CALLBACK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=30)
        return True
    except Exception:
        traceback.print_exc()
        return False


def _run_actions(page, actions, label: str) -> None:
    if not isinstance(actions, list) or not actions:
        return
    total = len(actions)
    for index, raw in enumerate(actions, 1):
        if not isinstance(raw, dict):
            continue
        action_type = str(raw.get("type") or "").strip().lower()
        if not action_type:
            continue
        action_label = f"{label} action {index}/{total}"
        try:
            action_repr = json.dumps({k: v for k, v in raw.items()}, sort_keys=True, default=str)
        except Exception:
            action_repr = str(raw)
        _log(f"{action_label}: {action_type} {action_repr}")
        try:
            timeout = raw.get("timeout_ms")
            if timeout is not None:
                try:
                    timeout = int(timeout)
                except Exception:
                    timeout = None
            if action_type == "click":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'click' requires selector")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                button = raw.get("button")
                if button:
                    kwargs["button"] = str(button)
                if "force" in raw:
                    kwargs["force"] = bool(raw.get("force"))
                page.click(selector, **kwargs)
            elif action_type in {"dblclick", "double_click"}:
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'double_click' requires selector")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.dblclick(selector, **kwargs)
            elif action_type == "hover":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'hover' requires selector")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.hover(selector, **kwargs)
            elif action_type == "fill":
                selector = raw.get("selector")
                value = raw.get("value")
                if not selector:
                    raise ValueError("Action 'fill' requires selector")
                if value is None:
                    value = ""
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.fill(selector, str(value), **kwargs)
            elif action_type == "type":
                selector = raw.get("selector")
                text_value = raw.get("text", raw.get("value"))
                if not selector:
                    raise ValueError("Action 'type' requires selector")
                if text_value is None:
                    text_value = ""
                delay = raw.get("delay_ms")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                if delay is not None:
                    kwargs["delay"] = int(delay)
                page.type(selector, str(text_value), **kwargs)
            elif action_type == "press":
                selector = raw.get("selector")
                key = raw.get("key")
                if not selector or not key:
                    raise ValueError("Action 'press' requires selector and key")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.press(selector, str(key), **kwargs)
            elif action_type == "focus":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'focus' requires selector")
                page.focus(selector)
            elif action_type == "check":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'check' requires selector")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.check(selector, **kwargs)
            elif action_type == "uncheck":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'uncheck' requires selector")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.uncheck(selector, **kwargs)
            elif action_type == "wait_for_selector":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'wait_for_selector' requires selector")
                state = raw.get("state")
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                if state:
                    kwargs["state"] = str(state)
                page.wait_for_selector(selector, **kwargs)
            elif action_type == "wait_for_load_state":
                state = raw.get("state") or "networkidle"
                kwargs = {}
                if timeout is not None:
                    kwargs["timeout"] = timeout
                page.wait_for_load_state(state, **kwargs)
            elif action_type == "wait":
                duration = raw.get("wait_ms", raw.get("duration_ms", 0))
                page.wait_for_timeout(int(duration))
            elif action_type == "evaluate":
                script = raw.get("script") or raw.get("value")
                if not script:
                    raise ValueError("Action 'evaluate' requires script")
                if "args" in raw:
                    page.evaluate(script, raw.get("args"))
                elif "arg" in raw:
                    page.evaluate(script, raw.get("arg"))
                else:
                    page.evaluate(script)
            elif action_type == "remove":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'remove' requires selector")
                page.evaluate("selector => { const el = document.querySelector(selector); if (el) el.remove(); }", selector)
            elif action_type == "set_attribute":
                selector = raw.get("selector")
                attribute = raw.get("attribute")
                value = raw.get("value")
                if not selector or attribute is None:
                    raise ValueError("Action 'set_attribute' requires selector and attribute")
                page.evaluate("([selector, attribute, value]) => { const el = document.querySelector(selector); if (el) el.setAttribute(attribute, value ?? ""); }", [selector, attribute, value])
            elif action_type == "disable_animations":
                css = raw.get("css", "*,*::before,*::after{animation:none!important;transition:none!important;scroll-behavior:auto!important;}")
                page.add_style_tag(content=str(css))
            elif action_type == "scroll_into_view":
                selector = raw.get("selector")
                if not selector:
                    raise ValueError("Action 'scroll_into_view' requires selector")
                page.evaluate("selector => { const el = document.querySelector(selector); if (el) el.scrollIntoView({behavior: 'instant', block: 'center', inline: 'center'}); }", selector)
            else:
                raise ValueError(f"Unsupported action type '{action_type}'")
            _log(f"{action_label} completed")
        except Exception as action_exc:
            _log(f"{action_label} failed: {action_exc}")
            raise


def main(config_path: str) -> None:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    artifacts_dir = Path(config.get("artifacts_dir", ".")).resolve()
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    videos_dir = artifacts_dir / "videos"
    videos_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "status": "error",
        "screenshot": None,
        "reference": None,
        "trace": None,
        "video": None,
        "error": None,
    }

    try:
        target_url = config.get("url", "<unknown>")
        _log(f"Starting execution for {target_url} (execution_id={EXECUTION_ID or 'n/a'})")
        with sync_playwright() as playwright:
            browser_name = config.get("browser", "chromium")
            if not hasattr(playwright, browser_name):
                raise RuntimeError(f"Unsupported browser '{browser_name}'")
            browser_type = getattr(playwright, browser_name)
            launch_kwargs = {"headless": True}
            if browser_name == "chromium":
                launch_kwargs["args"] = ["--disable-dev-shm-usage", "--no-sandbox"]
            browser = browser_type.launch(**launch_kwargs)
            _log(f"Launched {browser_name} browser")

            viewport = config.get("viewport") or {"width": 1280, "height": 720}
            context_kwargs = {
                "viewport": {"width": int(viewport["width"]), "height": int(viewport["height"])},
                "record_video_dir": str(videos_dir),
            }
            http_credentials = config.get("http_credentials")
            if http_credentials:
                context_kwargs["http_credentials"] = {
                    "username": http_credentials.get("username"),
                    "password": http_credentials.get("password"),
                }
            context = None
            page = None
            screenshot_path = artifacts_dir / "observed.png"

            max_attempts = 3
            last_error = None
            for attempt in range(max_attempts):
                if context is not None:
                    try:
                        context.tracing.stop()
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
                    context = None

                attempt_no = attempt + 1
                if browser.is_connected():
                    _log("Closing existing browser instance before retry")
                    try:
                        browser.close()
                    except Exception:
                        pass
                browser = browser_type.launch(**launch_kwargs)
                _log(f"Launched {browser_name} browser (attempt {attempt_no})")

                context = browser.new_context(**context_kwargs)
                context.tracing.start(screenshots=True, snapshots=True, sources=True)
                _log(f"Navigation target {target_url} (attempt {attempt_no})")

                page = context.new_page()
                goto_timeout = int(config.get("goto_timeout_ms", 45000))
                try:
                    response = page.goto(config["url"], wait_until="networkidle", timeout=goto_timeout)
                    status_code = "unknown"
                    final_url = config["url"]
                    if response is not None:
                        try:
                            status_code = str(response.status)
                        except Exception:
                            status_code = "unknown"
                        try:
                            final_url = response.url
                        except Exception:
                            final_url = config["url"]
                    _log(f"Page loaded successfully (status={status_code}, url={final_url})")

                    _run_actions(page, config.get("preparatory_actions"), "Preparatory")

                    preparatory_js = config.get("preparatory_js")
                    if isinstance(preparatory_js, str):
                        cleaned_preparatory = preparatory_js.strip()
                        if cleaned_preparatory and cleaned_preparatory.lower() != "none":
                            _log("Executing preparatory JavaScript")
                            try:
                                page.evaluate(cleaned_preparatory)
                                _log("Preparatory JavaScript executed successfully")
                            except Exception as prep_exc:
                                _log(f"Preparatory JavaScript failed: {prep_exc}")
                                raise
                    elif preparatory_js:
                        _log("Executing preparatory JavaScript (object)")
                        try:
                            page.evaluate(preparatory_js)
                            _log("Preparatory JavaScript executed successfully")
                        except Exception as prep_exc:
                            _log(f"Preparatory JavaScript failed: {prep_exc}")
                            raise

                    task_js = config.get("task_js")
                    if isinstance(task_js, str):
                        cleaned_task = task_js.strip()
                        if cleaned_task and cleaned_task.lower() != "none":
                            _log("Executing task JavaScript")
                            try:
                                page.evaluate(cleaned_task)
                                _log("Task JavaScript executed successfully")
                            except Exception as task_exc:
                                _log(f"Task JavaScript failed: {task_exc}")
                                raise
                    elif task_js:
                        _log("Executing task JavaScript (object)")
                        try:
                            page.evaluate(task_js)
                            _log("Task JavaScript executed successfully")
                        except Exception as task_exc:
                            _log(f"Task JavaScript failed: {task_exc}")
                            raise

                    _run_actions(page, config.get("task_actions"), "Task")

                    post_wait = int(config.get("post_wait_ms", 500))
                    if post_wait:
                        _log(f"Waiting {post_wait}ms before capture")
                        page.wait_for_timeout(post_wait)

                    _log("Capturing observed screenshot")
                    page.screenshot(path=str(screenshot_path), full_page=True, timeout=60000)
                    _log("Screenshot captured successfully")
                    reference_cfg = config.get("reference")
                    if reference_cfg and reference_cfg.get("url"):
                        reference_url = reference_cfg.get("url")
                        reference_path = artifacts_dir / "reference.png"
                        ref_page = None
                        _log(f"Capturing reference screenshot from {reference_url}")
                        try:
                            ref_page = context.new_page()
                            ref_page.goto(reference_url, wait_until="networkidle", timeout=goto_timeout)
                            _run_actions(ref_page, config.get("preparatory_actions"), "Preparatory (reference)")
                            reference_js = config.get("preparatory_js")
                            if isinstance(reference_js, str):
                                cleaned_reference = reference_js.strip()
                                if cleaned_reference and cleaned_reference.lower() != "none":
                                    _log("Executing preparatory JavaScript on reference")
                                    try:
                                        ref_page.evaluate(cleaned_reference)
                                        _log("Reference preparatory JavaScript executed successfully")
                                    except Exception as ref_js_exc:
                                        _log(f"Reference preparatory JavaScript failed: {ref_js_exc}")
                                        raise
                            ref_wait = int(reference_cfg.get("post_wait_ms", config.get("post_wait_ms", 500)))
                            if ref_wait:
                                ref_page.wait_for_timeout(ref_wait)
                            ref_page.screenshot(path=str(reference_path), full_page=True, timeout=60000)
                            result["reference"] = reference_path.name
                            _log("Reference screenshot captured successfully")
                        except Exception as ref_exc:
                            _log(f"Reference capture failed: {ref_exc}")
                        finally:
                            if ref_page:
                                try:
                                    ref_page.close()
                                except Exception:
                                    pass
                    break
                except Exception as exc:
                    last_error = exc
                    _log(f"Screenshot attempt {attempt_no} failed: {type(exc).__name__}: {exc}")
                    traceback.print_exc()
                    try:
                        page.close()
                    except Exception:
                        pass
                    page = None
                    continue
            else:
                raise RuntimeError(f"Unable to capture screenshot after {max_attempts} attempts: {last_error}")

            trace_path = artifacts_dir / "trace.zip"
            _log("Stopping Playwright trace")
            context.tracing.stop(path=str(trace_path))

            video_relative = None
            video_handle = None
            try:
                video_handle = page.video()
            except Exception:  # noqa: BLE001
                video_handle = None

            page.close()
            context.close()

            if video_handle:
                try:
                    video_path = Path(video_handle.path())
                    video_relative = str(video_path.relative_to(artifacts_dir))
                except Exception:  # noqa: BLE001
                    video_relative = None

            browser.close()
            _log("Browser session closed")

            result.update(
                {
                    "status": "ok",
                    "screenshot": screenshot_path.name,
                    "reference": result.get("reference"),
                    "trace": trace_path.name,
                    "video": video_relative,
                }
            )
            _log("Execution completed successfully")
    except Exception:
        _log("Execution failed; see traceback below")
        result["error"] = traceback.format_exc()

    if not _post_result(result):
        _log("Callback failed; emitting JSON payload to stdout")
        json.dump(result, sys.stdout)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main(sys.argv[1])
