import json
import sys
import traceback
from pathlib import Path

from playwright.sync_api import sync_playwright


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
        "trace": None,
        "video": None,
        "error": None,
    }

    try:
        with sync_playwright() as playwright:
            browser_name = config.get("browser", "chromium")
            if not hasattr(playwright, browser_name):
                raise RuntimeError(f"Unsupported browser '{browser_name}'")
            browser_type = getattr(playwright, browser_name)
            browser = browser_type.launch(headless=True)

            viewport = config.get("viewport") or {"width": 1280, "height": 720}
            context = browser.new_context(
                viewport={"width": int(viewport["width"]), "height": int(viewport["height"])},
                record_video_dir=str(videos_dir),
            )
            context.tracing.start(screenshots=True, snapshots=True, sources=True)

            page = context.new_page()
            goto_timeout = int(config.get("goto_timeout_ms", 45000))
            page.goto(config["url"], wait_until="networkidle", timeout=goto_timeout)

            preparatory_js = config.get("preparatory_js")
            if preparatory_js:
                page.evaluate(preparatory_js)

            task_js = config.get("task_js")
            if task_js:
                page.evaluate(task_js)

            post_wait = int(config.get("post_wait_ms", 500))
            if post_wait:
                page.wait_for_timeout(post_wait)

            screenshot_path = artifacts_dir / "observed.png"
            page.screenshot(path=str(screenshot_path), full_page=True)

            trace_path = artifacts_dir / "trace.zip"
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

            result.update(
                {
                    "status": "ok",
                    "screenshot": screenshot_path.name,
                    "trace": trace_path.name,
                    "video": video_relative,
                }
            )
    except Exception:
        result["error"] = traceback.format_exc()

    json.dump(result, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main(sys.argv[1])
