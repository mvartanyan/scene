from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from app.routes.api import _threshold_result
from app.schemas import ExecutionStatus, RunStatus
from app.services.orchestrator import DiffEngine


def _save(path: Path, size: tuple[int, int], color: tuple[int, int, int, int]) -> None:
    Image.new("RGBA", size, color).save(path)


@pytest.mark.unit
def test_diff_engine_identical_images_have_no_diff(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    _save(baseline, (4, 3), (32, 64, 128, 255))
    _save(observed, (4, 3), (32, 64, 128, 255))

    stats = DiffEngine().generate(baseline, observed, diff, heatmap)

    assert stats["pixel_count"] == 0
    assert stats["percentage"] == 0.0
    assert stats["normalization_action"] == "none"
    assert stats["baseline_original_dimensions"] == {"width": 4, "height": 3}
    assert stats["observed_original_dimensions"] == {"width": 4, "height": 3}
    assert stats["normalized_dimensions"] == {"width": 4, "height": 3}
    with Image.open(diff) as diff_img, Image.open(heatmap) as heatmap_img:
        assert diff_img.size == (4, 3)
        assert heatmap_img.size == (4, 3)


@pytest.mark.unit
def test_diff_engine_normalizes_one_pixel_dimension_mismatch_without_resize(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    _save(baseline, (3, 3), (200, 200, 200, 255))
    _save(observed, (4, 2), (200, 200, 200, 255))

    stats = DiffEngine().generate(baseline, observed, diff, heatmap)

    assert stats["pixel_count"] == 0
    assert stats["normalization_action"] == "normalize_to_max_canvas"
    assert stats["baseline_original_dimensions"] == {"width": 3, "height": 3}
    assert stats["observed_original_dimensions"] == {"width": 4, "height": 2}
    assert stats["normalized_dimensions"] == {"width": 4, "height": 3}
    assert stats["baseline_normalization_action"] == "pad_to_max_canvas"
    assert stats["observed_normalization_action"] == "pad_to_max_canvas"
    with Image.open(diff) as diff_img, Image.open(heatmap) as heatmap_img:
        assert diff_img.size == (4, 3)
        assert heatmap_img.size == (4, 3)


@pytest.mark.unit
def test_diff_engine_heatmap_is_stable_for_one_pixel_canvas_padding(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    fill = (10, 20, 30, 255)
    _save(baseline, (4, 4), fill)
    _save(observed, (3, 4), fill)

    stats = DiffEngine().generate(baseline, observed, diff, heatmap)

    assert stats["pixel_count"] == 0
    assert stats["percentage"] == 0.0
    assert stats["normalization_action"] == "normalize_to_max_canvas"
    assert stats["normalized_dimensions"] == {"width": 4, "height": 4}
    with Image.open(heatmap) as heatmap_img:
        assert heatmap_img.size == (4, 4)
        assert set(heatmap_img.convert("RGBA").getdata()) == {fill}


@pytest.mark.unit
def test_diff_engine_counts_exactly_one_unstable_pixel_above_tolerance(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    _save(baseline, (3, 3), (80, 90, 100, 255))
    image = Image.new("RGBA", (3, 3), (80, 90, 100, 255))
    image.putpixel((1, 1), (91, 90, 100, 255))
    image.save(observed)

    stats = DiffEngine(pixel_tolerance=10).generate(baseline, observed, diff, heatmap)
    tolerated_stats = DiffEngine(pixel_tolerance=11).generate(
        baseline,
        observed,
        tmp_path / "tolerated_diff.png",
        tmp_path / "tolerated_heatmap.png",
    )

    assert stats["pixel_count"] == 1
    assert stats["percentage"] == 11.1111
    assert stats["pixel_tolerance"] == 10
    assert tolerated_stats["pixel_count"] == 0


@pytest.mark.unit
def test_diff_engine_ignores_hidden_rgb_in_transparent_pngs(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    _save(baseline, (2, 2), (255, 0, 0, 0))
    _save(observed, (2, 2), (0, 0, 255, 0))

    stats = DiffEngine().generate(baseline, observed, diff, heatmap)

    assert stats["pixel_count"] == 0
    assert stats["baseline_normalization_action"] == "sanitize_transparent_pixels"
    assert stats["observed_normalization_action"] == "sanitize_transparent_pixels"


@pytest.mark.unit
def test_diff_engine_tolerance_ignores_sub_threshold_noise(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.png"
    observed = tmp_path / "observed.png"
    diff = tmp_path / "diff.png"
    heatmap = tmp_path / "heatmap.png"
    _save(baseline, (2, 2), (100, 100, 100, 255))
    _save(observed, (2, 2), (104, 103, 101, 255))

    stats = DiffEngine(pixel_tolerance=5).generate(baseline, observed, diff, heatmap)
    strict_stats = DiffEngine(pixel_tolerance=3).generate(
        baseline,
        observed,
        tmp_path / "strict_diff.png",
        tmp_path / "strict_heatmap.png",
    )

    assert stats["pixel_count"] == 0
    assert stats["pixel_tolerance"] == 5
    assert strict_stats["pixel_count"] == 4


@pytest.mark.unit
def test_threshold_result_treats_equal_thresholds_as_passes() -> None:
    run = {
        "status": RunStatus.finished.value,
        "summary": {"diff_average": 2.0},
    }
    executions = [
        {
            "id": "execution-1",
            "status": ExecutionStatus.finished.value,
            "diff_level": 4.0,
        }
    ]

    passed, failures = _threshold_result(
        run,
        executions,
        run_threshold=2.0,
        execution_threshold=4.0,
    )

    assert passed is True
    assert failures == []


@pytest.mark.unit
def test_threshold_result_fails_when_one_pixel_diff_exceeds_threshold() -> None:
    run = {
        "status": RunStatus.finished.value,
        "summary": {"diff_average": 11.1111},
    }
    executions = [
        {
            "id": "execution-1",
            "status": ExecutionStatus.finished.value,
            "diff": {"percentage": 11.1111},
        }
    ]

    passed, failures = _threshold_result(
        run,
        executions,
        run_threshold=11.0,
        execution_threshold=11.0,
    )

    assert passed is False
    assert failures == [
        "run_diff_average 11.1111 exceeds threshold 11.0000",
        "execution execution-1 diff 11.1111 exceeds threshold 11.0000",
    ]
