from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

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
