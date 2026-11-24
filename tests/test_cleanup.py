import json
import time

import pytest

from retail_stream.cleanup import analytics_lock, clean_parquet


def log(path, *rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("v1\n" + "\n".join(json.dumps(row) for row in rows))


@pytest.fixture
def archive(tmp_path):
    root = tmp_path / "analytics"
    staging = root / "orders"
    staging.mkdir(parents=True)
    entries = []
    for i in range(3):
        path = staging / f"part-{i}.parquet"
        path.write_bytes(b"parquet")
        entries.append({"path": path.as_uri(), "timestamp": int(time.time() * 1000), "batchId": i})
    checkpoint = root / "metrics-checkpoint"
    log(checkpoint / "commits" / "1", {})
    log(checkpoint / "offsets" / "1", {}, {"logOffset": 1})
    for i, entry in enumerate(entries):
        log(checkpoint / "sources" / "0" / str(i), entry)
    return root


def test_preview_retention_and_committed_boundary(archive):
    assert clean_parquet(archive, 24) == (0, 0)
    assert clean_parquet(archive, 0) == (1, 7)
    assert len(list((archive / "orders").glob("*.parquet"))) == 3
    assert clean_parquet(archive, 0, apply=True) == (1, 7)
    assert not (archive / "orders" / "part-0.parquet").exists()
    assert (archive / "orders" / "part-1.parquet").exists()
    assert (archive / "orders" / "part-2.parquet").exists()
    assert clean_parquet(archive, 0, apply=True) == (0, 0)


def test_lock_prevents_cleanup(archive):
    with analytics_lock(archive), pytest.raises(RuntimeError, match="Arrêtez"):
        clean_parquet(archive, 0, apply=True)


def test_corrupt_log_does_not_delete(archive):
    (archive / "metrics-checkpoint" / "sources" / "0" / "2").write_text("v9\n")
    with pytest.raises(ValueError):
        clean_parquet(archive, 0, apply=True)
    assert (archive / "orders" / "part-0.parquet").exists()


def test_compacted_log(archive):
    source = archive / "metrics-checkpoint" / "sources" / "0"
    rows = [json.loads((source / str(i)).read_text().splitlines()[1]) for i in range(3)]
    for i in range(3):
        (source / str(i)).unlink()
    log(source / "2.compact", *rows)
    assert clean_parquet(archive, 0, apply=True) == (1, 7)


def test_external_path_refused(archive, tmp_path):
    external = tmp_path / "outside.parquet"
    external.write_bytes(b"keep")
    log(
        archive / "metrics-checkpoint" / "sources" / "0" / "0",
        {"path": external.as_uri(), "timestamp": 0, "batchId": 0},
    )
    with pytest.raises(ValueError, match="hors"):
        clean_parquet(archive, 0, apply=True)
    assert external.exists()
