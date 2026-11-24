import argparse
import fcntl
import json
import math
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

from retail_stream.config import ANALYTICS_PATH


@contextmanager
def analytics_lock(path):
    root = Path(path).resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".analytics.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Arrêtez retail-aggregate avant le nettoyage.") from error
        try:
            yield root
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_log(path):
    lines = path.read_text().splitlines()
    if not lines or lines[0] != "v1":
        raise ValueError(f"Format de journal non reconnu : {path}")
    return [json.loads(line) for line in lines[1:]]


def candidates(root, retention_hours, now=None):
    if not math.isfinite(retention_hours) or retention_hours < 0:
        raise ValueError("La rétention doit être un nombre positif ou nul.")
    checkpoint = root / "metrics-checkpoint"
    commits = [int(p.name) for p in (checkpoint / "commits").iterdir() if p.name.isdigit()]
    if not commits:
        return []
    batch = max(commits)
    read_log(checkpoint / "commits" / str(batch))
    offsets = read_log(checkpoint / "offsets" / str(batch))
    if len(offsets) != 2 or set(offsets[1]) != {"logOffset"}:
        raise ValueError("Le nettoyage exige une seule source de fichiers Spark.")
    boundary = offsets[1]["logOffset"]
    source = checkpoint / "sources" / "0"
    logs = {}
    for path in source.iterdir():
        name = path.name.removesuffix(".compact")
        if name.isdigit():
            number = int(name)
            if number not in logs or path.suffix == ".compact":
                logs[number] = path
    compactions = [n for n, p in logs.items() if p.suffix == ".compact"]
    start = max(compactions, default=0)
    if not logs or any(n not in logs for n in range(start, max(logs) + 1)):
        raise ValueError("Journal source incomplet : aucun fichier supprimé.")
    entries = {}
    for number in sorted(logs):
        if number >= start:
            for entry in read_log(logs[number]):
                entries[entry["path"]] = entry
    cutoff = (time.time() if now is None else now) - retention_hours * 3600
    staging = root / "orders"
    result = []
    for uri, entry in entries.items():
        if entry["batchId"] >= boundary or entry["timestamp"] / 1000 > cutoff:
            continue
        parsed = urlsplit(uri)
        path = Path(unquote(parsed.path))
        if parsed.scheme != "file" or parsed.netloc or path.parent != staging:
            raise ValueError(f"Chemin hors du dossier Parquet : {uri}")
        if path.is_symlink() or path.suffix != ".parquet":
            raise ValueError(f"Fichier non admissible : {path}")
        if path.is_file() and path.stat().st_mtime <= cutoff:
            result.append(path)
    return sorted(result)


def clean_parquet(path, retention_hours=24, apply=False):
    with analytics_lock(path) as root:
        selected = candidates(root, retention_hours)
        size = sum(p.stat().st_size for p in selected)
        if apply:
            for parquet in selected:
                parquet.unlink()
                parquet.with_name(f".{parquet.name}.crc").unlink(missing_ok=True)
        return len(selected), size


def main():
    parser = argparse.ArgumentParser(description="Nettoyer les Parquet déjà traités par Spark.")
    parser.add_argument("--retention-hours", type=float, default=24)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        count, size = clean_parquet(ANALYTICS_PATH, args.retention_hours, args.apply)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        parser.exit(1, f"Nettoyage refusé : {error}\n")
    action = "supprimés" if args.apply else "supprimables (simulation)"
    print(f"{count} fichiers {action}, {size} octets.")
