from __future__ import annotations
from pathlib import Path

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def file_exists(p: Path) -> bool:
    return p.exists() and p.is_file()
