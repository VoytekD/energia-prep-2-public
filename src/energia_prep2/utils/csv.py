from __future__ import annotations
import csv
from pathlib import Path

def sniff_delimiter(path: Path, sample_size: int = 4096) -> str | None:
    """
    Lekka detekcja separatora CSV (użyteczna, gdy pliki zewnętrzne nie mają gwarantowanego formatu).
    Zwraca: ';' | ',' | '\t' | None
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        sample = f.read(sample_size)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=";, \t")
        return dialect.delimiter
    except Exception:
        return None
