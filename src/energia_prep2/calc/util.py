"""
calc.util — wspólne narzędzia
"""

from __future__ import annotations
import math
import time
import logging
from contextlib import contextmanager
from typing import Iterable, Iterator, Any

log = logging.getLogger("energia-prep-2.calc")

@contextmanager
def timer(msg: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        log.info(f"{msg} — {dt:.3f}s")

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def chunks(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    buf: list[Any] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

EPS = 1e-9

def feq(a: float, b: float, eps: float = EPS) -> bool:
    return abs(a - b) <= eps * max(1.0, abs(a), abs(b))
