# tests/test_csv_detect.py
# Testuje sniffer separatora CSV (utils.csv.sniff_delimiter).
from pathlib import Path
from energia_prep2.utils.csv import sniff_delimiter

def test_sniff_delimiter_semicolon(tmp_path: Path):
    p = tmp_path / "sample.csv"
    p.write_text("A;B;C\n1;2;3\n", encoding="utf-8")
    delim = sniff_delimiter(p)
    assert delim == ";"

def test_sniff_delimiter_comma(tmp_path: Path):
    p = tmp_path / "sample.csv"
    p.write_text("A,B,C\n1,2,3\n", encoding="utf-8")
    delim = sniff_delimiter(p)
    assert delim == ","
