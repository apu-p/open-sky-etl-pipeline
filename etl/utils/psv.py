"""Pipe-delimited (RFC-4180-quoted) flat-file writer, shared by the transform
layer (domain rows) and the load layer (metrics/audit rows). Lives in utils so
neither layer has to import the other (Section 4: no cross-layer imports).

csv.writer with delimiter='|' quotes any field containing the delimiter, a
quote, or a newline, and writes Python None as an unquoted empty field, which
COPY reads back as SQL NULL. No header row — the load reads data only."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


def write_psv(path: str | Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> str:
    path = Path(path)
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="|")
        for row in rows:
            writer.writerow([row.get(col) for col in columns])
    return str(path)
