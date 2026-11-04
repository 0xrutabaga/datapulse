from __future__ import annotations

from pathlib import Path

from datapulse.ingest import ROW_FIELD, check_columns, parse_csv, parse_jsonl, read_file

HEADER = b"order_id,customer_id,status,quantity,amount,currency,event_time,discount_code"
ROW = b"ORD-100001,CUS-12345,paid,2,19.98,EUR,2026-01-02T10:00:00Z,"


def write(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_csv_line_numbers_are_physical_and_blank_lines_skipped() -> None:
    parsed = parse_csv(HEADER + b"\n" + ROW + b"\n\n" + ROW + b"\n")
    assert [r.line for r in parsed.records] == [2, 4]


def test_crlf_and_bom_parse_like_lf() -> None:
    lf = parse_csv(HEADER + b"\n" + ROW + b"\n")
    crlf = parse_csv(b"\xef\xbb\xbf" + HEADER + b"\r\n" + ROW + b"\r\n")
    assert lf.columns == crlf.columns
    assert [r.values for r in lf.records] == [r.values for r in crlf.records]


def test_csv_row_problems_are_row_level_errors() -> None:
    data = HEADER + b"\n" + b"ORD-100001,CUS-1\n" + b"ORD-100001,CUS-\xff,paid,2,1,EUR,x,\n"
    records = parse_csv(data).records
    assert records[0].error == "expected 8 fields, got 2"
    assert records[1].error is not None and records[1].error.startswith("invalid UTF-8")
    assert r"CUS-\xff" in records[1].raw  # raw keeps the evidence, escaped


def test_csv_empty_cell_is_absent_value() -> None:
    record = parse_csv(HEADER + b"\n" + ROW + b"\n").records[0]
    assert record.values is not None
    assert "discount_code" not in record.values


def test_jsonl_errors_and_key_union() -> None:
    data = b'{"order_id": "ORD-1", "amount": 19.99}\n{"order_id": \n[1, 2]\n{"extra": 1}\n'
    parsed = parse_jsonl(data)
    assert [r.line for r in parsed.records] == [1, 2, 3, 4]
    assert parsed.records[1].error is not None and parsed.records[1].error.startswith(
        "invalid JSON"
    )
    assert parsed.records[2].error == "expected a JSON object"
    assert parsed.columns == ("order_id", "amount", "extra")
    assert str(parsed.records[0].values["amount"]) == "19.99"  # type: ignore[index]


def test_check_columns() -> None:
    assert check_columns(("order_id", "x")) == (
        ("amount", "currency", "customer_id", "event_time", "quantity", "status"),
        ("x",),
    )


def test_schema_drifted_csv_is_rejected_as_a_whole(tmp_path: Path) -> None:
    drifted = HEADER.replace(b"amount", b"total_amount") + b",channel\n" + ROW + b",web\n"
    result = read_file(write(tmp_path, "f.csv", drifted), "csv", file_seq=1)
    assert result.reject_reason == (
        "schema drift: missing required columns amount; unexpected columns channel, total_amount"
    )
    assert result.valid == [] and result.quarantine == []
    assert result.rows_read == 1


def test_extra_column_is_recorded_but_file_loads(tmp_path: Path) -> None:
    data = HEADER + b",channel\n" + ROW + b",web\n"
    result = read_file(write(tmp_path, "f.csv", data), "csv", file_seq=1)
    assert result.reject_reason is None
    assert result.extra_columns == ("channel",)
    assert len(result.valid) == 1


def test_jsonl_row_missing_a_key_is_quarantined_not_rejected(tmp_path: Path) -> None:
    good = (
        b'{"order_id": "ORD-100001", "customer_id": "CUS-12345", "status": "paid", "quantity": 1,'
        b' "amount": 2.50, "currency": "USD", "event_time": "2026-01-01T00:00:00Z"}'
    )
    missing = good.replace(b'"currency": "USD", ', b"")
    result = read_file(write(tmp_path, "f.jsonl", good + b"\n" + missing + b"\n"), "jsonl", 1)
    assert result.reject_reason is None
    assert len(result.valid) == 1
    assert [(q.line, q.field, q.reason) for q in result.quarantine] == [
        (2, "currency", "Field required")
    ]


def test_all_corrupt_jsonl_is_quarantined_not_treated_as_drift(tmp_path: Path) -> None:
    result = read_file(write(tmp_path, "f.jsonl", b"{bad\nnope\n"), "jsonl", 1)
    assert result.reject_reason is None
    assert [q.field for q in result.quarantine] == [ROW_FIELD, ROW_FIELD]
    assert result.rows_quarantined == 2


def test_empty_and_header_only_files(tmp_path: Path) -> None:
    assert read_file(write(tmp_path, "e.csv", b""), "csv", 1).rows_read == 0
    header_only = read_file(write(tmp_path, "h.csv", HEADER + b"\n"), "csv", 1)
    assert (header_only.rows_read, header_only.reject_reason) == (0, None)


def test_duplicate_header_rejected(tmp_path: Path) -> None:
    result = read_file(write(tmp_path, "d.csv", HEADER + b",status\n"), "csv", 1)
    assert result.reject_reason == "duplicate column names in header"
