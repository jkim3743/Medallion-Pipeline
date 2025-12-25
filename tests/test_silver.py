from medallion_pipeline.silver import clean_str, parse_timestamp, add_silver_timestamps, validate_state_priority


def test_clean_str():
    assert clean_str("  hello ") == "hello"
    assert clean_str("NULL") is None
    assert clean_str("") is None
    assert clean_str(None) is None


def test_parse_timestamp_servicenow_format():
    assert parse_timestamp("6/5/2025 13:10") is not None
    assert parse_timestamp("2/15/2025 3:21") is not None
    assert parse_timestamp("null") is None
    assert parse_timestamp("bad-format") is None
    assert parse_timestamp(None) is None


def test_add_silver_timestamps():
    row = {"opened_at": "6/5/2025 13:10", "closed_at": "6/9/2025 11:45"}
    out = add_silver_timestamps(row)
    assert out["opened_at_ts"] is not None
    assert out["closed_at_ts"] is not None

    row2 = {"opened_at": "2/15/2025 3:21", "closed_at": "null"}
    out2 = add_silver_timestamps(row2)
    assert out2["opened_at_ts"] is not None
    assert out2["closed_at_ts"] is None


def test_validate_state_priority():
    assert validate_state_priority({"state": "New", "priority": 1}) is True
    assert validate_state_priority({"state": None, "priority": 1}) is False
    assert validate_state_priority({"state": "New", "priority": None}) is False
