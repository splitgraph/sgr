from splitgraph.ingestion.inference import _infer_column_schema


def test_inference():
    assert _infer_column_schema(["1", "2", "3"]) == "integer"
    assert _infer_column_schema(["1.1", "2.4", "3"]) == "numeric"
    assert _infer_column_schema(["true", "TRUE", "f"]) == "boolean"
    assert _infer_column_schema(['{"a": 42}']) == "json"
    assert _infer_column_schema(["1", "", "", "4"]) == "integer"
    assert (
        _infer_column_schema(["2020-01-01 12:34:56", "2020-01-02 00:00:00.123", ""]) == "timestamp"
    )
    assert _infer_column_schema([""]) == "character varying"
