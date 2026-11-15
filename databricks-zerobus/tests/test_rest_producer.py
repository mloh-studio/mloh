from zerobus_demo.rest_producer import authorization_details, workspace_id_from_endpoint


def test_workspace_id_parsed_from_endpoint():
    assert workspace_id_from_endpoint(
        "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    ) == "1234567890123456"
    assert workspace_id_from_endpoint("987.zerobus.eastus.azuredatabricks.net") == "987"


def test_token_is_scoped_to_single_table():
    details = authorization_details("main.zerobus_demo.sensor_events_raw")
    paths = [d["object_full_path"] for d in details]
    assert paths == ["main", "main.zerobus_demo", "main.zerobus_demo.sensor_events_raw"]
    table = details[-1]
    assert table["privileges"] == ["SELECT", "MODIFY"]
    assert table["operations"] == ["zerobuswrite"]
