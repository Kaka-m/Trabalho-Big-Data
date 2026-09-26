def test_window_aggregation():
    events = [
        {'endpoint': 'login', 'count': 1},
        {'endpoint': 'login', 'count': 2}
    ]
    result = aggregate_events(events)
    assert result == {'endpoint': 'login', 'count': 3}
