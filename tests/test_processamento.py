def test_window_aggregation():
    # Simula eventos e verifica se a agregação está correta
    assert aggregate_events([{'endpoint': 'login', 'count': 1},
                             {'endpoint': 'login', 'count': 2}]) == 3
