from apps.collector.jobs.kalshi_sync import _map_market_status


def test_map_market_status_open_is_active() -> None:
    assert _map_market_status("open") == "active"


def test_map_market_status_active_aliases() -> None:
    assert _map_market_status("active") == "active"
    assert _map_market_status("trading") == "active"


def test_map_market_status_resolved_aliases() -> None:
    assert _map_market_status("resolved") == "resolved"
    assert _map_market_status("settled") == "resolved"


def test_map_market_status_closed_aliases() -> None:
    assert _map_market_status("closed") == "closed"
    assert _map_market_status("inactive") == "closed"
    assert _map_market_status("expired") == "closed"


def test_map_market_status_unknown_defaults_closed() -> None:
    assert _map_market_status("unknown") == "closed"
    assert _map_market_status(None) == "closed"
