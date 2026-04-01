from unittest.mock import Mock

import pytest
from rx import create

from src.pipeline.stream import (
    attributes_mapping,
    build_pipeline,
    machines_mapping,
    properties_mapping,
)


def test_build_pipeline_processes_single_event():
    test_event = {
        "TS": "2025-02-22T10:00:00",
        "PR": 1,
        "MC": "FB713A",
        "PS": {"T3": 30},
    }
    expected_event = {
        "TIMESTAMP": "2025-02-22T10:00:00",
        "PRODUCT": 1,
        "MACHINE": "FILLING",
        "PROPS": {"TIME": 30},
    }
    received_events = []

    def source_observable(observer, _):
        observer.on_next(test_event)
        observer.on_completed()

    source = create(source_observable)
    send_rich_event_mock = Mock()
    save_raw_event_mock = Mock()
    save_rich_event_mock = Mock()

    pipeline = build_pipeline(
        source,
        send_rich_event_mock,
        save_raw_event_mock,
        save_rich_event_mock,
    )
    pipeline.subscribe(
        on_next=received_events.append,
        on_error=lambda error: pytest.fail(f"Pipeline error: {error}"),
    )

    assert received_events == [expected_event]
    save_raw_event_mock.assert_called_once_with(test_event)
    send_rich_event_mock.assert_called_once_with(expected_event)
    save_rich_event_mock.assert_called_once_with(expected_event)


def test_build_pipeline_filters_unknown_machines():
    invalid_event = {
        "TS": "2025-02-22T10:00:00",
        "PR": 1,
        "MC": "UNKNOWN",
        "PS": {"T3": 30},
    }
    received_events = []

    def source_observable(observer, _):
        observer.on_next(invalid_event)
        observer.on_completed()

    source = create(source_observable)
    send_rich_event_mock = Mock()
    save_raw_event_mock = Mock()
    save_rich_event_mock = Mock()

    pipeline = build_pipeline(
        source,
        send_rich_event_mock,
        save_raw_event_mock,
        save_rich_event_mock,
    )
    pipeline.subscribe(
        on_next=received_events.append,
        on_error=lambda error: pytest.fail(f"Pipeline error: {error}"),
    )

    assert received_events == []
    save_raw_event_mock.assert_called_once_with(invalid_event)
    send_rich_event_mock.assert_not_called()
    save_rich_event_mock.assert_not_called()


def test_machines_mapping():
    assert machines_mapping("UNS56A") == "UNSCRAMBLER"
    assert machines_mapping("WS964F") == "WASHER"
    assert machines_mapping("CPM784") == "CAPPING"


def test_properties_mapping():
    assert properties_mapping("A7") == "LITERS"
    assert properties_mapping("T3") == "TIME"
    assert properties_mapping("P6") == "POWER"


def test_attributes_mapping():
    assert attributes_mapping("TS") == "TIMESTAMP"
    assert attributes_mapping("MC") == "MACHINE"
    assert attributes_mapping("PR") == "PRODUCT"
    assert attributes_mapping("PS") == "PROPS"
