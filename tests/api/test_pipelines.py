from unittest import mock
import pytest, json
from dltctl.api.pipelines import PipelinesApi, PipelineNameNotUniqueError
from dltctl.types.pipelines import PipelineSettings

_EVENT_DICT = {
    "id": "15e29b10-48bb-11ed-b1cf-9649472ee14a",
    "sequence": {
        "control_plane_seq_no": 1234567789
    },
    "origin": {
        "cloud": "AWS",
        "region": "us-west-2",
        "org_id": 1234567789,
        "pipeline_id": "some-id",
        "pipeline_name": "foobk1234",
        "update_id": "update-id",
        "request_id": "req-id"
    },
    "timestamp": "2022-10-10T16:46:28.161Z",
    "message": "Update 175b9d started by USER_ACTION.",
    "level": "INFO",
    "details": {
        "create_update": {
            "cause": "USER_ACTION",
            "config": {
                "id": "some-id",
                "name": "foobk1234",
                "storage": "dbfs:/pipelines/some-id",
                "configuration": {
                    "destination_table": "b",
                    "pipelines.enzyme.enabled": "true"
                },
                "clusters": [
                    {
                        "label": "default",
                        "node_type_id": "c5.4xlarge",
                        "driver_node_type_id": "c5.4xlarge",
                        "spark_env_vars": {
                            "DD_API_KEY": "{{secrets/foo_dlt/dd_api_key}}",
                            "DD_ENV": "dlt_test_pipeline",
                            "DD_SITE": "https://app.datadoghq.com"
                        },
                        "init_scripts": [
                            {
                                "dbfs": {
                                    "destination": "dbfs:/foo/init_scripts/datadog-install-driver-workers.sh"
                                }
                            }
                        ],
                        "autoscale": {
                            "min_workers": 1,
                            "max_workers": 4,
                            "mode": "ENHANCED"
                        }
                    }
                ],
                "libraries": [
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/dlt_listener.py"
                        }
                    },
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/multiplex_cdc_bronze.py"
                        }
                    }
                ],
                "target": "default",
                "filters": {},
                "email_notifications": {},
                "continuous": "true",
                "development": "true",
                "photon": "false",
                "edition": "ADVANCED",
                "channel": "CURRENT"
            },
            "failed_attempts": 0,
            "full_refresh": "false",
            "run_as_user_id": 123445678
        }
    },
    "event_type": "create_update",
    "maturity_level": "STABLE"
}
_EVENT_JSON = """{
    "id": "15e29b10-48bb-11ed-b1cf-9649472ee14a",
    "sequence": {
        "control_plane_seq_no": 1234567789
    },
    "origin": {
        "cloud": "AWS",
        "region": "us-west-2",
        "org_id": 1234567789,
        "pipeline_id": "some-id",
        "pipeline_name": "foobk1234",
        "update_id": "update-id",
        "request_id": "req-id"
    },
    "timestamp": "2022-10-10T16:46:28.161Z",
    "message": "Update 175b9d started by USER_ACTION.",
    "level": "INFO",
    "details": {
        "create_update": {
            "cause": "USER_ACTION",
            "config": {
                "id": "some-id",
                "name": "foobk1234",
                "storage": "dbfs:/pipelines/some-id",
                "configuration": {
                    "destination_table": "b",
                    "pipelines.enzyme.enabled": "true"
                },
                "clusters": [
                    {
                        "label": "default",
                        "node_type_id": "c5.4xlarge",
                        "driver_node_type_id": "c5.4xlarge",
                        "spark_env_vars": {
                            "DD_API_KEY": "{{secrets/foo_dlt/dd_api_key}}",
                            "DD_ENV": "dlt_test_pipeline",
                            "DD_SITE": "https://app.datadoghq.com"
                        },
                        "init_scripts": [
                            {
                                "dbfs": {
                                    "destination": "dbfs:/foo/init_scripts/datadog-install-driver-workers.sh"
                                }
                            }
                        ],
                        "autoscale": {
                            "min_workers": 1,
                            "max_workers": 4,
                            "mode": "ENHANCED"
                        }
                    }
                ],
                "libraries": [
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/dlt_listener.py"
                        }
                    },
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/multiplex_cdc_bronze.py"
                        }
                    }
                ],
                "target": "default",
                "filters": {},
                "email_notifications": {},
                "continuous": "true",
                "development": "true",
                "photon": "false",
                "edition": "ADVANCED",
                "channel": "CURRENT"
            },
            "failed_attempts": 0,
            "full_refresh": "false",
            "run_as_user_id": 123445678
        }
    },
    "event_type": "create_update",
    "maturity_level": "STABLE"
}"""

_WAITING_EVENT_JSON = """{
    "id": "15e29b10-48bb-11ed-b1cf-9649472ee14a",
    "sequence": {
        "control_plane_seq_no": 1234567789
    },
    "origin": {
        "cloud": "AWS",
        "region": "us-west-2",
        "org_id": 1234567789,
        "pipeline_id": "some-id",
        "pipeline_name": "foobk1234",
        "update_id": "update-id",
        "request_id": "req-id"
    },
    "timestamp": "2022-10-10T16:46:28.161Z",
    "message": "WAITING_FOR_RESOURCES",
    "level": "INFO",
    "details": {
        "create_update": {
            "cause": "USER_ACTION",
            "config": {
                "id": "some-id",
                "name": "foobk1234",
                "storage": "dbfs:/pipelines/some-id",
                "configuration": {
                    "destination_table": "b",
                    "pipelines.enzyme.enabled": "true"
                },
                "clusters": [
                    {
                        "label": "default",
                        "node_type_id": "c5.4xlarge",
                        "driver_node_type_id": "c5.4xlarge",
                        "spark_env_vars": {
                            "DD_API_KEY": "{{secrets/foo_dlt/dd_api_key}}",
                            "DD_ENV": "dlt_test_pipeline",
                            "DD_SITE": "https://app.datadoghq.com"
                        },
                        "init_scripts": [
                            {
                                "dbfs": {
                                    "destination": "dbfs:/foo/init_scripts/datadog-install-driver-workers.sh"
                                }
                            }
                        ],
                        "autoscale": {
                            "min_workers": 1,
                            "max_workers": 4,
                            "mode": "ENHANCED"
                        }
                    }
                ],
                "libraries": [
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/dlt_listener.py"
                        }
                    },
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/multiplex_cdc_bronze.py"
                        }
                    }
                ],
                "target": "default",
                "filters": {},
                "email_notifications": {},
                "continuous": "true",
                "development": "true",
                "photon": "false",
                "edition": "ADVANCED",
                "channel": "CURRENT"
            },
            "failed_attempts": 0,
            "full_refresh": "false",
            "run_as_user_id": 123445678
        }
    },
    "event_type": "create_update",
    "maturity_level": "STABLE"
}"""
_FAILURE_EVENT_JSON = """{
    "id": "15e29b10-48bb-11ed-b1cf-9649472ee14a",
    "error": {"exceptions": "some failure exception"},
    "sequence": {
        "control_plane_seq_no": 1234567789
    },
    "origin": {
        "cloud": "AWS",
        "region": "us-west-2",
        "org_id": 1234567789,
        "pipeline_id": "some-id",
        "pipeline_name": "foobk1234",
        "update_id": "update-id",
        "request_id": "req-id"
    },
    "timestamp": "2022-10-10T16:46:28.161Z",
    "message": "WAITING_FOR_RESOURCES",
    "level": "ERROR",
    "details": {
        "create_update": {
            "cause": "USER_ACTION",
            "config": {
                "id": "some-id",
                "name": "foobk1234",
                "storage": "dbfs:/pipelines/some-id",
                "configuration": {
                    "destination_table": "b",
                    "pipelines.enzyme.enabled": "true"
                },
                "clusters": [
                    {
                        "label": "default",
                        "node_type_id": "c5.4xlarge",
                        "driver_node_type_id": "c5.4xlarge",
                        "spark_env_vars": {
                            "DD_API_KEY": "{{secrets/foo_dlt/dd_api_key}}",
                            "DD_ENV": "dlt_test_pipeline",
                            "DD_SITE": "https://app.datadoghq.com"
                        },
                        "init_scripts": [
                            {
                                "dbfs": {
                                    "destination": "dbfs:/foo/init_scripts/datadog-install-driver-workers.sh"
                                }
                            }
                        ],
                        "autoscale": {
                            "min_workers": 1,
                            "max_workers": 4,
                            "mode": "ENHANCED"
                        }
                    }
                ],
                "libraries": [
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/dlt_listener.py"
                        }
                    },
                    {
                        "notebook": {
                            "path": "/Users/foo@foo.com/dltctl_artifactsz/test_nested/multiplex_cdc_bronze.py"
                        }
                    }
                ],
                "target": "default",
                "filters": {},
                "email_notifications": {},
                "continuous": "true",
                "development": "true",
                "photon": "false",
                "edition": "ADVANCED",
                "channel": "CURRENT"
            },
            "failed_attempts": 0,
            "full_refresh": "false",
            "run_as_user_id": 123445678
        }
    },
    "event_type": "create_update",
    "maturity_level": "STABLE"
}"""
_EVENTS = [_EVENT_JSON]
_WAITING_EVENTS = [_WAITING_EVENT_JSON]
_FAILURE_EVENTS = [_FAILURE_EVENT_JSON]

_EVENTS_RESPONSE = {"next_page_token": 12345,
"prev_page_token": 12346, "events_json": _EVENTS}

_WAITING_EVENTS_RESPONSE = {"next_page_token": 12345,
"prev_page_token": 123456, "events_json": _WAITING_EVENTS}

_FAILURE_EVENTS_RESPONSE = {"next_page_token": 12345,
"prev_page_token": 123456, "events_json": _FAILURE_EVENTS}

@pytest.fixture()
def pipelines_api_get_mock():
    client_mock = mock.MagicMock()

    def server_response(*args, **kwargs):
        if args[0] == 'GET':
            libs = [{"notebook": {"path": "/Users/foo@foo.com/test.py"}}]
            updates = [{"update_id": 1, "state": "COMPLETED", "creation_time": "2021-08-13T00:37:30.279Z"}]
            return {
                "pipeline_id": "1337",
                "state": "IDLE",
                "latest_updates": updates,
                "spec": {
                    "id": "1337",
                    "name": "mycoolpipeline",
                    "libraries": libs
                }
            }

    baz = []
    foo = {
        "foo": "bar",
        "baz": baz,
    }

    client_mock.perform_query = mock.MagicMock(side_effect=server_response)
    _pipelines_api = PipelinesApi(client_mock)
    yield _pipelines_api

@pytest.fixture()
def pipelines_api_get_status_mock():
    client_mock = mock.MagicMock()

    def server_response(*args, **kwargs):
        if args[0] == 'GET':
            libs = [{"notebook": {"path": "/Users/foo@foo.com/test.py"}}]
            updates = [{"update_id": 1, "state": "COMPLETED", "creation_time": "2021-08-13T00:37:30.279Z"}]
            return {
                "pipeline_id": "1337",
                "state": "RUNNING",
                "latest_updates": updates,
                "spec": {
                    "id": "1337",
                    "name": "mycoolpipeline",
                    "libraries": libs
                }
            }

    baz = []
    foo = {
        "foo": "bar",
        "baz": baz,
    }

    def sec_server_response(*args, **kwargs):
        if args[0] == 'GET':
            libs = [{"notebook": {"path": "/Users/foo@foo.com/test.py"}}]
            updates = [{"update_id": 1, "state": "COMPLETED", "creation_time": "2021-08-13T00:37:30.279Z"}]
            return {
                "pipeline_id": "1337",
                "state": "IDLE",
                "latest_updates": updates,
                "spec": {
                    "id": "1337",
                    "name": "mycoolpipeline",
                    "libraries": libs
                }
            }

    baz = []
    foo = {
        "foo": "bar",
        "baz": baz,
    }

    client_mock.perform_query = mock.MagicMock(side_effect=[server_response, sec_server_response])
    _pipelines_api = PipelinesApi(client_mock)
    yield _pipelines_api

@pytest.fixture()
def pipelines_api_list_mock():
    client_mock = mock.MagicMock()

    def server_response(*args, **kwargs):
        if args[0] == 'GET':
            return {"statuses":[{"pipeline_id": 1, "name": "foo"}, {"pipeline_id": 2, "name": "bar"}]}

    client_mock.perform_query = mock.MagicMock(side_effect=server_response)
    _pipelines_api = PipelinesApi(client_mock)
    yield _pipelines_api

@pytest.fixture()
def pipelines_api_list_dups_mock():
    client_mock = mock.MagicMock()

    def server_response(*args, **kwargs):
        if args[0] == 'GET':
            return {"statuses":[{"pipeline_id": 1, "name": "foo"}, {"pipeline_id": 2, "name": "foo"}]}

    client_mock.perform_query = mock.MagicMock(side_effect=server_response)
    _pipelines_api = PipelinesApi(client_mock)
    yield _pipelines_api

def test_get_pipeline_by_name(pipelines_api_list_mock):
    returned_pipeline = pipelines_api_list_mock.get_pipeline_id_by_name("foo")
    assert returned_pipeline == 1

def test_get_pipeline_by_name_no_exist(pipelines_api_list_mock):
    returned_pipeline = pipelines_api_list_mock.get_pipeline_id_by_name("baz")
    assert returned_pipeline == None

def test_get_pipeline_by_name_duplicates(pipelines_api_list_dups_mock):
    with pytest.raises(PipelineNameNotUniqueError):
        pipelines_api_list_dups_mock.get_pipeline_id_by_name("foo")

def test_get_pipeline_settings(pipelines_api_get_mock):
    settings = pipelines_api_get_mock.get_pipeline_settings("1337")
    assert settings["name"] == "mycoolpipeline"

def test_get_pipeline_state(pipelines_api_get_mock):
    state = pipelines_api_get_mock.get_pipeline_state("1337")
    assert state == "IDLE"

def test_get_pipeline_update_id(pipelines_api_get_mock):
    update_id = pipelines_api_get_mock.get_last_update_id("1337")
    assert update_id == 1

def test_get_pipeline_libraries(pipelines_api_get_mock):
    libraries = pipelines_api_get_mock.get_pipeline_libraries("1337")
    assert libraries == ["/Users/foo@foo.com/test.py"]

def test_stop_pipeline_async(pipelines_api_get_mock):
    client_mock = pipelines_api_get_mock.client.client.perform_query
    pipelines_api_get_mock.stop_async("12345")
    client_mock.assert_called_with("POST", "/pipelines/12345/stop", data={}, headers=None)
    assert client_mock.call_count == 1

def test_stop_pipeline_sync():
    with mock.patch('databricks_cli.sdk.ApiClient') as client_mock:
        api = PipelinesApi(client_mock)
        side_effect = [{"state": "IDLE"}]
        client_mock.perform_query.return_value = {"state": "IDLE"}
        api.stop("1337")
        client_mock.perform_query.assert_any_call("GET", "/pipelines/1337", data={}, headers=None)
        client_mock.perform_query.assert_any_call("POST", "/pipelines/1337/stop", data={}, headers=None)
        assert client_mock.perform_query.call_count == 2
    


def test_edit_pipeline(pipelines_api_get_mock):
    client_mock = pipelines_api_get_mock.client.client.perform_query
    settings = PipelineSettings()
    settings.id = "12345"
    pipelines_api_get_mock.edit("12345", settings)
    client_mock.assert_called_with("PUT", "/pipelines/12345", data=settings.to_json(), headers=None)
    assert client_mock.call_count == 1

def test_create_pipeline(pipelines_api_get_mock):
    client_mock = pipelines_api_get_mock.client.client.perform_query
    p = PipelineSettings()
    settings = p.to_json()
    pipelines_api_get_mock.create(settings)
    client_mock.assert_called_with("POST", "/pipelines", data=settings, headers=None)
    assert client_mock.call_count == 1

def test_get_events():
    client_mock = mock.MagicMock()
    client_mock.perform_query.side_effect = [_EVENTS_RESPONSE]
    data = {'max_results': 100, 'order_by': 'timestamp asc'}
    p = PipelinesApi(client_mock)
    e = p.get_events(12345)
    client_mock.perform_query.assert_called_with("GET", "/pipelines/12345/events", data=data)
    assert e == _EVENTS_RESPONSE

def test_stream_events_single_event(capsys):
    client_mock = mock.MagicMock()
    client_mock.perform_query.side_effect = [_EVENTS_RESPONSE, [], [], []]
    p = PipelinesApi(client_mock)
    e = p.stream_events(12345, max_polls_without_events=2)
    out, err = capsys.readouterr()
    assert "create_update Update 175b9d started by USER_ACTION." in out

def test_stream_events_resource_waiting(capsys):
    client_mock = mock.MagicMock()
    client_mock.perform_query.side_effect = [_EVENTS_RESPONSE, [], _WAITING_EVENTS_RESPONSE, _EVENTS_RESPONSE, []]
    p = PipelinesApi(client_mock)
    e = p.stream_events(12345, max_polls_without_events=1)
    out, err = capsys.readouterr()
    assert "create_update Update 175b9d started by USER_ACTION." in out

def test_stream_events_resource_waiting(capsys):
    client_mock = mock.MagicMock()
    client_mock.perform_query.side_effect = [_EVENTS_RESPONSE, _FAILURE_EVENTS_RESPONSE]
    p = PipelinesApi(client_mock)
    e = p.stream_events(12345, max_polls_without_events=1)
    out, err = capsys.readouterr()
    assert "Pipeline execution has FAILED." in out



