from dltctl.types.events import PipelineEvent, PipelineEventsResponse

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
_EVENTS = [_EVENT_JSON]

_EVENTS_RESPONSE = {"next_page_token": 12345,
"prev_page_token": 12346, "events_json": _EVENTS}

def test_pipeline_events_reponse():
    event_response = PipelineEventsResponse().from_json_response(_EVENTS_RESPONSE)
    assert event_response.events == _EVENTS

def test_pipeline_events_response_to_events():
    event_response = PipelineEventsResponse().from_json_response(_EVENTS_RESPONSE)
    events = event_response.to_pipeline_events()
    assert events[0].message == "Update 175b9d started by USER_ACTION."

def test_pipeline_event():
    event = PipelineEvent().from_json(_EVENTS[0])
    assert event.message == "Update 175b9d started by USER_ACTION."
