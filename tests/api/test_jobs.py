from dltctl.api.jobs import JobsApi
import pytest, mock

def test_get_job_id_by_name_single_name():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.return_value = {"jobs":[{"job_id": 1, "settings": {"name": "foo"}}]}
    id = j_api.get_job_id_by_name("foo")
    assert id == 1

def test_get_job_id_by_non_unique_name():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.return_value = {"jobs":[{"job_id": 1, "settings": {"name": "foo"}},{"job_id": 2, "settings": {"name": "foo"}}]}
    with pytest.raises(Exception):
        id = j_api.get_job_id_by_name("foo")

def test_run_now():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.return_value = {"run_id": 12345}
    run_id = j_api.run_now(1337)
    assert run_id == 12345

def test_ensure_run_quick_start_success():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.side_effect = [{"state": { "life_cycle_state": "RUNNING",  "state_message": "tis running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "still running"}}
    ]
    j_api.ensure_run_start(123)
    assert client_mock.perform_query.call_count == 2

def test_ensure_run_slow_start_success():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.side_effect = [{"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
        {"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "tis running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "still running"}}
    ]
    j_api.ensure_run_start(123)
    assert client_mock.perform_query.call_count == 4

def test_ensure_run_slow_start_failure():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.side_effect = [{"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
        {"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "tis running"}},
    {"state": { "life_cycle_state": "FAILED",  "state_message": "still running"}}
    ]
    with pytest.raises(Exception):
        j_api.ensure_run_start(123)

def test_ensure_run_slow_start_error():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.side_effect = [{"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
        {"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "tis running"}},
    {"state": { "life_cycle_state": "ERROR",  "state_message": "still running"}}
    ]
    with pytest.raises(Exception):
        j_api.ensure_run_start(123)

def test_ensure_run_slow_start_skipped():
    client_mock = mock.MagicMock()
    j_api = JobsApi(client_mock)
    client_mock.perform_query.side_effect = [{"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
        {"state": { "life_cycle_state": "PENDING",  "state_message": "tis not running"}},
    {"state": { "life_cycle_state": "RUNNING",  "state_message": "tis running"}},
    {"state": { "life_cycle_state": "SKIPPED",  "state_message": "still running"}}
    ]
    with pytest.raises(Exception):
        j_api.ensure_run_start(123)




        
