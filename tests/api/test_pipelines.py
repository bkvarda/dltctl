from unittest import mock
import pytest
from dltctl.api.pipelines import PipelinesApi, PipelineNameNotUniqueError

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
    




