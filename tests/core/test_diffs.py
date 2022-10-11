from dltctl.core.helpers import *
from dltctl.types.pipelines import JobConfig, PipelineSettings
from dltctl.api.jobs import JobsApi
from dltctl.api.pipelines import PipelinesApi
from unittest import mock

def test_is_pipeline_conf_diff():
    mock_client = mock.MagicMock()
    orig_pipeline_settings = PipelineSettings("foo")
    new_pipeline_settings = PipelineSettings("foo",development=False)
    orig_pipeline_settings.id = 1234
    new_pipeline_settings.id = 1234
    orig_pipeline_return_val = {"spec": orig_pipeline_settings.to_json()}
    mock_client.perform_query.return_value = orig_pipeline_return_val

    assert is_pipeline_settings_diff(mock_client, new_pipeline_settings)
    mock_client.perform_query.assert_called_once_with("GET", "/pipelines/1234", data={}, headers=None)

    
def test_is_pipeline_conf_diff_no_diff():
    mock_client = mock.MagicMock()
    orig_pipeline_settings = PipelineSettings("foo")
    new_pipeline_settings = PipelineSettings("foo")
    orig_pipeline_settings.id = 1234
    new_pipeline_settings.id = 1234
    orig_pipeline_return_val = {"spec": orig_pipeline_settings.to_json()}
    mock_client.perform_query.return_value = orig_pipeline_return_val 
    assert not is_pipeline_settings_diff(mock_client, new_pipeline_settings)
    mock_client.perform_query.assert_called_once_with("GET", "/pipelines/1234", data={}, headers=None)

def test_is_job_conf_diff():
    mock_client = mock.MagicMock()
    orig_job_conf = JobConfig(name="foo")
    new_job_conf = JobConfig(name="foo", tags={"foo": "bar"})
    orig_job_return_val = {"settings": orig_job_conf.to_dict()}
    mock_client.perform_query.return_value = orig_job_return_val

    assert is_job_conf_diff(mock_client, new_job_conf, 12345)
    mock_client.perform_query.assert_called_with("GET", "/jobs/get", data={'job_id': 12345}, headers=None, version=None)

def test_is_job_conf_diff_no_diff():
    mock_client = mock.MagicMock()
    orig_job_conf = JobConfig(name="foo")
    new_job_conf = JobConfig(name="foo")
    orig_job_return_val = {"settings": orig_job_conf.to_dict()}
    mock_client.perform_query.return_value = orig_job_return_val

    assert not is_job_conf_diff(mock_client, new_job_conf, 12345)
    mock_client.perform_query.assert_called_with("GET", "/jobs/get", data={'job_id': 12345}, headers=None, version=None)