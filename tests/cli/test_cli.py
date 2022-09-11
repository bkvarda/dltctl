from json import JSONDecodeError
from unittest import mock
import pytest
from click import Context, Command
from click.testing import CliRunner
from pathlib import Path
from tests.utils import provide_conf
import dltctl.cli as cli
from dltctl.types.pipelines import PipelineSettings


@pytest.fixture()
def valid_pipeline_settings():
    with mock.patch('dltctl.cli._get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def valid_pipeline_settings_job_id():
    with mock.patch('dltctl.cli._get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_job_id/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def valid_pipeline_settings_no_id():
    with mock.patch('dltctl.cli._get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_no_id/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def invalid_pipeline_settings():
    with mock.patch('dltctl.cli._get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def settings_save_mock():
    with mock.patch('dltctl.cli.PipelineSettings.save') as settings_save_mock:
        _settings_save_mock = mock.MagicMock()
        settings_save_mock.return_value = _settings_save_mock
        yield _settings_save_mock

@pytest.fixture()
def pipeline_artifacts():
    with mock.patch('dltctl.cli._get_dlt_artifacts') as pipeline_artifacts:
        pipeline_artifacts.return_value = ["foo.py","bar.sql"]
        yield pipeline_artifacts

@pytest.fixture()
def no_pipeline_artifacts():
    with mock.patch('dltctl.cli._get_dlt_artifacts') as no_pipeline_artifacts:
        no_pipeline_artifacts.return_value = None
        yield no_pipeline_artifacts


@pytest.fixture()
def pipelines_api_mock():
    with mock.patch('dltctl.cli.PipelinesApi') as PipelinesApiMock:
        _pipelines_api_mock = mock.MagicMock()
        PipelinesApiMock.return_value = _pipelines_api_mock
        yield _pipelines_api_mock

@pytest.fixture()
def workspace_api_mock():
    with mock.patch('dltctl.cli.WorkspaceApi') as WorkspaceApiMock:
        _workspace_api_mock = mock.MagicMock()
        WorkspaceApiMock.return_value = _workspace_api_mock
        yield _workspace_api_mock

@pytest.fixture()
def jobs_api_mock():
    with mock.patch('dltctl.cli.JobsApi') as JobsApiMock:
        _jobs_api_mock = mock.MagicMock()
        JobsApiMock.return_value = _jobs_api_mock
        yield _jobs_api_mock

@pytest.fixture()
def click_ctx():
    """
    A dummy Click context to allow testing of methods that raise exceptions. Fixes Click capturing
    actual exceptions and raising its own `RuntimeError: There is no active click context`.
    """
    return Context(Command('cmd'))

@provide_conf
def test_create_pipeline_valid_settings(pipelines_api_mock, workspace_api_mock, valid_pipeline_settings_no_id, settings_save_mock):
 
    runner = CliRunner()
    result = runner.invoke(cli.create)
    assert "Creating pipeline named: mycoolname" in result.stdout
    assert result.exit_code == 0


def test_create_pipeline_valid_name(pipelines_api_mock, workspace_api_mock, settings_save_mock):
 
    runner = CliRunner()
    result = runner.invoke(cli.create, args=["fooname"])
    assert "Creating pipeline named: fooname" in result.stdout
    assert result.exit_code == 0

def test_create_pipeline_no_name():
 
    runner = CliRunner()
    result = runner.invoke(cli.create)
    assert "Missing pipeline name" in result.stdout
    assert result.exit_code == 1

def test_deploy_pipeline_no_name():
 
    runner = CliRunner()
    result = runner.invoke(cli.deploy)
    assert "Missing pipeline name" in result.stdout
    assert result.exit_code == 1

def test_start_pipeline_no_id(valid_pipeline_settings_no_id):
 
    runner = CliRunner()
    result = runner.invoke(cli.start)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_start_pipeline(valid_pipeline_settings, pipelines_api_mock, settings_save_mock):
 
    pipelines_api_mock.get_pipeline_settings.return_value = valid_pipeline_settings
    pipelines_api_mock.start_update.return_value = "some update message"
    pipelines_api_mock.stream_events.return_value = "some events"

    runner = CliRunner()
    result = runner.invoke(cli.start)
    assert result.exit_code == 0

def test_start_pipeline_as_job(valid_pipeline_settings, pipelines_api_mock, jobs_api_mock, settings_save_mock):
 
    path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
    settings = PipelineSettings().load(path)
    pipelines_api_mock.get_pipeline_settings.return_value = settings.to_json()
    pipelines_api_mock.edit.return_value = ""
    jobs_api_mock.create_job.return_value = {'job_id': "1337"}
    jobs_api_mock.run_now.return_value = ""

    runner = CliRunner()
    result = runner.invoke(cli.start,["--as-job"] )
    assert "Running non-interactively as a job" in result.stdout
    assert "Created job 1337" in result.stdout
    assert "Run started. Job ID: 1337" in result.stdout
    assert result.exit_code == 0

def test_start_pipeline_as_existing_job(valid_pipeline_settings_job_id, pipelines_api_mock, jobs_api_mock, settings_save_mock):
 
    path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_job_id/'
    settings = PipelineSettings().load(path)
    pipelines_api_mock.get_pipeline_settings.return_value = settings.to_json()
    pipelines_api_mock.edit.return_value = ""
    jobs_api_mock.run_now.return_value = "12345"

    assert settings.get_job_id() == "foo1338"

    runner = CliRunner()
    result = runner.invoke(cli.start,["--as-job"] )
    
    assert "Running non-interactively as a job" in result.stdout
    assert "Created job" not in result.stdout
    assert "Run started. Job ID: foo1338" in result.stdout
    assert result.exit_code == 0

def test_start_pipeline_as_existing_job_missing_job(valid_pipeline_settings_job_id, pipelines_api_mock, jobs_api_mock, settings_save_mock):
 
    path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_job_id/'
    settings = PipelineSettings().load(path)
    pipelines_api_mock.get_pipeline_settings.return_value = settings.to_json()
    pipelines_api_mock.edit.return_value = ""
    jobs_api_mock.create_job.return_value = {'job_id': "bar1338"}
    jobs_api_mock.run_now.side_effect = [Exception("does not exist"), "12345"]

    assert settings.get_job_id() == "foo1338"

    runner = CliRunner()
    result = runner.invoke(cli.start,["--as-job"] )
    
    assert "Running non-interactively as a job" in result.stdout
    assert "Created job" in result.stdout
    assert "Run started. Job ID: bar1338" in result.stdout
    assert result.exit_code == 0

def test_stop_pipeline_no_id(valid_pipeline_settings_no_id):
 
    runner = CliRunner()
    result = runner.invoke(cli.stop)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_no_id(valid_pipeline_settings_no_id):
 
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_no_artifacts(valid_pipeline_settings, no_pipeline_artifacts, workspace_api_mock, settings_save_mock):
 
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert "Unable to detect pipeline files" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_valid_artifacts(valid_pipeline_settings, pipeline_artifacts, workspace_api_mock, pipelines_api_mock, settings_save_mock):
    workspace_api_mock.get_default_workspace_path.return_value = "/Users/foo@databricks.com"
    workspace_api_mock.get_status.return_value = ""
    workspace_api_mock.import_workspace.return_value = ""
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert result.exit_code == 0

def test_delete_pipeline_no_id(valid_pipeline_settings_no_id):
 
    runner = CliRunner()
    result = runner.invoke(cli.delete)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_delete_pipeline(valid_pipeline_settings, pipelines_api_mock, settings_save_mock):
    runner = CliRunner()
    result = runner.invoke(cli.delete)
    assert "Pipeline successfully deleted" in result.stdout
    assert result.exit_code == 0
