from json import JSONDecodeError
from unittest import mock
import pytest
import tempfile, os
from click import Context, Command
from click.testing import CliRunner
from pathlib import Path
from tests.utils import provide_conf
import dltctl.cli as cli
from dltctl.types.pipelines import PipelineSettings
from dltctl.types.project import ProjectConfig
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999  


@pytest.fixture()
def valid_pipeline_settings():
    with mock.patch('dltctl.core.commands.get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
        print(path)
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def valid_project_settings():
    with mock.patch('dltctl.core.commands.get_project_settings') as project_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
        print(path)
        settings = ProjectConfig().load(path)
        project_settings.return_value = settings
        yield project_settings

@pytest.fixture()
def invalid_project_settings():
    with mock.patch('dltctl.core.commands.get_project_settings') as project_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid_no_name/'
        print(path)
        settings = ProjectConfig().load(path)
        project_settings.return_value = settings
        yield project_settings

@pytest.fixture()
def valid_project_settings_continuous():
    with mock.patch('dltctl.core.commands.get_project_settings') as project_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_continuous/'
        print(path)
        settings = ProjectConfig().load(path)
        project_settings.return_value = settings
        yield project_settings

@pytest.fixture()
def valid_pipeline_settings_continuous():
    with mock.patch('dltctl.core.commands.get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_continuous/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def valid_pipeline_settings_job_id():
    with mock.patch('dltctl.core.commands.get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_job_id/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def valid_pipeline_settings_no_id():
    with mock.patch('dltctl.core.commands.get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_no_id/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def invalid_pipeline_settings():
    with mock.patch('dltctl.core.commands.get_pipeline_settings') as pipeline_settings:
        path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
        settings = PipelineSettings().load(path)
        pipeline_settings.return_value = settings
        yield pipeline_settings

@pytest.fixture()
def settings_save_mock():
    with mock.patch('dltctl.core.commands.PipelineSettings.save') as settings_save_mock:
        _settings_save_mock = mock.MagicMock()
        settings_save_mock.return_value = _settings_save_mock
        yield _settings_save_mock

@pytest.fixture()
def artifact_diffs():
    with mock.patch('dltctl.core.commands.get_artifact_diffs') as artifact_diffs:
        artifact_diffs.return_value = {"keep": [], 
        "upload": ["foo.py","bar.sql"], 
        "delete": []}
        yield artifact_diffs

@pytest.fixture()
def artifact_diffs_delete_only():
    with mock.patch('dltctl.core.commands.get_artifact_diffs') as artifact_diffs:
        artifact_diffs.return_value = {"keep": ["bar.py"], 
        "upload": [], 
        "delete": ["foo.py"]}
        yield artifact_diffs

@pytest.fixture()
def no_local_artifact_diffs():
    with mock.patch('dltctl.core.commands.get_artifact_diffs') as artifact_diffs:
        artifact_diffs.return_value = {"keep": ["bar.py"], 
        "upload": [], 
        "delete": []}
        yield artifact_diffs

@pytest.fixture()
def pipeline_artifacts():
    with mock.patch('dltctl.core.commands.get_dlt_artifacts') as pipeline_artifacts:
        pipeline_artifacts.return_value = ["foo.py","bar.sql"]
        yield pipeline_artifacts

@pytest.fixture()
def no_pipeline_artifacts():
    with mock.patch('dltctl.core.commands.get_dlt_artifacts') as no_pipeline_artifacts:
        no_pipeline_artifacts.return_value = None
        yield no_pipeline_artifacts

@pytest.fixture()
def edit_and_stop_continuous_mock():
    with mock.patch('dltctl.core.commands.edit_and_stop_continuous') as esc:
        esc.return_value = ""
        yield esc


@pytest.fixture()
def pipelines_api_mock():
    with mock.patch('dltctl.core.commands.PipelinesApi') as PipelinesApiMock:
        _pipelines_api_mock = mock.MagicMock()
        PipelinesApiMock.return_value = _pipelines_api_mock
        yield _pipelines_api_mock

@pytest.fixture()
def workspace_api_mock():
    with mock.patch('dltctl.core.commands.WorkspaceApi') as WorkspaceApiMock:
        _workspace_api_mock = mock.MagicMock()
        WorkspaceApiMock.return_value = _workspace_api_mock
        yield _workspace_api_mock

@pytest.fixture()
def helpers_jobs_api_mock():
    with mock.patch('dltctl.core.helpers.JobsApi') as HelpersJobsApiMock:
        _jobs_api_mock = mock.MagicMock()
        HelpersJobsApiMock.return_value = _jobs_api_mock
        yield _jobs_api_mock

@pytest.fixture()
def helpers_pipelines_api_mock():
    with mock.patch('dltctl.core.helpers.PipelinesApi') as HelpersPipelinesApiMock:
        _pipelines_api_mock = mock.MagicMock()
        HelpersPipelinesApiMock.return_value = _pipelines_api_mock
        yield _pipelines_api_mock

@pytest.fixture()
def click_ctx():
    """
    A dummy Click context to allow testing of methods that raise exceptions. Fixes Click capturing
    actual exceptions and raising its own `RuntimeError: There is no active click context`.
    """
    return Context(Command('cmd'))

@provide_conf
def test_create_pipeline_valid_settings(pipelines_api_mock, workspace_api_mock, valid_project_settings, valid_pipeline_settings_no_id, settings_save_mock):
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    pipelines_api_mock.create.return_value = ""
    runner = CliRunner()
    result = runner.invoke(cli.create)
    assert "Creating pipeline named: mycoolname" in result.stdout
    assert result.exit_code == 0


def test_create_pipeline_valid_name(pipelines_api_mock, workspace_api_mock, valid_project_settings, settings_save_mock):
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    runner = CliRunner()
    result = runner.invoke(cli.create)
    assert "Creating pipeline named: mycoolname" in result.stdout
    assert result.exit_code == 0

def test_create_pipeline_no_name(invalid_project_settings):
 
    runner = CliRunner()
    result = runner.invoke(cli.create)
    assert "Missing pipeline name" in result.stdout
    assert result.exit_code == 1

def test_deploy_pipeline_no_name(invalid_project_settings):
 
    runner = CliRunner()
    result = runner.invoke(cli.deploy)
    assert "Missing pipeline name" in result.stdout
    assert result.exit_code == 1

def test_start_pipeline_no_name(invalid_project_settings, pipelines_api_mock):
    
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    runner = CliRunner()
    result = runner.invoke(cli.start)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_start_pipeline(valid_pipeline_settings, valid_project_settings, pipelines_api_mock, settings_save_mock):
 
    pipelines_api_mock.get_pipeline_settings.return_value = valid_pipeline_settings
    pipelines_api_mock.start_update.return_value = "some update message"
    pipelines_api_mock.stream_events.return_value = "some events"

    runner = CliRunner()
    result = runner.invoke(cli.start)
    assert result.exit_code == 0

def test_start_pipeline_as_job(valid_project_settings,valid_pipeline_settings, pipelines_api_mock, settings_save_mock, helpers_jobs_api_mock, helpers_pipelines_api_mock):
 
    path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
    settings = ProjectConfig().load(path)
    pipelines_api_mock.get_pipeline_settings.return_value = settings.pipeline_settings.to_json()
    pipelines_api_mock.edit.return_value = ""
    helpers_pipelines_api_mock.edit.return_value = ""
    helpers_jobs_api_mock.run_now.return_value = ""
    helpers_jobs_api_mock.get_job_id_by_name.return_value = None
    helpers_jobs_api_mock.create_job.return_value = {'job_id': "1337"}
    helpers_jobs_api_mock.ensure_run_start.return_value = ""

    runner = CliRunner()
    result = runner.invoke(cli.start,["--as-job"] )
    assert "Running non-interactively as a job" in result.stdout
    assert "Created job 1337" in result.stdout
    assert "Run started. Job ID: 1337" in result.stdout
    assert result.exit_code == 0

def test_start_pipeline_as_existing_job(valid_project_settings, valid_pipeline_settings_job_id, pipelines_api_mock, helpers_jobs_api_mock, helpers_pipelines_api_mock, settings_save_mock):
 
    path = str(Path(__file__).parent.parent.resolve()) + '/files/valid_job_id/'
    settings = ProjectConfig().load(path)
    pipelines_api_mock.get_pipeline_settings.return_value = settings.pipeline_settings.to_json()
    pipelines_api_mock.edit.return_value = ""
    helpers_pipelines_api_mock.edit.return_value = ""
    helpers_jobs_api_mock.get_job_id_by_name.return_value = "foo1338"
    helpers_jobs_api_mock.run_now.return_value = "12345"
    helpers_jobs_api_mock.ensure_run_start.return_value = ""

    runner = CliRunner()
    result = runner.invoke(cli.start,["--as-job"] )
    
    assert "Running non-interactively as a job" in result.stdout
    assert "Run started. Job ID: foo1338" in result.stdout
    assert result.exit_code == 0


def test_stop_pipeline_no_id(invalid_project_settings, pipelines_api_mock):
    
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    runner = CliRunner()
    result = runner.invoke(cli.stop)
    assert "No existing pipeline with name" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_no_name(invalid_project_settings, pipelines_api_mock):
    
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_no_artifacts(valid_project_settings, no_pipeline_artifacts, workspace_api_mock, settings_save_mock, pipelines_api_mock):
    
    pipelines_api_mock.get_pipeline_id_by_name.return_value = "12345"
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert "Unable to detect pipeline files" in result.stdout
    assert result.exit_code == 1

def test_stage_pipeline_valid_artifacts(valid_project_settings, pipeline_artifacts, artifact_diffs, workspace_api_mock, pipelines_api_mock, settings_save_mock):
    workspace_api_mock.get_default_workspace_path.return_value = "/Users/foo@databricks.com"
    workspace_api_mock.get_status.return_value = ""
    workspace_api_mock.import_workspace.return_value = ""
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert result.exit_code == 0

def test_stage_pipeline_valid_artifacts_continous(valid_project_settings_continuous, pipeline_artifacts, artifact_diffs, workspace_api_mock, edit_and_stop_continuous_mock, pipelines_api_mock, settings_save_mock):
    workspace_api_mock.get_default_workspace_path.return_value = "/Users/foo@databricks.com"
    workspace_api_mock.get_status.return_value = ""
    workspace_api_mock.import_workspace.return_value = ""
    runner = CliRunner()
    result = runner.invoke(cli.stage)
    assert "The DLT Edit API will start a pipeline when set to continuous" in result.stdout
    assert result.exit_code == 0

def test_delete_pipeline_no_id(invalid_project_settings, pipelines_api_mock):
    
    pipelines_api_mock.get_pipeline_id_by_name.return_value = None
    runner = CliRunner()
    result = runner.invoke(cli.delete)
    assert "No pipeline ID" in result.stdout
    assert result.exit_code == 1

def test_delete_pipeline(valid_project_settings, pipelines_api_mock, settings_save_mock):
    runner = CliRunner()
    result = runner.invoke(cli.delete)
    assert "Pipeline successfully deleted" in result.stdout
    assert result.exit_code == 0

def test_init_config():
    with tempfile.TemporaryDirectory() as tmpdirname:
        runner = CliRunner()
        result = runner.invoke(cli.init, args=[
            "foo", 
            "-i", "12345", 
            "-o", f"{tmpdirname}",
            "-p", "-co"])
        p = ProjectConfig().load(tmpdirname)
        s = p.pipeline_settings
        assert s.clusters[0]["policy_id"] =="12345"
        assert s.continuous
        assert s.photon
        assert s.development

def test_init_config_clusters():
    with tempfile.TemporaryDirectory() as tmpdirname:
        runner = CliRunner()
        cluster_def = """{
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scripts": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
        result = runner.invoke(cli.init, args=[
            "foo", 
            "-i", "12345", 
            "-o", f"{tmpdirname}",
            "-c", f"{cluster_def}"])
        p = ProjectConfig().load(tmpdirname)
        s = p.pipeline_settings
        assert s.clusters[0]["policy_id"] =="12345"
        assert s.clusters[0]["init_scripts"]["dbfs"]["location"] == "dbfs:/foo/bar.sh"

def test_init_config_multiple_clusters():
    with tempfile.TemporaryDirectory() as tmpdirname:
        runner = CliRunner()
        cluster_def = """{
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scriptss": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
        other_cluster_def = """{
            "label": "maintenance",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scripts": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
        result = runner.invoke(cli.init, args=[
            "foo", 
            "-i", "12345", 
            "-o", f"{tmpdirname}",
            "-c", f"{cluster_def}",
            "-c", f"{other_cluster_def}" ])
        p = ProjectConfig().load(tmpdirname)
        s = p.pipeline_settings
        assert len(s.clusters) == 2
        assert s.clusters[0]["label"] == "default"
        assert s.clusters[1]["label"] == "maintenance"

def test_init_config_multiple_clusters_same_label():
    with tempfile.TemporaryDirectory() as tmpdirname:
        runner = CliRunner()
        cluster_def = """{
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scripts": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
        other_cluster_def = """{
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scripts": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
        result = runner.invoke(cli.init, args=[
            "foo", 
            "-i", "12345", 
            "-o", f"{tmpdirname}",
            "-c", f"{cluster_def}",
            "-c", f"{other_cluster_def}" ])
        
        assert "Cluster configs have duplicate labels" in result.stdout
        assert result.exit_code == 1

def test_init_invalid_cluster_conf():
    with tempfile.TemporaryDirectory() as tmpdirname:
        runner = CliRunner()
        cluster_def = """{
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,,,
            ,
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge",
            "init_scripts": {
                "dbfs": {
                    "location": "dbfs:/foo/bar.sh"
                }
            }
        }"""
       
        result = runner.invoke(cli.init, args=[
            "foo", 
            "-i", "12345", 
            "-o", f"{tmpdirname}",
            "-c", f"{cluster_def}"])
        
        assert "Invalid JSON string for cluster config" in result.stdout
        assert result.exit_code == 1
    
        



