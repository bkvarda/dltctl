from dltctl.types.pipelines import AccessConfig, JobConfig, PipelineSettings
from dltctl.types.project import ProjectConfig
from pathlib import Path
from yaml import YAMLError
import pytest, tempfile, os

def test_load_no_project_yaml():
      #FileNotFoundError
      path = str(Path(__file__).parent.parent.resolve()) + '/files/nopipelinefile'
      with pytest.raises(FileNotFoundError) as fnftest:
        ProjectConfig().load(path)

def test_load_valid():
      path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
      settings = ProjectConfig().load(path)
      assert settings.databricks_cluster_policy_id == 12345
      assert settings.databricks_url == "http://foo.com"
      assert settings.pipeline_settings.name == "mycoolname"
      assert settings.pipeline_settings.development == True
      assert settings.pipeline_settings.continuous == False
      assert settings.pipeline_settings.clusters[0]["autoscale"]["max_workers"] == 5
      assert settings.access_config.manager_groups == ['foo@foo.com', 'bar@bar.com']

def test_save():
      pipeline_settings = PipelineSettings()
      job_config = JobConfig()
      access_config = AccessConfig()
      settings = ProjectConfig(pipeline_settings,job_config,access_config)
      with tempfile.TemporaryDirectory() as tmpdirname:
        settings.save(tmpdirname)
        assert "dltctl.yaml" in os.listdir(tmpdirname)

def test_save_and_load():
      pipeline_settings = PipelineSettings(name="testname1337")
      print(pipeline_settings.clusters)
      job_config = JobConfig()
      access_config = AccessConfig()
      settings = ProjectConfig(pipeline_settings,job_config,access_config)
      assert settings.pipeline_settings.development == True
      assert settings.pipeline_settings.continuous == False
      assert settings.pipeline_settings.name == "testname1337"
      with tempfile.TemporaryDirectory() as tmpdirname:
        settings.save(tmpdirname)
        assert "dltctl.yaml" in os.listdir(tmpdirname)
        loaded_settings = ProjectConfig().load(tmpdirname)
        assert loaded_settings.pipeline_settings.development == True
        assert loaded_settings.pipeline_settings.continuous == False
        assert loaded_settings.pipeline_settings.name == "testname1337"
        assert loaded_settings.pipeline_settings.clusters[0]["autoscale"]["max_workers"] == 5

def test_load_invalid():
      path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
      with pytest.raises(YAMLError) as ymltest:
         ProjectConfig().load(path)
