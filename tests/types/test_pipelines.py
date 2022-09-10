import unittest
from unittest import mock
from pathlib import Path
from json import JSONDecodeError
import tempfile, os

from dltctl.types.pipelines import PipelineSettings
from dltctl.types.pipelines import ClusterConfig

class TestPipelineSettings(unittest.TestCase):
    def test_pipeline_settings_from_json(self):
        js = '{"name": "foo"}'
        expected = PipelineSettings(name="foo")
        self.assertEqual(expected.name, 'foo')
        self.assertEqual(expected.continuous, False)
        self.assertEqual(expected.channel, 'CURRENT')

    def test_multi_cluster(self):
        js = """{ 
  "cluster_name": "my-cluster",
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "spark_conf": {
    "spark.speculation": true
  },
  "aws_attributes": {
    "availability": "SPOT",
    "zone_id": "us-west-2a"
  },
  "num_workers": 5
  }"""
        maint_js = """{ 
  "label": "maintenance",
  "cluster_name": "my-cluster",
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "spark_conf": {
    "spark.speculation": true
  },
  "aws_attributes": {
    "availability": "SPOT",
    "zone_id": "us-west-2a"
  },
  "num_workers": 5
  }"""
        js_conf = ClusterConfig().from_json(js).to_dict()
        maintenance_conf = ClusterConfig().from_json(maint_js).to_dict()
        pipeline_settings_json = """{
    "libraries": [],
    "clusters": [
        {      
  "cluster_name": "my-cluster",
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "spark_conf": {
    "spark.speculation": true
  },
  "aws_attributes": {
    "availability": "SPOT",
    "zone_id": "us-west-2a"
  },
  "num_workers": 5
  },
  { 
  "label": "maintenance",
  "cluster_name": "my-cluster",
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "spark_conf": {
    "spark.speculation": true
  },
  "aws_attributes": {
    "availability": "SPOT",
    "zone_id": "us-west-2a"
  },
  "num_workers": 5
  }
    ],
    "continuous": false,
    "development": true,
    "edition": "advanced",
    "photon": false,
    "channel": "CURRENT",
    "name": "mypipeline"
}"""
        settings = PipelineSettings(clusters=[js_conf, maintenance_conf])
        settings_dict = settings.to_json()
        test_settings_dict = PipelineSettings().from_json(pipeline_settings_json).to_json()

        self.assertEqual(settings_dict["clusters"][0]["label"], test_settings_dict["clusters"][0]["label"])
        self.assertEqual(settings_dict["clusters"][0]["label"], "default")
        self.assertEqual(settings_dict["clusters"][1]["label"], test_settings_dict["clusters"][1]["label"])
        self.assertEqual(settings_dict["clusters"][0]["aws_attributes"], test_settings_dict["clusters"][1]["aws_attributes"])


    def test_pipeline_settings_to_json(self):
        settings = PipelineSettings(name="foo")
        settings_json = settings.to_json()

        self.assertEqual(settings_json["name"], "foo")
        self.assertEqual(settings_json["development"], True)
        self.assertEqual(settings_json["photon"], False)

    def test_save(self):
      settings = PipelineSettings()
      with tempfile.TemporaryDirectory() as tmpdirname:
        settings.save(tmpdirname)
        self.assertIn("pipeline.json",os.listdir(tmpdirname))
    
    def test_load_valid(self):
      path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
      settings = PipelineSettings().load(path)
      self.assertEqual(settings.id, 'some-id')
      self.assertEqual(settings.name, 'mycoolname')
      self.assertEqual(settings.get_job_id(), "foo")

    def test_load_invalid(self):
      path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
      settings = PipelineSettings()
      self.assertRaises(JSONDecodeError, lambda: settings.load(path))

    def test_load_invalid_key(self):
      path = str(Path(__file__).parent.parent.resolve()) + '/files/invalidkey/'
      settings = PipelineSettings().load(path)
      self.assertEqual(hasattr(settings, "foo"), False)

    def test_load_no_pipeline_json(self):
      #FileNotFoundError
      path = str(Path(__file__).parent.parent.resolve()) + '/files/nopipelinefile'
      self.assertRaises(FileNotFoundError, lambda: PipelineSettings().load(path))

    def test_set_get_job_id(self):
      settings = PipelineSettings()
      settings.set_job_id("foo")
      self.assertEqual(settings.configuration["job_id"], "foo")
      self.assertEqual(settings.get_job_id(), "foo")
     





