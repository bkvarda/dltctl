import unittest
from unittest.mock import MagicMock
import pytest

from dltctl.types.pipelines import PipelineSettings
from dltctl.types.pipelines import ClusterConfig

class TestCreatePipelineConfig(unittest.TestCase):
    def test_pipeline_settings_from_json(self):
        js = '{"name": "foo"}'
        expected = PipelineSettings(name="foo")
        self.assertEqual(expected.name, 'foo')
        self.assertEqual(expected.cluster_min_workers, 1)
        self.assertEqual(expected.cluster_max_workers, 5)

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
    js_conf = ClusterConfig().from_json(js)
    maintenance_conf = ClusterConfig().from_json(js)
    maintenance_conf.label = "maintenance"
    settings.clusters = [js_conf.to_dict(),maintenance_conf.to_dict()]


    #def test_pipeline_settings_to_json(self):

