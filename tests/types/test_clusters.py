import unittest

from dltctl.types.pipelines import ClusterConfig

class TestCreateClusterConfig(unittest.TestCase):
    def test_cluster_config_from_json(self):
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
  "num_workers": 25
  }"""
        expected = ClusterConfig(
            cluster_name="my-cluster",
            node_type_id="i3.xlarge",
            spark_conf={"spark.speculation":True},
            aws_attributes={"availability":"SPOT","zone_id":"us-west-2a"},
            num_workers=25)

        expected_dict = {
            "label": "default",
            "cluster_name": "my-cluster",
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.speculation": True
            },
            "aws_attributes": {
                "availability": "SPOT",
                "zone_id": "us-west-2a"
            },
            "num_workers": 25
        }

        testconf = ClusterConfig().from_json(js)
        testdict = testconf.to_dict()
    
        
        self.assertEqual(expected.cluster_name, testconf.cluster_name)
        self.assertEqual(expected.num_workers, testconf.num_workers)
        self.assertEqual(testconf.num_workers, 25)
        self.assertEqual(expected.aws_attributes, testconf.aws_attributes)
        self.assertEqual(testconf.autoscale, None)
        self.assertEqual(hasattr(testconf,"spark_version"),False)
        self.assertEqual(expected_dict, testdict)