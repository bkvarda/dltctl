import json
from pathlib import Path

class ClusterConfig():
    def __init__(self, label = "default", num_workers=None,
    driver_node_type_id="c5.4xlarge", node_type_id="c5.4xlarge", 
    policy_id=None, aws_attributes=None, spark_conf=None, 
    spark_env_vars=None, init_script=None, cluster_log_conf=None,
    autoscale=None,custom_tags=None,
    cluster_name=None):
        self.label = label
        self.num_workers = num_workers
        self.autoscale = autoscale
        self.driver_node_type_id = driver_node_type_id
        self.node_type_id = node_type_id
        self.policy_id = policy_id
        self.aws_attributes = aws_attributes
        self.spark_conf = spark_conf
        self.spark_env_vars = spark_env_vars
        self.init_script = init_script
        self.custom_tags = custom_tags
        self.cluster_log_conf = cluster_log_conf
        self.cluster_name = cluster_name
    
    def from_json(self, json_str):
        try:
            settings_dict = json.loads(json_str)
            for k, v in settings_dict.items():
                if hasattr(self, k):
                  setattr(self, k, v)
        except Exception as e:
            raise
        
        return self

    def to_dict(self):
        base_dict = self.__dict__
        cluster_dict = {}
        for k, v in base_dict.items():
            if v is None:
                pass
            else:
                cluster_dict[k] = v
              
        return cluster_dict



class PipelineSettings():
    def __init__(self, name = None, libraries = None, 
    edition = 'advanced', target=None, storage=None,
    continuous=False,photon=False, 
    channel='CURRENT', development_mode=True, id = None, 
    configuration=None, clusters=None, pipeline_files=None):
        self.name = name
        self.libraries = libraries
        self.edition= edition
        self.target = target
        self.storage = storage
        self.continuous = continuous
        self.photon = photon
        self.channel = channel
        self.development_mode = development_mode
        self.id = id
        self.configuration = configuration
        self.clusters = clusters
        self.pipeline_files = pipeline_files
    
    
    def load(self, directory):
        settings_path  = Path(directory,"pipeline.json").as_posix()
        try:
            with open(settings_path, 'r') as f:
                json_str = f.read()
                return self.from_json(json_str)
        except Exception as e:
            raise

    def save(self, directory):
        settings_path  = Path(directory,"pipeline.json").as_posix()
        try:
            with open(settings_path, 'w') as f:
                json.dump(self.to_json(), f, indent=4)
            
            return
        except Exception as e:
            raise
    
    def from_json(self, json_str):
        try:
            settings_dict = json.loads(json_str)
            for k, v in settings_dict.items():
                setattr(self, k, v)
        except Exception as e:
            raise
        
        return self

    def from_dict(self, dict):
        try:
            for k, v in dict.items():
                setattr(self, k, v)
        except Exception as e:
            raise
        return self

    def to_json(self):
        json_body = {
           "libraries": [

           ],
           "clusters": [

           ],
           "continuous": self.continuous,
           "development": self.development_mode,
           "edition": self.edition,
           "photon": self.photon,
           "channel": self.channel
        }
        if self.configuration:
            json_body["configuration"] = self.configuration
        if self.storage:
            json_body["storage"] = self.storage

        if self.target:
            json_body["target"] = self.target
        
        if self.id:
            json_body["id"] = self.id
        
        if self.name:
            json_body["name"] = self.name

        if self.libraries and not self.pipeline_files:
            json_body["libraries"] = self.libraries
        
        if self.pipeline_files:
            for i in range(len(self.pipeline_files)):
                json_body["libraries"].append(
                    {"notebook": {"path": self.pipeline_files[i]}}
                )
        if self.clusters is None:
            default_cluster = ClusterConfig(autoscale={"min_workers":1, "max_workers":5})
            json_body["clusters"].append(default_cluster.to_dict())
        else:
            for c in self.clusters:
                json_body["clusters"].append(c)
        
        return json_body
