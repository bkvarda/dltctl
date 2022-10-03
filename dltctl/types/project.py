from dltctl.types.pipelines import PipelineSettings, JobConfig, AccessConfig
from pathlib import Path
import yaml

class ProjectConfig:
    def __init__(self, pipeline_settings: PipelineSettings = None, 
    job_config: JobConfig = None, access_config : AccessConfig = None, 
    databricks_url: str = None,
    databricks_cluster_policy_id: str = None,
    pipeline_files_local_dir: Path = None,
    pipeline_files_workspace_dir: Path = None):
        self.pipeline_settings = pipeline_settings
        self.job_config = job_config
        self.access_config = access_config
        self.databricks_url = databricks_url
        self.databricks_cluster_policy_id = databricks_cluster_policy_id
        self.pipeline_files_local_dir = pipeline_files_local_dir
        self.pipeline_files_workspace_dir = pipeline_files_workspace_dir

    def to_dict(self):
        dct = {}
        if self.pipeline_settings:
            dct["pipeline_settings"] = self.pipeline_settings.to_json(
                omit_libraries=True, omit_id=True)
        if self.access_config:
            dct["access_config"] = self.access_config.to_dict()
        if self.job_config:
            dct["job_config"] = self.job_config.to_dict()
        if self.databricks_url:
            dct["databricks_url"] = self.databricks_url
        if self.databricks_cluster_policy_id:
            dct["databricks_cluster_policy_id"] = self.databricks_cluster_policy_id
        if self.pipeline_files_local_dir:
            dct["pipeline_files_local_dir"] = self.pipeline_files_local_dir
        if self.pipeline_files_workspace_dir:
            dct["pipeline_files_workspace_dir"] = self.pipeline_files_workspace_dir
        return dct

    def from_dict(self, proj_dict):
        try:
            for k, v in proj_dict.items():
                if hasattr(self, k):
                  if k == "pipeline_settings":
                    v = PipelineSettings().from_dict(proj_dict["pipeline_settings"])
                  elif k == "access_config":
                    v = AccessConfig().from_dict(proj_dict["access_config"])
                  elif k == "job_config":
                    v = JobConfig().from_dict(proj_dict["job_config"])
                  setattr(self, k, v)
        except Exception as e:
            raise   
        return self
 
    def to_yaml(self):
        return yaml.safe_dump(self.to_dict())

    def save(self, directory):
        proj_settings_path  = Path(directory,"dltctl.yaml").as_posix()
        with open(proj_settings_path, 'w') as f:
            yaml.safe_dump(self.to_dict(), f)
        return

    def load(self, directory):
        proj_settings_path  = Path(directory,"dltctl.yaml").as_posix()
        proj_dict = yaml.safe_load(Path(proj_settings_path).read_text())
        return self.from_dict(proj_dict)
