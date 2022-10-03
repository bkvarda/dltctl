from dltctl.types.permissions import AclList
from dltctl.types.pipelines import PipelineSettings, JobConfig
from dltctl.types.project import ProjectConfig
from dltctl.utils.print_utils import event_print
from dltctl.api.pipelines import PipelinesApi
from dltctl.api.permissions import PermissionsApi
from dltctl.api.jobs import JobsApi
from dltctl.api.workspace import WorkspaceApi
from pathlib import Path
import os, copy, hashlib, base64, glob

def set_acls(api_client, settings):
    if not settings.access_config:
        event_print(
        type="cli_status",
        level='INFO',
        msg="No access_config in project settings. No changes to ACLs needed.")
        return

    pipelines_api = PipelinesApi(api_client)
    permissions_api = PermissionsApi(api_client)
    pipeline_id = pipelines_api.get_pipeline_id_by_name(settings.pipeline_settings.id)
    access_cfg = settings.access_config
    acls = AclList().from_access_config(access_cfg)
    current_acls = permissions_api.get_pipeline_permissions(pipeline_id)
    owner_info = AclList().from_arr(current_acls)
    acls.add(owner_info.owner_type, owner_info.owner,'IS_OWNER')
    PermissionsApi(api_client).set_pipeline_permissions(pipeline_id, acls.to_arr())
    if settings.job_config.name:
        # Set job ACLs
        print()

def create_or_update_job(api_client, settings, full_refresh, pipeline_id):
    
    # Check for JobConfig name in project settings
    if not settings.job_config:
        raise Exception('No job_config in dltctl.yaml. job_config is required to run as job')
    elif not settings.job_config.name:
        raise Exception('job_config present in dltctl.yaml but no name. Name is minimally required.')
    
    job_conf = settings.job_config
    job_id = JobsApi(api_client).get_job_id_by_name(settings.job_config.name)
    # If job doesn't exist for name create one
    if not job_id:
        event_print(
        type="cli_status",
        level='INFO',
        msg="Detected first time running as job. Creating job")

        job_conf.set_pipeline_task(pipeline_id, full_refresh=full_refresh)
        try:
            created_job_id = JobsApi(api_client).create_job(job_conf.to_dict())["job_id"]
            event_print(
              type="cli_status",
              level='INFO',
              msg=f"Created job {created_job_id}")

            return created_job_id
        except Exception as e:
            raise
    else:
        # Check to see if job needs to be updated given settings
        event_print(
            type="cli_status",
            level='INFO',
            msg="Checking for job diffs")
        if is_job_conf_diff(api_client, job_conf, job_id):
            # We need to update the job
            job_conf.set_pipeline_task(pipeline_id, full_refresh=full_refresh)
            payload = {"job_id": job_id, "new_settings": job_conf.to_dict()}
            event_print(
            type="cli_status",
            level='INFO',
            msg="Updating job with new settings")
            JobsApi(api_client).reset_job(payload)
        return job_id
    
def run_as_job(api_client, settings, full_refresh, pipeline_id):
    event_print(
            type="cli_status",
            level='INFO',
            msg="Running non-interactively as a job")
    
    # Check if associated job
    job_id = create_or_update_job(api_client, settings, full_refresh, pipeline_id)

    # Check if job is to be scheduled - if so, don't start it
    if settings.job_config.schedule:
        event_print(
            type="cli_status",
            level='INFO',
            msg="Job has schedule set. Nothing else to do")
        return

    
    # Otherwise start the job
    event_print(
              type="cli_status",
              level='INFO',
              msg=f"Starting job {job_id}")
    
    try:
        run_id = JobsApi(api_client).run_now(job_id,full_refresh=bool(full_refresh))
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"Watching run id: {run_id} to ensure no immediate failures")
        JobsApi(api_client).ensure_run_start(run_id)
        event_print(
             type="cli_status",
             level='INFO',
             msg=f"Run started. Job ID: {job_id}, Run ID: {run_id}")

    except Exception as e:
        raise
    
    return

def edit_and_stop_continuous(api_client, settings):
    """Workaround for pipelines Edit API which starts any continuous update"""
    if settings.continuous:
        PipelinesApi(api_client).edit(settings.id, settings)
        PipelinesApi(api_client).stop(settings.id)
    return

def get_save_dir(pipeline_config=None):
    if pipeline_config:
        return pipeline_config
    else:
        return os.getcwd()

def get_pipeline_settings(pipeline_config=None):
    
    current_dir = os.getcwd()
    local_settings_path = current_dir + '/pipeline.json'
    settings = PipelineSettings()

    #  Use another pipeline settings if defined
    if pipeline_config:
        settings = PipelineSettings().load(pipeline_config)
    #  Try to load a pipeline settings if in current directory
    elif os.path.exists(local_settings_path):
        settings = PipelineSettings().load(current_dir)
    # Otherwise use defaults
    else:
        pass
    return settings

def get_project_settings(settings_dir=None):
    current_dir = os.getcwd()
    local_settings_path = current_dir + '/dltctl.yaml'
    #  Use another pipeline settings if defined
    try:
       if settings_dir:
           return ProjectConfig().load(settings_dir)
       #  Try to load a pipeline settings if in current directory
       elif os.path.exists(local_settings_path):
           return ProjectConfig().load(current_dir)
       # Otherwise raise exception
       else:
           raise Exception("No dltctl.yaml found. Use dltctl init if you haven't created one yet.")
    except Exception as e:
        raise Exception(f"Project config YAML parsing error: {e}")
    
def is_job_conf_diff(api_client, job_conf, job_id):
    remote_settings = JobConfig().from_dict(JobsApi(api_client).get_job(job_id)["settings"])
    r = copy.deepcopy(remote_settings)
    # Don't compare tasks or description, we're concerned about the core settings
    r.tasks = None
    r.description = None
    s = copy.deepcopy(job_conf)
    settings_diff = not r.to_dict() == s.to_dict()
    if settings_diff:
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"Diff in job configuration found")
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"Current: {r.to_dict()}")
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"New: {s.to_dict()}")
    else:
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"No diff in job configuration found")
    return settings_diff

def is_access_conf_diff(api_client, access_conf):
    return
def is_pipeline_settings_diff(api_client, settings):
    remote_settings = PipelineSettings().from_dict(PipelinesApi(api_client).get_pipeline_settings(settings.id))
    r = copy.deepcopy(remote_settings)
    s = copy.deepcopy(settings)
    # Don't compare libraries or pipeline files, we're concerned about the core settings
    r.libraries = None
    r.pipeline_files = None
    r.storage = None
    r.id = None
    s.libraries = None
    s.pipeline_files = None
    s.storage = None
    s.id = None
    settings_diff = not r.to_json() == s.to_json()
    if settings_diff:
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"Diff in pipeline settings found")
    else:
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"No diff in pipeline settings found")
    return settings_diff


def get_dlt_artifacts(pipeline_files_dir=None):  
    files_dir = Path(pipeline_files_dir).as_posix() if pipeline_files_dir else os.getcwd()
    py_files = glob.glob(files_dir + '/**/*.py', recursive=True)
    sql_files = glob.glob(files_dir + '/**/*.sql', recursive=True)
    pipeline_files = py_files + sql_files

    if len(pipeline_files) < 1:
        return None
        
    return pipeline_files

def get_artifact_diffs(api_client, settings, artifacts):
    # This is a new pipeline, no need to look for diffs
    if not settings.id:
        return artifacts
    
    event_print(
            type="cli_status",
            level="INFO",
            msg="Checking for artifact diffs"
        )

    libs = PipelinesApi(api_client).get_pipeline_libraries(settings.id)
    remote_md5s = []
    local_md5s = []
    remote_md5_lookup = {}
    local_md5_lookup = {}
    for l in libs:
        content = WorkspaceApi(api_client).get_workspace_file_b64(l)
        decoded = base64.b64decode(content).decode("utf-8")
        first_nl = decoded.find('\n') + 1
        clean_str = decoded[first_nl:len(decoded)]
        clean_str = "".join([s for s in clean_str.splitlines(True) if s.strip("\r\n")])
        md5_hash = hashlib.md5(clean_str.encode('utf-8')).hexdigest()
        remote_md5s.append(md5_hash)
        remote_md5_lookup[md5_hash] = l
    for a in artifacts:
        with open(a) as f:
            content = f.read()
            content = "".join([s for s in content.splitlines(True) if s.strip("\r\n")])
            md5_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
            local_md5s.append(md5_hash)
            local_md5_lookup[md5_hash] = a
   
    keeps = set(remote_md5s).intersection(set(local_md5s))
    diffs = set(local_md5s) - set(remote_md5s)
    deletes = set(remote_md5s) - set(local_md5s)
    artifacts_to_keep = []
    artifacts_to_upload = []
    artifacts_to_delete = []

    for checksum in deletes:
        event_print(
        type="cli_status",
        level="INFO",
        msg=f"Remote {remote_md5_lookup[checksum]} will be de-referenced by pipeline or replaced by updated changes."
        )
        artifacts_to_delete.append(remote_md5_lookup[checksum])

    for checksum in keeps:
        event_print(
        type="cli_status",
        level="INFO",
        msg=f"Keeping: {remote_md5_lookup[checksum]}"
        )
        artifacts_to_keep.append(remote_md5_lookup[checksum])

    for checksum in diffs:
        event_print(
        type="cli_status",
        level="INFO",
        msg=f"Found diffs in: {local_md5_lookup[checksum]}"
        )
        artifacts_to_upload.append(local_md5_lookup[checksum])
    
    if len(diffs) == 0:
        event_print(
        type="cli_status",
        level="INFO",
        msg=f"No local artifact diffs found. Nothing new to upload."
        )

    ret = {"keep": artifacts_to_keep, "upload": artifacts_to_upload, "delete": artifacts_to_delete}
    return ret