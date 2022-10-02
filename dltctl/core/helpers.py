from dltctl.types.permissions import AclList
from dltctl.types.pipelines import PipelineSettings, JobConfig
from dltctl.utils.print_utils import event_print
from dltctl.api.pipelines import PipelinesApi
from dltctl.api.permissions import PermissionsApi
from dltctl.api.jobs import JobsApi
from dltctl.api.workspace import WorkspaceApi
import os, copy, hashlib, base64

def set_acls(api_client, settings):
    
    if not settings.has_access_config():
        return

    access_cfg = settings.get_access_config()
    acls = AclList().from_access_config(access_cfg)
    current_acls = PermissionsApi(api_client).get_pipeline_permissions(settings.id)
    owner_info = AclList().from_arr(current_acls)
    acls.add(owner_info.owner_type, owner_info.owner,'IS_OWNER')

    if settings.get_job_id():
        # Set job ACLs
        print()

    PermissionsApi(api_client).set_pipeline_permissions(settings.id, acls.to_arr())

def create_or_update_job(api_client, settings, full_refresh):
    # If job doesn't exist create one
        if not settings.get_job_id():
            event_print(
            type="cli_status",
            level='INFO',
            msg="Detected first time running as job. Creating job")

            job_conf = JobConfig(settings.name)
            job_conf.set_pipeline_task(settings.id, full_refresh=full_refresh)
            try:
                res = JobsApi(api_client).create_job(job_conf.to_dict())
                settings.set_job_id(res["job_id"])
                event_print(
                  type="cli_status",
                  level='INFO',
                  msg=f"Created job {settings.get_job_id()}")
                # We also need to update the pipeline config to reference the new job
                PipelinesApi(api_client).edit(settings.id, settings)
                return settings
            except Exception as e:
                raise
        else:
            return settings
    
def run_as_job(api_client, settings, full_refresh, settings_dir):
    event_print(
            type="cli_status",
            level='INFO',
            msg="Running non-interactively as a job")
    
    # Check if associated job
    settings = create_or_update_job(api_client, settings, full_refresh)
    settings.save(settings_dir)
    
    # Otherwise start the job
    event_print(
              type="cli_status",
              level='INFO',
              msg=f"Starting job {settings.get_job_id()}")
    
    try:
        run_id = JobsApi(api_client).run_now(settings.get_job_id(),full_refresh=bool(full_refresh))
        event_print(
              type="cli_status",
              level='INFO',
              msg=f"Watching run id: {run_id} to ensure no immediate failures")
        JobsApi(api_client).ensure_run_start(run_id)
        event_print(
             type="cli_status",
             level='INFO',
             msg=f"Run started. Job ID: {settings.get_job_id()}, Run ID: {run_id}")

    except Exception as e:
        # If the job in conf was deleted out of band we can create another
        if "does not exist" in str(e):
            event_print(
              type="cli_status",
              level='INFO',
              msg=f"Job {settings.get_job_id()} from settings does not exist. Creating a new job")
            settings.delete_job_id()
            run_as_job(api_client, settings, full_refresh, settings_dir)
        else:
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

def is_settings_diff(api_client, settings):
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


def get_dlt_artifacts(pipeline_files=None):   
    current_dir = os.getcwd()
    all_files = os.listdir(current_dir)
    #  Automatically add any local python or sql files as pipeline artifacts
    if pipeline_files is None:
       pipeline_files = []

    else:
        all_files = pipeline_files.replace(" ","").split(',')
        pipeline_files = []

    for f in all_files:
        if f.endswith(".py") or f.endswith(".sql"):
            pipeline_files.append(f)
      
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
        clean_str = "".join([s for s in clean_str.splitlines(True) if s.strip()])
        md5_hash = hashlib.md5(clean_str.encode('utf-8')).hexdigest()
        remote_md5s.append(md5_hash)
        remote_md5_lookup[md5_hash] = l
    for a in artifacts:
        with open(a) as f:
            content = f.read()
            content = "".join([s for s in content.splitlines(True) if s.strip()])
            md5_hash =hashlib.md5(content.encode('utf-8')).hexdigest()
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
        msg=f"No artifact diffs found. Nothing to upload."
        )

    ret = {"keep": artifacts_to_keep, "upload": artifacts_to_upload, "delete": artifacts_to_delete}
    return ret