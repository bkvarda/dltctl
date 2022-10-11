from dltctl.core.helpers import *
from dltctl.core.constants import *
from dltctl.utils.print_utils import event_print
from dltctl.api.pipelines import PipelinesApi
from dltctl.types.pipelines import ClusterConfig,PipelineSettings
from dltctl.types.project import ProjectConfig
from pathlib import Path
import datetime

def create(api_client, proj_config_dir, workspace_path, pipeline_files_dir):
    """Creates a pipeline with the specified configuration."""
    try:
        proj_settings = get_project_settings(proj_config_dir)
        pipeline_files_dir = pipeline_files_dir if pipeline_files_dir else proj_settings.pipeline_files_local_dir
        workspace_path = workspace_path if workspace_path else proj_settings.pipeline_files_workspace_dir
        settings = proj_settings.pipeline_settings
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    workspace_api = WorkspaceApi(api_client)
    pipelines_api = PipelinesApi(api_client)


    if not settings.name:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Missing pipeline name argument or config. A pipeline name is required for a first-time deployment")
        exit(1)

    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)

    if settings.id:
       event_print("cli_status", level="ERROR", msg=f"Trying to create a pipeline using a name that already exists. Existing pipeline ID: {settings.id}")
       exit(1)

    if settings.libraries:
        pass
    else:
        if not workspace_path:
            workspace_path = workspace_api.get_default_workspace_path()
            

        pipeline_files = get_dlt_artifacts(pipeline_files_dir)

        if not pipeline_files:
          event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
          return
    
        artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files,workspace_path)
        settings.pipeline_files = artifacts
    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    event_print("cli_status", level="INFO", msg=f"Creating pipeline named: {settings.name}")
    json_settings = settings.to_json()
    pipeline = pipelines_api.create(settings=json_settings)
    #set_acls(api_client, proj_settings)

def deploy(api_client, as_job, full_refresh, pipeline_files_dir, workspace_path, verbose_events, proj_config_dir, force):
    """Stages artifacts, creates/starts and/or restarts a DLT pipeline"""
    try:
        proj_settings = get_project_settings(proj_config_dir)
        pipeline_files_dir = pipeline_files_dir if pipeline_files_dir else proj_settings.pipeline_files_local_dir
        workspace_path = workspace_path if workspace_path else proj_settings.pipeline_files_workspace_dir
        settings = proj_settings.pipeline_settings
        pipeline_files = get_dlt_artifacts(pipeline_files_dir)
        pipelines_api = PipelinesApi(api_client)
        workspace_api = WorkspaceApi(api_client)
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)


    if not settings.name:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Missing pipeline name in config. A pipeline name is required for a first-time deployment")
        exit(1)

    if not pipeline_files:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
        exit(1)
    # Update settings with any defined settings

    if not workspace_path:
        workspace_path = workspace_api.get_default_workspace_path()

    
    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)
    pipeline_files_diffs = get_artifact_diffs(api_client, settings, pipeline_files)

    try:
      if(settings.id):
        if (len(pipeline_files_diffs["upload"]) > 0 
             or len(pipeline_files_diffs["delete"]) > 0 
             or is_pipeline_settings_diff(api_client, settings)
             or bool(force)):
          
          if bool(force):
              event_print(
              type="cli_status",
              level="INFO",
              msg="Force flag was set - force uploading all artifacts"
          )
              artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files,workspace_path)
              settings.pipeline_files = artifacts
          else:
              artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files_diffs["upload"],workspace_path)
              settings.pipeline_files = artifacts + pipeline_files_diffs["keep"]
        else:
          event_print(
              type="cli_status",
              level="INFO",
              msg="No changes detected. Nothing new to deploy."
          )
          return
      else:
        artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files,workspace_path)
        settings.pipeline_files = artifacts
      
      json_settings = settings.to_json()
      # If there's a pipeline id, it's an update
      if(settings.id):
          pipeline = pipelines_api.get(settings.id)
          if pipeline["state"] == 'RUNNING':
              event_print(
              type="cli_status",
              level='INFO',
              msg=f"Pipeline {settings.id} is currrently RUNNING. Stopping pipeline.")
              update = pipelines_api.stop(settings.id)
              
      # Otherwise it's a new pipeline
      else:
          event_print(
              type="cli_status",
              level='INFO',
              msg=f"Detected first time deploy. Creating pipeline named {settings.name}")

          pipeline = pipelines_api.create(settings=json_settings)
          settings.id = pipeline["pipeline_id"]

          event_print(
              type="cli_status",
              level='INFO',
              msg=f"Successfully created with pipeline ID: {settings.id}") 

      event_print(
              type="cli_status",
              level='INFO',
              msg=f"Updating settings for pipeline ID: {settings.id}")

      #set_acls(api_client, proj_settings)

      # Workaround for Pipeline Edit API starting continuous pipelines
      if settings.continuous:
          edit_and_stop_continuous(api_client, settings)
      else:
          pipelines_api.edit(settings.id, settings)

      if(bool(as_job)):
        run_as_job(api_client=api_client, 
          settings=proj_settings, 
          full_refresh=bool(full_refresh),
          pipeline_id=settings.id)
      else:
          event_print(
                  type="cli_status",
                  level='INFO',
                  msg=f"Starting pipeline {settings.id}")
         
          pipelines_api.start_update(settings.id, bool(full_refresh))
          
          event_print(
                  type="cli_status",
                  level='INFO',
                  msg=f"Waiting for pipeline events...")
          ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    
          # If it's a streaming pipeline, we stop tailing events after some time without events
          if(settings.continuous):  
              pipelines_api.stream_events(settings.id, ts=ts, max_polls_without_events=10, verbose=bool(verbose_events))
              exit(0)
          else:
              pipelines_api.stream_events(settings.id, ts=ts, verbose=bool(verbose_events))
              exit(0)
      
    
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)

def delete(api_client, proj_config_dir):
    """Deletes a pipeline"""
    try:
        proj_settings = get_project_settings(proj_config_dir)
        settings = proj_settings.pipeline_settings
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    pipelines_api = PipelinesApi(api_client)

    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)
    
    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID found for configured name. Nothing to delete.")
        exit(1)

    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    res = pipelines_api.delete(settings.id)

    event_print(
            type="cli_status",
            level='INFO',
            msg="Pipeline successfully deleted and pipeline ID removed from config.")
    return

def init(pipeline_name, edition, channel, continuous, prod_mode, enable_photon, 
storage, target, policy_id, configuration, clusters, force, output_dir):
    """Initializes local pipeline and cluster settings"""
    
    output_dir = output_dir if output_dir else os.getcwd()
    if not force:
        output_path = Path(output_dir, 'dltctl.yaml').as_posix()
        if os.path.exists(output_path):
            event_print("cli_status", level="ERROR", msg=f"Settings already exist in {output_path}. Delete or use -f to overwrite")
            exit(1)

    dev_mode = False if bool(prod_mode) else True

    settings = PipelineSettings(
        name=pipeline_name,
        edition=edition,
        target=target,
        storage=storage,
        continuous=bool(continuous),
        photon=bool(enable_photon),
        channel=channel,
        development=dev_mode,
        configuration=configuration
    )

    if(len(clusters) < 1 and policy_id):
       c = ClusterConfig()
       c.policy_id = policy_id
       clusters = [c.to_dict()]
    elif(len(clusters) < 1):
        clusters = None
    else:
        cluster_confs = []
        labels = []
        for cluster in clusters:
            try:
                c = ClusterConfig().from_json(cluster)
                if policy_id:
                    c.policy_id = policy_id
                cluster_confs.append(c.to_dict())
                labels.append(c.label)
            except Exception as e:
                event_print("cli_status", level="ERROR", msg=f"Invalid JSON string for cluster config: {cluster}")
                exit(1)
        # Ensure there is only unique labels
        if len(set(labels)) != len(labels):
            event_print("cli_status", level="ERROR", msg=f"Cluster configs have duplicate labels: {labels}")
            exit(1)
        
        clusters = cluster_confs

    settings.clusters = clusters
    project_settings = ProjectConfig(pipeline_settings=settings)
    project_settings.save(output_dir)
    return

def show(api_client, proj_config_dir):
    try:
        proj_settings = get_project_settings(proj_config_dir)
        settings = proj_settings.pipeline_settings
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    pipelines_api = PipelinesApi(api_client)
    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)
    
    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to show.")
        return

    # TODO - make this prettier
    p = pipelines_api.get(settings.id)
    print(p)
    return

def stage(api_client, proj_config_dir, pipeline_files_dir, workspace_path, force):
    try:
        proj_settings = get_project_settings(proj_config_dir)
        pipeline_files_dir = pipeline_files_dir if pipeline_files_dir else proj_settings.pipeline_files_local_dir
        workspace_path = workspace_path if workspace_path else proj_settings.pipeline_files_workspace_dir
        settings = proj_settings.pipeline_settings
        pipeline_files = get_dlt_artifacts(pipeline_files_dir)
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    workspace_api = WorkspaceApi(api_client)
    pipelines_api = PipelinesApi(api_client)

    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg=f"No pipeline ID found for pipeline {settings.name} or no settings found. Nothing to stage.")
        exit(1)

    if not pipeline_files:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
        exit(1)

    if not workspace_path:
        workspace_path = workspace_api.get_default_workspace_path()
    
    pipeline_files_diffs = get_artifact_diffs(api_client, settings, pipeline_files)

    if (len(pipeline_files_diffs["upload"]) > 0 
           or len(pipeline_files_diffs["delete"]) > 0 
           or is_pipeline_settings_diff(api_client, settings)
           or bool(force)):
        
        if bool(force):
            event_print(
            type="cli_status",
            level="INFO",
            msg="Force flag was set - force uploading all artifacts"
        )
            artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files,workspace_path)
            settings.pipeline_files = artifacts
        else:
            artifacts = workspace_api.upload_pipeline_artifacts(pipeline_files_diffs["upload"],workspace_path)
            settings.pipeline_files = artifacts + pipeline_files_diffs["keep"]

        # An edit starts a pipeline for a continuous pipeline which may not be desired.
    
        event_print(
            type="cli_status",
            level="INFO",
            msg="Updating Pipeline settings"
        )
        if settings.continuous:
            event_print(
                type="cli_status",
                level='WARNING',
                msg="The DLT Edit API will start a pipeline when set to continuous. Stopping pipeline after edit.")
            edit_and_stop_continuous(api_client, settings)
        else:
            pipelines_api.edit(settings.id, settings)
        
        #set_acls(api_client, proj_settings)

def start(api_client, as_job, full_refresh, proj_config_dir):
    """Starts a pipeline given a config file or pipeline ID"""
    try:
        proj_settings = get_project_settings(proj_config_dir)
        settings = proj_settings.pipeline_settings
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    
    pipelines_api = PipelinesApi(api_client)

    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID found for defined pipeline name. Nothing to start.")
        exit(1)

    # Check to see if the pipeline is already running
    pipeline = pipelines_api.get(settings.id)
    if pipeline["state"] == 'RUNNING':
        event_print(
        type="cli_status",
        level='ERROR',
        msg=f"Pipeline {settings.name} with ID {settings.id} is currrently RUNNING. Run dltctl stop to stop or use dltctl deploy")
        exit(1)

    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    
    if(bool(as_job)):
        run_as_job(api_client=api_client, 
          settings=proj_settings, 
          full_refresh=bool(full_refresh),
          pipeline_id=settings.id)
        exit(0)
    else:
        pipelines_api.start_update(settings.id, bool(full_refresh))
        pipelines_api.stream_events(settings.id, ts=ts, max_polls_without_events=10)
        exit(0)

def stop(api_client, proj_config_dir):
    """Stops a pipeline if it is running."""
    try: 
        proj_settings = get_project_settings(proj_config_dir)
        settings = proj_settings.pipeline_settings
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
    pipelines_api = PipelinesApi(api_client)

    settings.id = pipelines_api.get_pipeline_id_by_name(settings.name)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg=f"No existing pipeline with name {settings.name} found or no settings found. Nothing to stop.")
        exit(1)

    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    state = pipelines_api.get_pipeline_state(settings.id)
    
    event_print("cli_status", level="INFO", msg=f"Current state for pipeline {settings.id} : {state}")

    if state != 'RUNNING':
        event_print("cli_status", level="INFO", msg=f"Pipeline {settings.id} is not running. Nothing to do.")
        return
    else:
        pipelines_api.stop_async(settings.id)
        pipelines_api.stream_events(settings.id, ts=ts, max_polls_without_events=5)
