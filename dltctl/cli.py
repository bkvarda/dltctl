#!/usr/bin/env python3
from email.policy import default
from threading import local
import click
import os, datetime, json, time, sys
from pathlib import Path
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.configure.cli import configure_cli
from databricks_cli.utils import pipelines_exception_eater
from dltctl.api.pipelines import PipelinesApi
from dltctl.api.workspace import WorkspaceApi
from dltctl.api.jobs import JobsApi
from dltctl.types.pipelines import PipelineSettings, ClusterConfig, JobConfig
from dltctl.types.events import PipelineEventsResponse
from dltctl.api.warehouses import DBSQLClient
from dltctl.utils.print_utils import event_print

pipeline_name_help = "Name of DLT pipeline"
workspace_path_help = "Full workspace path to use for dlt pipeline file uploads"
pipeline_files_help = "Comma-delimited list of files that make up your DLT pipeline code and will be uploaded."
verbose_events_help = "Will print more verbose DLT event logs to the console"
pipeline_config_help = "Directory containing config named pipeline.json. If not specified, will look in local directory for pipeline.json"
full_refresh_help = "Whether to execute a full refresh of the pipeline"
as_job_help = "(experimental) Whether to run as a Databricks job non-interactively"

def _create_or_update_job(api_client, settings, full_refresh):
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
    
def _run_as_job(api_client, settings, full_refresh, settings_dir):
    event_print(
            type="cli_status",
            level='INFO',
            msg="Running non-interactively as a job")
    
    # Check if associated job
    settings = _create_or_update_job(api_client, settings, full_refresh)
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
            _run_as_job(api_client, settings, full_refresh, settings_dir)
        else:
            raise
    
    return


def _get_save_dir(pipeline_config=None):
    if pipeline_config:
        return pipeline_config
    else:
        return os.getcwd()

def _get_pipeline_settings(pipeline_config=None):
    
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

def _get_dlt_artifacts(pipeline_files=None):   
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



@click.group(help="CLI for development and CI/CD of DLT Pipelines")
def cli():
    pass

cli.add_command(configure_cli, name='configure')

@cli.command()
@click.argument('pipeline_name', type=str, default=None, required=False)
@click.option('-j', '--as-job', 'as_job', is_flag=True, help=as_job_help)
@click.option('-r', '--full-refresh', 'full_refresh', is_flag=True, help=full_refresh_help)
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=workspace_path_help)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=pipeline_files_help)
@click.option('-v', '--verbose-events', 'verbose_events', is_flag=True, help=verbose_events_help)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def deploy(api_client, as_job, full_refresh, pipeline_name, pipeline_files, workspace_path, verbose_events, pipeline_config):
    """Stages artifacts, creates/starts and/or restarts a DLT pipeline"""
    output_dir = _get_save_dir(pipeline_config)
    settings = _get_pipeline_settings(pipeline_config)
    pipeline_files = _get_dlt_artifacts(pipeline_files)

    if not settings.id and not pipeline_name and not settings.name:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Missing pipeline name argument or config. A pipeline name is required for a first-time deployment")
        exit(1)

    if not pipeline_files:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
        exit(1)
    # Update settings with any defined settings
    if not settings.name:
        settings.name = pipeline_name

    if not workspace_path:
        workspace_path = WorkspaceApi(api_client).get_default_workspace_path()

    try:
      artifacts = WorkspaceApi(api_client).upload_pipeline_artifacts(pipeline_files,workspace_path)
      settings.pipeline_files = artifacts
      json_settings = settings.to_json()
      
      # If there's a pipeline id, it's an update
      if(settings.id):
          pipeline = PipelinesApi(api_client).get(settings.id)
          if pipeline["state"] == 'RUNNING':
              event_print(
              type="cli_status",
              level='INFO',
              msg=f"Pipeline {settings.id} is currrently RUNNING. Stopping pipeline.")
              update = PipelinesApi(api_client).stop(settings.id)
              
      # Otherwise it's a new pipeline
      else:
          event_print(
              type="cli_status",
              level='INFO',
              msg=f"Detected first time deploy. Creating pipeline named {settings.name}")

          pipeline = PipelinesApi(api_client).create(settings=json_settings)
          settings = PipelineSettings().from_dict(PipelinesApi(api_client).get_pipeline_settings(pipeline["pipeline_id"]))

          event_print(
              type="cli_status",
              level='INFO',
              msg=f"Successfully created with pipeline ID: {settings.id}")
          settings.save(output_dir)
      
      settings.save(output_dir)
      event_print(
              type="cli_status",
              level='INFO',
              msg=f"Updating settings for pipeline ID: {settings.id}")
      PipelinesApi(api_client).edit(settings.id, settings)

      if(bool(as_job)):
        _run_as_job(api_client=api_client, 
          settings=settings, 
          full_refresh=bool(full_refresh),
          settings_dir=output_dir)
      else:
          event_print(
                  type="cli_status",
                  level='INFO',
                  msg=f"Starting pipeline {settings.id}")
          if not settings.continuous:
            PipelinesApi(api_client).start_update(settings.id, bool(full_refresh))
          
          event_print(
                  type="cli_status",
                  level='INFO',
                  msg=f"Waiting for pipeline events...")
          ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    
          # If it's a streaming pipeline, we stop tailing events after some time without events
          if(settings.continuous):  
              PipelinesApi(api_client).stream_events(settings.id, ts=ts, max_polls_without_events=10, verbose=bool(verbose_events))
          else:
              PipelinesApi(api_client).stream_events(settings.id, ts=ts, verbose=bool(verbose_events))
      
    
    except Exception as e:
        event_print(
              type="cli_status",
              level='ERROR',
              msg=f"{str(e)}")
        exit(1)
  
@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=workspace_path_help)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=pipeline_files_help)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
def stage(api_client, pipeline_config, pipeline_files, workspace_path):
    """Stages DLT pipeline code artifacts as notebooks and updates settings."""
    settings = _get_pipeline_settings(pipeline_config)
    output_dir = _get_save_dir(pipeline_config)
    pipeline_files = _get_dlt_artifacts(pipeline_files)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to stop.")
        exit(1)

    if not pipeline_files:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
        exit(1)

    if not workspace_path:
        workspace_path = WorkspaceApi(api_client).get_default_workspace_path()
    

    artifacts = WorkspaceApi(api_client).upload_pipeline_artifacts(pipeline_files,workspace_path)
    settings.pipeline_files = artifacts

    # An edit starts a pipeline for a continuous pipeline which may not be desired.
    if settings.continuous:
        event_print(
            type="cli_status",
            level='WARNING',
            msg="The DLT Edit API will start a pipeline when set to continuous. \
                Artifacts are uploaded but stage will not edit settings for a continuous pipeline. Use dltctl deploy instead.")
    else:
        pipeline = PipelinesApi(api_client).edit(settings.id, settings)
        
    settings.save(output_dir)
    

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
def stop(api_client, pipeline_config):
    """Stops a pipeline if it is running."""
    settings = _get_pipeline_settings(pipeline_config)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to stop.")
        exit(1)


    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    state = PipelinesApi(api_client).get_pipeline_state(settings.id)
    
    event_print("cli_status", level="INFO", msg=f"Current state for pipeline {settings.id} : {state}")

    if state != 'RUNNING':
        event_print("cli_status", level="INFO", msg=f"Pipeline {settings.id} is not running. Nothing to do.")
        return
    else:
        PipelinesApi(api_client).stop_async(settings.id)
        PipelinesApi(api_client).stream_events(settings.id, ts=ts, max_polls_without_events=5)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.argument('pipeline_name', type=str, required=False)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=workspace_path_help)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=pipeline_files_help)
def create(api_client, pipeline_config, pipeline_name, workspace_path, pipeline_files):
    """Creates a pipeline with the specified configuration."""
    output_dir = _get_save_dir(pipeline_config)
    settings = _get_pipeline_settings(pipeline_config)
    if settings.id:
        event_print("cli_status", level="ERROR", msg=f"Trying to create a pipeline using a config with a pipeline ID: {settings.id}")
        exit(1)

    if not pipeline_name and not settings.name:
        event_print(
            type="cli_status",
            level='ERROR',
            msg="Missing pipeline name argument or config. A pipeline name is required for a first-time deployment")
        exit(1)

    if settings.libraries:
        pass
    else:
        if not workspace_path:
            workspace_path = WorkspaceApi(api_client).get_default_workspace_path()
            

        pipeline_files = _get_dlt_artifacts(pipeline_files)

        if not pipeline_files:
          event_print(
            type="cli_status",
            level='ERROR',
            msg="Unable to detect pipeline files in current directory and no pipeline files specified. Pipeline files are required for a pipeline to be created")
          return
    
        artifacts = WorkspaceApi(api_client).upload_pipeline_artifacts(pipeline_files,workspace_path)
        settings.pipeline_files = artifacts
        settings.save(output_dir)
    
    # Update settings with any defined settings
    if not settings.name:
        settings.name = pipeline_name
    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    event_print("cli_status", level="INFO", msg=f"Creating pipeline named: {settings.name}")
    json_settings = settings.to_json()
    pipeline = PipelinesApi(api_client).create(settings=json_settings)
    settings = PipelineSettings().from_dict(PipelinesApi(api_client).get_pipeline_settings(pipeline["pipeline_id"]))
    settings.save(output_dir)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path())
def delete(api_client, pipeline_config):
    """Deletes a pipeline"""
    output_dir = _get_save_dir(pipeline_config)
    settings = _get_pipeline_settings(pipeline_config)
    
    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to delete.")
        exit(1)

    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    res = PipelinesApi(api_client).delete(settings.id)
    settings.id = None
    settings.save(output_dir)
    event_print(
            type="cli_status",
            level='INFO',
            msg="Pipeline successfully deleted and pipeline ID removed from config.")

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-j', '--as-job', 'as_job', is_flag=True, help=as_job_help)
@click.option('-r', '--full-refresh', 'full_refresh', is_flag=True, help=full_refresh_help)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
def start(api_client, as_job, full_refresh, pipeline_config):
    """Starts a pipeline given a config file or pipeline ID"""
    settings = _get_pipeline_settings(pipeline_config)
    output_dir = _get_save_dir(pipeline_config)
    
    p_api = PipelinesApi(api_client)

    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to start.")
        exit(1)

    # Check to see if the pipeline is already running
    pipeline = PipelinesApi(api_client).get(settings.id)
    if pipeline["state"] == 'RUNNING':
        event_print(
        type="cli_status",
        level='ERROR',
        msg=f"Pipeline {settings.id} is currrently RUNNING. Run dltctl stop to stop or use dltctl deploy")
        exit(1)


    updated_settings = PipelineSettings().from_dict(p_api.get_pipeline_settings(settings.id))
    settings = updated_settings
    settings.save(output_dir)


    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    
    if(bool(as_job)):
        _run_as_job(api_client=api_client, 
          settings=settings, 
          full_refresh=bool(full_refresh),
          settings_dir=output_dir)
    else:
        p_api.start_update(settings.id, bool(full_refresh))
        p_api.stream_events(settings.id, ts=ts, max_polls_without_events=10)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-p', '--pipeline-config', 'pipeline_config', type=click.Path(), help=pipeline_config_help)
def show(api_client, pipeline_config):
    """Shows details about pipeline"""
    settings = _get_pipeline_settings(pipeline_config)
    
    if not settings.id:
        event_print(
            type="cli_status",
            level='INFO',
            msg="No pipeline ID in settings or no settings found. Nothing to show.")
        return

    # TODO - make this prettier
    p = PipelinesApi(api_client).get(settings.id)
    print(p)


@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@click.argument('pipeline_name', type=str, required=True)
@click.option('-e', '--edition', 'edition', type=str, default="advanced", help="The DLT edition to use")
@click.option('-ch', '--channel', 'channel', type=str, default="CURRENT", help="The DLT channel to use")
@click.option('-co', '--continuous', 'continuous',  is_flag=True, help="Whether to use continuous mode or not")
@click.option('-pd', '--prod-mode', 'prod_mode', is_flag=True, help="Whether to use development mode")
@click.option('-p', '--enable-photon', 'enable_photon',is_flag=True, help="Enable photon for the cluster")
@click.option('-s', '--storage', 'storage', type=str, default=None, help="The default storage location for pipeline events and data")
@click.option('-t', '--target', 'target', type=str, default=None, help="The target db/schema for the pipeline tables")
@click.option('-i', '--policy-id', 'policy_id', type=str, default=None, help="The cluster policy ID to use")
@click.option('-cf','--config','configuration',type=str, default=None, help="Additional configuration JSON string of k/v pairs for Pipeline")
@click.option('-c', '--cluster', 'clusters', multiple=True, type=str, help="JSON cluster config to use for clusters. Can be specified multiple times with different lables")
@click.option('-f', '--force', 'force', default=False, is_flag=True, help="Whether to overwrite the existing pipline settings file with these settings, if it exists.")
@click.option('-o', '--output-dir', 'output_dir', default=None, help="Where to write the pipeline settings config file to. Defaults to current directory.")
def init(pipeline_name, edition, channel, continuous, prod_mode, enable_photon, 
storage, target, policy_id, configuration, clusters, force, output_dir):
    """Initializes local pipeline and cluster settings"""
    
    output_dir = output_dir if output_dir else os.getcwd()
    if not force:
        output_path = Path(output_dir, 'pipeline.json').as_posix()
        if os.path.exists(output_path):
            event_print("cli_status", level="ERROR", msg=f"Settings already exist in {output_path}. Delete or use -f to overwrite")
            return

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

    settings.save(output_dir)

   


@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def test(api_client):
    settings = _get_pipeline_settings()
    default_path = WorkspaceApi(api_client).get_default_workspace_path()
    print(default_path)


@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.argument('query')
def query(api_client, query):
    """Query tables to validate output"""
    DBSQLClient(api_client).query(query)

if __name__ == '__main__':
    cli()