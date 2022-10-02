#!/usr/bin/env python3
import click
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.configure.cli import configure_cli
from databricks_cli.utils import pipelines_exception_eater
from dltctl.core import commands
from dltctl.core.constants import *

@click.group(help="CLI for development and CI/CD of DLT Pipelines")
def cli():
    pass

cli.add_command(configure_cli, name='configure')

@cli.command()
@click.argument('pipeline_name', type=str, default=None, required=False)
@click.option('-j', '--as-job', 'as_job', is_flag=True, help=AS_JOB_HELP)
@click.option('-r', '--full-refresh', 'full_refresh', is_flag=True, help=FULL_REFRESH_HELP)
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=WORKSPACE_PATH_HELP)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=PIPELINE_FILES_HELP)
@click.option('-v', '--verbose-events', 'verbose_events', is_flag=True, help=VERBOSE_EVENTS_HELP)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def deploy(api_client, as_job, full_refresh, pipeline_name, pipeline_files, workspace_path, verbose_events, pipeline_config):
    """Stages artifacts, creates/starts and/or restarts a DLT pipeline"""
    commands.deploy(
        api_client, as_job, 
        full_refresh, pipeline_name, 
        pipeline_files, workspace_path, 
        verbose_events, pipeline_config)
  
@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=WORKSPACE_PATH_HELP)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=PIPELINE_FILES_HELP)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
def stage(api_client, pipeline_config, pipeline_files, workspace_path):
    """Stages DLT pipeline code artifacts as notebooks and updates settings."""
    commands.stage(api_client, pipeline_config, pipeline_files, workspace_path)
    

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
def stop(api_client, pipeline_config):
    """Stops a pipeline if it is running."""
    commands.stop(api_client, pipeline_config)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.argument('pipeline_name', type=str, required=False)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
@click.option('-w', '--workspace-path', 'workspace_path', type=str, help=WORKSPACE_PATH_HELP)
@click.option('-f', '--pipeline-files', 'pipeline_files', type=click.Path(), help=PIPELINE_FILES_HELP)
def create(api_client, pipeline_config, pipeline_name, workspace_path, pipeline_files):
    """Creates a pipeline with the specified configuration."""
    commands.create(api_client, pipeline_config, pipeline_name, workspace_path, pipeline_files)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path())
def delete(api_client, pipeline_config):
    """Deletes a pipeline"""
    commands.delete(api_client, pipeline_config)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-j', '--as-job', 'as_job', is_flag=True, help=AS_JOB_HELP)
@click.option('-r', '--full-refresh', 'full_refresh', is_flag=True, help=FULL_REFRESH_HELP)
@click.option('-c', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
def start(api_client, as_job, full_refresh, pipeline_config):
    """Starts a pipeline given a config file or pipeline ID"""
    commands.start(api_client, as_job, full_refresh, pipeline_config)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.option('-p', '--pipeline-config', 'pipeline_config', type=click.Path(), help=PIPELINE_CONFIG_HELP)
def show(api_client, pipeline_config):
    """Shows details about pipeline"""
    commands.show(api_client, pipeline_config)

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
    commands.init(
        pipeline_name, 
        edition, 
        channel, 
        continuous, 
        prod_mode, 
        enable_photon, 
        storage, 
        target, 
        policy_id, 
        configuration, 
        clusters, 
        force, 
        output_dir)

@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def test(api_client):
    return
   
@cli.command()
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
@click.argument('query')
def query(api_client, query):
    """Query tables to validate output"""
    commands.query(api_client, query)

if __name__ == '__main__':
    cli()