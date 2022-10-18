## dltctl
![Build badge](https://img.shields.io/github/workflow/status/bkvarda/dltctl/testing)  
A CLI tool for fast local iteration on Delta Live Tables pipelines and rapid deployment 

#### Installation
```
pip install dltctl
```

#### First-time Configuration
In order to function dltctl needs to know which Databricks workspace and which tokens/auth info to use. If you already use the databricks cli, dltctl will use whatever you've configured there. Otherwise, you can configure it using the same commands you would with the databricks CLI:
```
dltctl configure --jobs-api-version=2.1 --token
```

#### Usage
 dltctl requires a configuration file in order to function. To generate one, run:
```
dltctl init mypipeline
```
That will generate a dltctl.yaml file in your current directory that looks like this:
```
pipeline_settings:
  channel: CURRENT
  clusters:
  - autoscale:
      max_workers: 5
      min_workers: 1
      mode: ENHANCED
    driver_node_type_id: c5.4xlarge
    label: default
    node_type_id: c5.4xlarge
  continuous: false
  development: true
  edition: advanced
  name: mypipeline
  photon: false
```
This is a minimally viable dlt project yaml file. For more advanced settings, edit the file directly or use flags:
```
dltctl init mypipeline -f -c '{"label":"default", "aws_attributes": {"instance_profile_arn":"myprofilearn"}}'
```
Now you just need to bring your own DLT pipeline.  
Or if you just want to get started, you can try this:
```
echo "CREATE LIVE TABLE $(whoami | sed 's/\.//g')_dltctl_quickstart AS SELECT 1" > test.sql
```

Now you have the basics for a DLT pipeline deployment. You can deploy with dltctl like this:

```
dltctl deploy mypipeline
```
By default, dltctl uses a bunch of sane defaults to make getting started easy:
- It will search your current working directory recursively for .py and .sql files and add them as libraries to your DLT pipeline. To override this behavior, use the --pipeline-files-dir argument and specify a different directory, or use the
- It will use the same default pipeline configurations as the DLT UI
- It will upload your pipeline files to the Databricks workspace and convert them to notebooks using the Import API. By default it automatically determines your username and will store them in your user directory. You can override this behavior by specifying a workspace target using the --workspace-path flag or with config file settings. 
- It will then create and start the pipeline based on settings in dltctl.yaml 

Say you make changes to your code and want to restart your pipeline with your new version of the code, or with different pipeline settings. Simply update your pipeline settings (pipeline.json), save your code changes and run:
```
dltctl deploy
``` 

As an alternative, you could instead create a pipeline without starting it:
```
dltctl create
```
You can stage new files and settings to the workspace as often as you make changes:
```
dltctl stage
```
You can start and stop the pipeline manually:
```
dltctl start
dltctl stop
```
Or as before, you can still use dltctl deploy to combine all of these together.
```
dltctl deploy
```
You can trigger a full refresh using the -r flag:
```
dltctl start -r
dltctl deploy -r
```
If you don't want to watch the events, you can instead start or deploy as a job instead. You need to at least add a job_config parameter with a job name to your config file to do that. A minimally viable dltctl.yaml with job config looks like this:
```
job_config:
  name: mydltctljob
pipeline_settings:
  channel: CURRENT
  clusters:
  - autoscale:
      max_workers: 5
      min_workers: 1
      mode: ENHANCED
    driver_node_type_id: c5.4xlarge
    label: default
    node_type_id: c5.4xlarge
  continuous: false
  development: true
  edition: advanced
  photon: false
  name: mypipeline
```
Then you can run as a job instead:
```
dltctl deploy --as-job
```
Note that `dltctl deploy` and `dltctl stage` won't push changes and/or restart the pipeline if there aren't any changes. This means that adding a job config without changing anything else won't result in a job immediately created. You can force update though with the `--force` flag:
```
dltctl deploy --as-job --force
```
Or alternatively you can just start as a job since there are no other changes:
```
dltctl start --as-job
```
Here is an example of a more advanced dltctl.yaml:
```
pipeline_files_local_dir: .
pipeline_files_workspace_dir: /Users/foo@foo.com/dltctl_artifacts/nested_dir
job_config:
  name: foobk1234
  email_notifications:
    #on_start: [foo@foo.com]
    on_failure: [bar@bar.com]
  schedule:
    quartz_cron_expression: "0 0 12 * * ?"
    timezone_id: "America/Los_Angeles"
    pause_status: "UNPAUSED"
  tags:
    foo: bar
    bar: baz
pipeline_settings:
  channel: CURRENT
  clusters:
  - autoscale:
      max_workers: 4
      min_workers: 1
      mode: "ENHANCED"
    driver_node_type_id: c5.4xlarge
    label: default
    node_type_id: c5.4xlarge
    init_scripts:
    - dbfs:
        destination: dbfs:/bkvarda/init_scripts/datadog-install-driver-workers.sh
    spark_env_vars:
      DD_API_KEY: "{{secrets/bkvarda_dlt/dd_api_key}}"
      DD_ENV: dlt_test_pipeline
      DD_SITE: https://app.datadoghq.com
  continuous: true
  development: true
  edition: advanced
  name: foobk1234
  photon: false
  configuration:
    destination_table: "b"
    starting_offsets: "earliest"
```





