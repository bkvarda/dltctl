## dltctl
![Build badge](https://img.shields.io/github/workflow/status/bkvarda/dltctl/testing)  
A CLI tool for fast local iteration on Delta Live Tables pipelines and rapid deployment 

#### Installation
```
pip install git+https://github.com/bkvarda/dltctl.git
```

#### First-time Configuration
In order to function dltctl needs to know which Databricks workspace and which tokens/auth info to use. If you already use the databricks cli, dltctl will use whatever you've configured there. Otherwise, you can configure it using the same commands you would with the databricks CLI:
```
dltctl configure --jobs-api-version=2.1 --token
```

#### Usage
 The easiest way to get started is to run a dlt with your dlt pipeline code:
```
dltctl deploy mypipeline
```
By default, dltctl uses a bunch of sane defaults to make getting started easy:
- It will search your current working directory for .py and .sql files and add them as libraries to your DLT pipeline. To override this behavior, use the --pipeline-files argument and specify a comma delimited list of full paths to files
- It will use the same default pipeline configurations as the DLT UI
- It will upload your pipeline files to the Databricks workspace and convert them to notebooks using the Import API. By default it automatically determines your username and will store them in your user directory. You can override this behavior by specifying a workspace target using the --workspace-path flag. 
- It will then create and start the pipeline, and store the pipeline and cluster settings locally in a file called pipeline.json. The pipeline ID is also stored in this file, and will be automatically used in subsequent dltctl commands unless instructed not to. 
- If the pipeline ID exists in the pipeline settings file, it will know that it needs to update a running pipeline, so will stop the executing pipeline, update the settings and the code, and then start it again. 

Say you make changes to your code and want to restart your pipeline with your new version of the code, or with different pipeline settings. Simply update your pipeline settings (pipeline.json), save your code changes and run:
```
dltctl deploy
``` 
If you don't currently have a pipeline written and aren't ready to deploy, you can instead initialize pipeline and cluster settings without deploying to yourworkspace like this:
```
dltctl init mypipeline
```
That will result in a pipeline.json settings file that looks like this:
```
{
    "libraries": [],
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "driver_node_type_id": "c5.4xlarge",
            "node_type_id": "c5.4xlarge"
        }
    ],
    "continuous": false,
    "development": true,
    "edition": "advanced",
    "photon": false,
    "channel": "CURRENT",
    "name": "mypipeline"
}
```
You can edit this file directly or use the flags in the init command to change what this looks like. For example, you can specify a specific cluster config like this:
```
dltctl init mypipeline -f -c '{"label":"default", "aws_attributes": {"instance_profile_arn":"myprofilearn"}}'
```
This pipeline config file is used by default in other commands. For example, you could then create a pipeline without starting it:
```
dltctl create
```
You can stage new files to the workspace as often as you make changes:
```
dltctl stage
```
You can start and stop the pipeline manually:
```
dltctl start
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
If you don't want to watch the events, you can instead start or deploy as a job instead:
```
dltctl deploy --full-refresh --as-job
```





