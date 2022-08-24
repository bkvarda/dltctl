## dltctl
A CLI tool for fast local iteration on Delta Live Tables pipelines and rapid deployment

#### Installation
```
pip install git+https://github.com/bkvarda/dltctl.git
```

#### First-time Configuration
In order to function dltctl needs to know which Databricks workspace and which tokens/auth info to use. If you already use the databricks cli, dltctl will use whatever you've configured there. Otherwise, you can configure it using the same commands you would with the databricks CLI:
```
dltctl configure --token=mydatabrickspattoken
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

Say you make changes to your code and want to restart your pipeline with your new version of the code, or with different pipeline settings. Simply update your pipeline settings (pipeline.json), save your code changes and run:
```
dltctl deploy
``` 



