from databricks_cli.jobs.api import JobsApi

class JobsApi(JobsApi):  

    def run_now(self, job_id, full_refresh=False):
        return self.client.run_now(job_id, jar_params=None, notebook_params=None, python_params=None,
                                   spark_submit_params=None, python_named_params=None,
                                   idempotency_token=None, headers=None, version=None, pipeline_params={"full_refresh": full_refresh})
