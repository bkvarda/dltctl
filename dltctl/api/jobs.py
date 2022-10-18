from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
import time

class JobsApi(JobsApi):  
    
    def get_job_id_by_name(self, name):
        jobs_with_name = self._list_jobs_by_name(name)
        if(len(jobs_with_name) < 1):
            return None
        elif (len(jobs_with_name) > 1):
            raise Exception("Unable to get job by name - Multiple jobs with the same name. Jobs:" + str(jobs_with_name))
        else:
            return jobs_with_name[0]["job_id"]

    def run_now(self, job_id, full_refresh=False):
        res = self.client.run_now(job_id, jar_params=None, notebook_params=None, python_params=None,
                                   spark_submit_params=None, python_named_params=None,
                                   idempotency_token=None, headers=None, version=None, pipeline_params={"full_refresh": full_refresh})
        return res["run_id"]

    def ensure_run_start(self, run_id):
        status = 'PENDING'
        status_msg = ''
        runs_api = RunsApi(self.client.client)
        while status == 'PENDING':
            run = runs_api.get_run(run_id)
            state = run["state"]
            status = state["life_cycle_state"]
            status_msg = state["state_message"]
            time.sleep(3)

             
        # Do a short wait to see if it transitions to a failed state rapidly
        if status == 'RUNNING':
           time.sleep(10)
           run = runs_api.get_run(run_id)
           state = run["state"]
           status = state["life_cycle_state"]
           status_msg = state["state_message"]

    
        if status == 'SKIPPED':
            raise(Exception(f"Run skipped. Status: {status}. Message: {status_msg}"))
        elif 'ERROR' in status or 'FAILED' in status:
            raise Exception(f"Run failed immediately after start. Status: {status}. Message: {status_msg}")
        else:
            return
        
