class PermissionsApi:
    def __init__(self, client):
       self.client=client

    def get_pipeline_permissions(self, pipeline_id):
        headers = {}
        _data = {}
        response = self.client.perform_query(
            'GET', f'/permissions/pipelines/{pipeline_id}', data=_data, headers=headers)
        
        return response["access_control_list"]

    def set_pipeline_permissions(self, pipeline_id, acls):
        headers = {}
        _data = {}
        _data["access_control_list"] = acls
        response = self.client.perform_query(
            'PUT', f'/permissions/pipelines/{pipeline_id}', data=_data, headers=headers)
        
        return response

    def get_notebook_permissions(self, notebook_id):
        return

    def set_notebook_permissions(self):
        return

    def get_job_permissions(self,job_id):
        headers = {}
        _data = {}
        response = self.client.perform_query(
            'GET', f'/permissions/jobs/{job_id}', data=_data, headers=headers)
        
        return response["access_control_list"]
    
    def set_job_permissions(self, job_id, acls):
        headers = {}
        _data = {}
        _data["access_control_list"] = acls
        response = self.client.perform_query(
            'PUT', f'/permissions/jobs/{job_id}', data=_data, headers=headers)
        return response
