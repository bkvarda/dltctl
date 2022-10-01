from pathlib import Path
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.workspace.types import WorkspaceLanguage, WorkspaceFormat
from dltctl.utils.print_utils import event_print

class WorkspaceApi(WorkspaceApi):

    def get_workspace_file_b64(self, path):
      try:
          response = self.client.export_workspace(path, WorkspaceFormat.SOURCE)
          return response['content']
      except:
        raise


    def get_default_workspace_path(self):
      response = self.client.client.perform_query(
            'GET', f'/preview/scim/v2/Me')
      user_name = response["userName"]
      default_path = Path('/Users',f'{user_name}/').as_posix()
      return default_path

    def upload_pipeline_artifacts(self, artifacts, workspace_destination, print_event=True):
        # Check that workspace destination exists or not
        uploaded_workspace_paths = []
        try:
          res = self.get_status(workspace_destination)
        except Exception as e: 
            if 'RESOURCE_DOES_NOT_EXIST' in str(e):
                if print_event:
                  event_print(
                    type="cli_status",
                    level='INFO',
                    msg=f"{workspace_destination} path does not currently exist.")
                try:
                    self.mkdirs(workspace_destination)
                except Exception as e:
                    if print_event:
                      event_print(
                        type="cli_status",
                        level='ERROR',
                        msg=f"Attempted to create {workspace_destination} but failed")
                    raise e 
        for artifact in artifacts:
                filename = Path(artifact).name
                full_path = Path(workspace_destination,filename).as_posix()
                if print_event:
                  event_print(
                    type="cli_status",
                    level='INFO',
                    msg=f"Uploading {artifact} to {workspace_destination}.")
                  
                lang_fmt = WorkspaceLanguage.to_language_and_format(artifact)
                try:
                  self.import_workspace(artifact,full_path,language=lang_fmt[0],fmt=lang_fmt[0],is_overwrite=True)
                  uploaded_workspace_paths.append(full_path)
                  if print_event:
                    event_print(
                      type="cli_status",
                      level='INFO',
                      msg=f"Successfully uploaded {artifact} to {workspace_destination}.")

                except Exception as e:
                    raise e
        return uploaded_workspace_paths

            