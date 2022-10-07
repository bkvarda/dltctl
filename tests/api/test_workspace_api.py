from json import JSONDecodeError
from dltctl.api.workspace import WorkspaceApi
import unittest, pytest, base64
from unittest import mock
from pathlib import Path


class TestWorkspaceApi(unittest.TestCase):
    
    def test_get_default_workspace_path(self):
          client_mock = mock.MagicMock()
          client_mock.perform_query.return_value = {"userName":"foo@foo.com"}
          api = WorkspaceApi(client_mock)
          dir = api.get_default_workspace_path()
          self.assertEqual(dir, '/Users/foo@foo.com')

    def test_upload_artifacts_valid_path(self):
          client_mock = mock.MagicMock()
          client_mock.perform_query.side_effect = [{
            "object_type": "DIRECTORY",
            "path": "/Users/me@example.com/MyFolder/",
            "object_id": 123456789012345
          },"",""]
          api = WorkspaceApi(client_mock)
          with mock.patch('dltctl.api.workspace.WorkspaceApi.import_workspace') as import_mock:
            import_mock.side_effect = ["",""]
            a = WorkspaceApi(client_mock).upload_pipeline_artifacts(["foo.py","bar.py"],'/Users/foo@foo.com')
            self.assertEqual(a, ['/Users/foo@foo.com/foo.py', '/Users/foo@foo.com/bar.py'] )

    def test_upload_artifacts_invalid_path(self):
          client_mock = mock.MagicMock()
          client_mock.perform_query.side_effect = [
            'RESOURCE_DOES_NOT_EXIST',Exception("""{'error_code': 'DIRECTORY_PROTECTED', 'message': 'Folder Users is protected'}""")]
          with mock.patch('dltctl.api.workspace.WorkspaceApi.import_workspace') as import_mock:
              import_mock.side_effect = [""]
              api = WorkspaceApi(client_mock)
              self.assertRaises(Exception, WorkspaceApi(client_mock).upload_pipeline_artifacts(["foo.py"],"/Users/foo"))

    def test_get_workspace_file_b64(self):
      path = str(Path(__file__).parent.parent.resolve()) + '/files/source/export.py'
      client = mock.MagicMock()
      api = WorkspaceApi(client)
      content = None
      with open(path, 'rb') as f:
         content = base64.b64encode(f.read())
      
      client.perform_query.return_value = {"content": content}
      workspace_file = api.get_workspace_file_b64("/foo/bar/source.py")
      assert base64.b64decode(workspace_file) == base64.b64decode(content)
      client.perform_query.assert_called_with("GET", "/workspace/export", data={"path": "/foo/bar/source.py", "format": "SOURCE"}, headers=None)




