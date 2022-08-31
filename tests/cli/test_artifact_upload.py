from dltctl.cli import _get_dlt_artifacts
import unittest
from unittest import mock

EMPTY_ARTIFACTS_RETURN = None
CORRECT_ARTIFACTS_RETURN = ["foo.py","bar.sql","baz.py"]
INCORRECT_ARTIFACTS_RETURN = None
NONE_ARTIFACTS_RETURN = None
EMPTY_ARTIFACTS = ""
CORRECT_ARTIFACTS = 'foo.py,bar.sql,baz.py'
INCORRECT_ARTIFACTS = 'foo.rb,test.json,foo.pyc'
NONE_ARTIFACTS = None

class TestGetArtifacts(unittest.TestCase):
    def test_empty(self):
        artifacts = _get_dlt_artifacts(EMPTY_ARTIFACTS)
        self.assertEqual(EMPTY_ARTIFACTS_RETURN, artifacts)
        
    def test_invalid_extensions(self):
        artifacts = _get_dlt_artifacts(INCORRECT_ARTIFACTS)
        self.assertEqual(INCORRECT_ARTIFACTS_RETURN, artifacts)

    def test_none(self):
        with mock.patch('os.listdir') as os_mock:
          os_mock.return_value = []
          artifacts = _get_dlt_artifacts(None)
          self.assertEqual(artifacts, None)

    def test_correct(self):
        artifacts = _get_dlt_artifacts(CORRECT_ARTIFACTS)
        self.assertEqual(len(artifacts), 3)

    def test_none_but_find_local(self):
        with mock.patch('os.listdir') as os_mock:
          os_mock.return_value = ["foo.py","foo.rb","foo.pyc","otherfoo.py","otherfoo.sql"]
          artifacts = _get_dlt_artifacts(None)
          self.assertEqual(len(artifacts), 3)


