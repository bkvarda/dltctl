from dltctl.core.helpers import get_project_settings
from dltctl.types.pipelines import PipelineSettings
from dltctl.types.project import ProjectConfig
import unittest, os, pytest
from unittest import mock
from pathlib import Path
from yaml import YAMLError
from json import JSONDecodeError

DEFAULT_SETTINGS = ProjectConfig()

class TestCliPipelineSettings(unittest.TestCase):
    def test_no_settings_file(self):
        with mock.patch('os.path.exists') as os_mock:
          os_mock.return_value = False
          with pytest.raises(Exception):
            settings = get_project_settings(None)
 
    def test_local_settings(self):
        with mock.patch('os.getcwd') as cwd_mock:
            with mock.patch('os.path.exists') as os_mock:
                path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
                cwd_mock.return_value = path
                os_mock.return_value = True
                settings = get_project_settings(None)
                self.assertEqual(settings.pipeline_settings.id, None)
                self.assertEqual(settings.pipeline_settings.name, 'mycoolname')


    def test_specified_settings(self):
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
        settings = get_project_settings(path)
        self.assertEqual(settings.pipeline_settings.id, None)
        self.assertEqual(settings.pipeline_settings.name, 'mycoolname')

    def test_invalid_local_settings(self):
        with mock.patch('os.getcwd') as cwd_mock:
            with mock.patch('os.path.exists') as os_mock:
                path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
                cwd_mock.return_value = path
                os_mock.return_value = True
                self.assertRaises(YAMLError, lambda: get_project_settings(None))


    def test_invalid_specified_settings(self):
        path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
        self.assertRaises(YAMLError, lambda: get_project_settings(path))