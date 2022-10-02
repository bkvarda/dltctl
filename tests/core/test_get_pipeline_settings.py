from dltctl.core.helpers import get_pipeline_settings
from dltctl.types.pipelines import PipelineSettings
import unittest, os
from unittest import mock
from pathlib import Path
from json import JSONDecodeError

DEFAULT_SETTINGS = PipelineSettings()

class TestCliPipelineSettings(unittest.TestCase):
    def test_default_settings(self):
        with mock.patch('os.path.exists') as os_mock:
          os_mock.return_value = False
          settings = get_pipeline_settings(None)
          self.assertEqual(settings.name, DEFAULT_SETTINGS.name)
          self.assertEqual(settings.edition, DEFAULT_SETTINGS.edition)
          self.assertEqual(settings.photon, DEFAULT_SETTINGS.photon)
        
    def test_local_settings(self):
        with mock.patch('os.getcwd') as cwd_mock:
            with mock.patch('os.path.exists') as os_mock:
                path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
                cwd_mock.return_value = path
                os_mock.return_value = True
                settings = get_pipeline_settings(None)
                self.assertEqual(settings.id, 'some-id')
                self.assertEqual(settings.name, 'mycoolname')


    def test_specified_settings(self):
        path = str(Path(__file__).parent.parent.resolve()) + '/files/valid/'
        settings = get_pipeline_settings(pipeline_config=path)
        self.assertEqual(settings.id, 'some-id')
        self.assertEqual(settings.name, 'mycoolname')

    def test_invalid_local_settings(self):
        with mock.patch('os.getcwd') as cwd_mock:
            with mock.patch('os.path.exists') as os_mock:
                path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
                cwd_mock.return_value = path
                os_mock.return_value = True
                self.assertRaises(JSONDecodeError, lambda: get_pipeline_settings(None))


    def test_invalid_specified_settings(self):
        path = str(Path(__file__).parent.parent.resolve()) + '/files/invalid/'
        self.assertRaises(JSONDecodeError, lambda: get_pipeline_settings(pipeline_config=path))