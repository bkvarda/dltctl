from dltctl.core.helpers import get_dlt_artifacts
import unittest, tempfile, os
from pathlib import Path
from unittest import mock

EMPTY_ARTIFACTS_RETURN = None
CORRECT_ARTIFACTS_RETURN = ["foo.py","bar.sql","baz.py"]
INCORRECT_ARTIFACTS_RETURN = None
NONE_ARTIFACTS_RETURN = None
EMPTY_ARTIFACTS = " "
CORRECT_ARTIFACTS = 'foo.py,bar.sql,baz.py'
INCORRECT_ARTIFACTS = ["foo.rb","test.json","foo.pyc"]
NONE_ARTIFACTS = None

class TestGetArtifacts(unittest.TestCase):
    def test_invalid_path(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            artifacts = get_dlt_artifacts(Path(tmpdirname,"foobaz1343141").as_posix())
            assert artifacts == None

    def test_empty(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            os.makedirs(Path(tmpdirname,"foobaz1343141"))
            artifacts = get_dlt_artifacts(Path(tmpdirname,"foobaz1343141").as_posix())
            assert artifacts == None

    def test_nested(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            os.makedirs(Path(tmpdirname,"foobaz1343141"))
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, "foobaz1343141", f).as_posix(), 'w')
                new.close()

            artifacts = get_dlt_artifacts(Path(tmpdirname).as_posix())
            assert len(artifacts) == 6
        
    def test_invalid_extensions(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            for f in INCORRECT_ARTIFACTS:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            artifacts = get_dlt_artifacts(tmpdirname)
            self.assertEqual(len(artifacts), 3)
            assert Path(tmpdirname, "foo.py").as_posix() in artifacts
            assert Path(tmpdirname, "foo.pyc").as_posix() not in artifacts

    def test_none(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            artifacts = get_dlt_artifacts(tmpdirname)
            self.assertEqual(artifacts, None)

    def test_correct(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            for f in CORRECT_ARTIFACTS_RETURN:
                new = open(Path(tmpdirname, f).as_posix(), 'w')
                new.close()
            artifacts = get_dlt_artifacts(tmpdirname)
            self.assertEqual(len(artifacts), 3)

    def test_none_but_find_local(self):
        with mock.patch('glob.glob') as glob_mock:
          glob_mock.side_effect = [["foo.py","otherfoo.py","test_otherfoo.py"],["otherfoo.sql", "test_otherfoo.sql"]]
          artifacts = get_dlt_artifacts(None)
          self.assertEqual(len(artifacts), 3)

    def test_none_but_find_local_with_tests(self):
        with mock.patch('glob.glob') as glob_mock:
          glob_mock.side_effect = [["foo.py","otherfoo.py","test_otherfoo.py"],["otherfoo.sql", "test_otherfoo.sql"]]
          artifacts = get_dlt_artifacts(None, ignore_tests=False)
          self.assertEqual(len(artifacts), 5)


