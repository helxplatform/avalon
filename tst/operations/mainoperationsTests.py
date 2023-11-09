import unittest

from lakefs_sdk import Configuration

from avalon.mainoperations import put_files, get_files, get_repo_name
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.operations.files import get_filepaths, create_dirs

LOCALTEMPPATH = "temp"

# These are not fully automated tests.
class MainOperationsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_dirs([LOCALTEMPPATH])

    def get_config(self) -> Configuration:
        config = Configuration(host='http://localhost:8000/api/v1',
                                             username='AKIAIOSFOLQUICKSTART',
                                             password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

        config.temp_folder_path = LOCALTEMPPATH
        return config

    def test_put_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        put_files(local_path="../data/test1/",
                  branch="main",
                  metafilename="latestrun.out",
                  remote_path="result",
                  commit_id="1" * 64,
                  pipeline_id="TestAvalon",
                  task_docker_image="image2",
                  task_args=[],
                  lake_fs_client=lfs,
                  s3storage=False,
                  task_name="Task3")

        result = lfs.get_filelist("main", get_repo_name("TestAvalon","Task3"), remote_path="result")

        self.assertListEqual(result, [
                                      'result/dir1/file2.txt',
                                      'result/dir1/file3.txt',
                                      'result/file1.txt',
                                      'result/latestrun.out'
                                      ])


    def test_get_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        get_files(local_path=LOCALTEMPPATH,
                  lake_fs_client=lfs,
                  metafilename="latestrun.out",
                  remote_path="result",
                  branch="main",
                  changes_only=False,
                  pipeline_id="TestAvalon",
                  task_name="Task3")

        result = get_filepaths(LOCALTEMPPATH)
        self.assertListEqual(result, ['temp/.empty',
                                      'temp/result/latestrun.out',
                                      'temp/result/file1.txt',
                                      'temp/result/dir1/file3.txt',
                                      'temp/result/dir1/file2.txt'])




if __name__ == '__main__':
    unittest.main()
