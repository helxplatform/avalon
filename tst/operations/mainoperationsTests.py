import unittest
import shutil

from lakefs_sdk import Configuration

from avalon.mainoperations import put_files, get_files, get_commit_id_by_input_commit_id, get_last_input_commit_id
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.operations.files import get_filepaths, create_dirs


LOCALTEMPPATH = "temp"
REPO = "testavalon-task3"


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
                  repo=REPO,
                  branch="main",
                  remote_path="result",
                  commit_id="1" * 64,
                  pipeline_id="TestAvalon",
                  task_docker_image="image2",
                  task_args=[],
                  lake_fs_client=lfs,
                  s3storage=False,
                  task_name="Task3")

        result = lfs.get_filelist("main", REPO, remote_path="result")

        self.assertListEqual(result, [
                                      'result/dir1/file2.txt',
                                      'result/dir1/file3.txt',
                                      'result/file1.txt'
                                      ])

    def test_put_files_update_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        put_files(local_path="../data/test2/",
                  repo=REPO,
                  branch="main",
                  remote_path="result",
                  commit_id="1" * 63 + "2",
                  pipeline_id="TestAvalon",
                  task_docker_image="image2",
                  task_args=[],
                  lake_fs_client=lfs,
                  s3storage=False,
                  task_name="Task3")

        put_files(local_path="../data/test3/",
                  repo=REPO,
                  branch="main",
                  remote_path="result",
                  commit_id="1" * 63 + "3",
                  pipeline_id="TestAvalon",
                  task_docker_image="image2",
                  task_args=[],
                  lake_fs_client=lfs,
                  s3storage=False,
                  task_name="Task3")


        result = lfs.get_filelist("main", REPO, remote_path="result")

        self.assertListEqual(result, [
                                      'result/dir1/file2.txt',
                                      'result/dir1/file3.txt',
                                      'result/file1.txt',
                                      'result/file4.txt'
                                      ])


    def test_get_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        get_files(local_path=LOCALTEMPPATH,
                  lake_fs_client=lfs,
                  remote_path="result",
                  repo=REPO,
                  branch="main",
                  changes_only=False)

        result = get_filepaths(LOCALTEMPPATH)
        self.assertListEqual(result, ['temp/result/.empty',
                                      'temp/result/file1.txt',
                                      'temp/result/dir1/file3.txt',
                                      'temp/result/dir1/file2.txt'])

    def test_get_files_changes_only(self):
      #  self.remove_tempresult_folder()
        lfs = LakeFsWrapper(configuration=self.get_config())

        commit_id = get_commit_id_by_input_commit_id(lake_fs_client=lfs,
                                                      remote_path="result",
                                                      repo=REPO,
                                                      branch="main",
                                                      input_commit_id="1" * 63 + "1")

        get_files(local_path=LOCALTEMPPATH,
                  lake_fs_client=lfs,
                  remote_path="result",
                  repo=REPO,
                  branch="main",
                  changes_only=True,
                  changes_from=commit_id)

        result = get_filepaths(LOCALTEMPPATH)
        self.assertListEqual(result, ['temp/.empty',
                                      'temp/result/file4.txt',
                                      'temp/result/dir1/file3.txt',
                                      'temp/result/dir1/file2.txt'])

    def test_get_last_input_commit_id(self):
        #need to run after test_put_files_update_files
        lfs = LakeFsWrapper(configuration=self.get_config())
        input_commit_id = get_last_input_commit_id(branch="main", lake_fs_client=lfs, remote_path="result", repo=REPO)
        self.assertEquals("1" * 63 + "3", input_commit_id)



    def remove_tempresult_folder(self):
        shutil.rmtree(LOCALTEMPPATH)


if __name__ == '__main__':
    unittest.main()
