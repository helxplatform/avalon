import unittest

import lakefs_client
from lakefs_client import Configuration

from avalon.mainoperations import put_files, get_files
from avalon.operations.LakeFsWrapper import LakeFsWrapper


# These are not fully automated tests.
class MainOperationsTests(unittest.TestCase):

    def get_config(self) -> Configuration:
        config = lakefs_client.Configuration(host='http://localhost:8001/api/v1',
                                             username='AKIAIOSFOLQUICKSTART',
                                             password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

        config.temp_folder_path = "/home/admin2/Documents/tmp"
        return config

    def test_put_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        put_files(local_path="../data/test1/",
                  branch="main",
                  metafilename="latestrun.out",
                  remote_path="OperationRes",
                  commit_id="1" * 64,
                  pipeline_id="Pipeline1",
                  task_docker_image="image2",
                  task_args=[],
                  lake_fs_client=lfs,
                  s3storage=False,
                  task_name="Task3")

    def test_get_files(self):
        lfs = LakeFsWrapper(configuration=self.get_config())

        get_files(local_path="/home/admin2/Documents/avalon/tmp",
                  lake_fs_client=lfs,
                  metafilename="latestrun.out",
                  remote_path="OperationRes",
                  branch="main",
                  changes_only=False,
                  pipeline_id="Pipeline1",
                  task_name="Task3")




if __name__ == '__main__':
    unittest.main()
