import os.path
import unittest
from datetime import datetime

import lakefs_client
from lakefs_client import Configuration

from avalon.models.pipeline import Repository, CommitMetaData, Commit
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.operations.files import get_filepaths, get_dest_filepaths, create_dirs

BIGPIPELINEOPERATION = "bigpipelineoperation3"
BIGPIPELINEOPERATION_NS = "local://bigpipelineoperation3/"
LOCALTEMPPATH = "temp"


# These are not fully automated tests.
# It is expected that you have local running LakeFS container.
class LakeFsWrapperTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_dirs(["temp"])

    def get_config(self) -> Configuration:
        config = lakefs_client.Configuration(host='http://localhost:8001/api/v1',
                                             username='AKIAIOSFOLQUICKSTART',
                                             password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

        # config = lakefs_client.Configuration(host='http://lakefs2.apps.renci.org:8000/api/v1',
        #                                      username='<ENTERHERE>',
        #                                      password='<ENTERHERE>')

        config.temp_folder_path = LOCALTEMPPATH
        return config

    def test_ListRepositories(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        repos = lfs.list_repo()
        self.assertTrue(len(repos) > 0)

    def test_CreateRepository(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        lfs.create_repository(Repository(BIGPIPELINEOPERATION, BIGPIPELINEOPERATION_NS))

    def test_Upload(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        rootpath = "../data/test1/"
        files = get_filepaths(rootpath)
        root_destpath = "20001212/"
        dest_paths = get_dest_filepaths(files, rootpath, root_destpath)

        cmt_meta = CommitMetaData(
            pipeline_id="pipeline_1",
            task_name="task_name_1",
            task_image="docker_image_1"
        )
        cmt = Commit(
            message=f"commit pushed by task: task_name_1, pipeline: pipeline_1",
            repo=BIGPIPELINEOPERATION,
            branch="main",
            metadata=cmt_meta,
            files_added=files,
            committer="avalon",
            commit_date=datetime.now()
        )
        lfs.upload_files(cmt.branch, cmt.repo, files, dest_paths)
        lfs.upload_file(cmt.branch, cmt.repo, "TEST111", os.path.join(root_destpath, "lastrun.roger"))

        lfs.commit_files(cmt)

    def test_GetFiles(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "20001212")
        lfs.download_files(files, LOCALTEMPPATH, BIGPIPELINEOPERATION, "main")

    #expected to fail
    def test_GetNonExistingFiles(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "20001212")
        lfs.download_files(['filenotexist.txt'], LOCALTEMPPATH, BIGPIPELINEOPERATION, "main")

    def test_GetChanges(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_changes("main", BIGPIPELINEOPERATION, "20001212",
                                "e5759e1ea49a81bdfcd8acfef186dfb04458e5baaca03c38fca58d79f662d2ac")
        print(files)


if __name__ == '__main__':
    unittest.main()
