import hashlib
import logging
import sys
import unittest
from datetime import datetime

import lakefs_sdk
from lakefs_sdk import Configuration

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
        logging.basicConfig(
            level=logging.INFO,
        )
        handler = logging.StreamHandler(sys.stdout)
        logging.getLogger().addHandler(handler)


    def get_config(self) -> Configuration:
        config = lakefs_sdk.Configuration(host='http://localhost:8001/api/v1',
                                             username='AKIAIOSFOLQUICKSTART',
                                             password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

        # config = lakefs_sdk.Configuration(host='http://lakefs2.apps.renci.org:8000/api/v1',
        #                                      username='<ENTERHERE>',
        #                                      password='<ENTERHERE>')

        config.temp_folder_path = LOCALTEMPPATH
        return config

    def test_login(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        cookies = lfs._get_login_cookie()
        self.assertIsNotNone(cookies)

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
        lfs.commit_files(cmt)

    def test_GetFiles(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "20001212")
        lfs.download_files(files, LOCALTEMPPATH, BIGPIPELINEOPERATION, "main")

    def test_FileIntegrityTest_Upload(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        rootpath = "../data/test4/"
        files = get_filepaths(rootpath)
        root_destpath = "integrity/"
        dest_paths = get_dest_filepaths(files, rootpath, root_destpath)

        cmt_meta = CommitMetaData(pipeline_id="pipeline_1", task_name="task_name_1", task_image="docker_image_1")
        cmt = Commit(
            message=f"test4_fileintegritytest",
            repo=BIGPIPELINEOPERATION,
            branch="main",
            metadata=cmt_meta,
            files_added=files,
            committer="avalon",
            commit_date=datetime.now()
        )
        lfs.upload_files(cmt.branch, cmt.repo, files, dest_paths)
        lfs.commit_files(cmt)

        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "intergrity")
        lfs.download_files(files, LOCALTEMPPATH, BIGPIPELINEOPERATION, "main")

    def test_FileIntegrityTest_Download(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "integrity")
        lfs.download_files(files, LOCALTEMPPATH, BIGPIPELINEOPERATION, "main")

        file1_hash = self.calculateHash(LOCALTEMPPATH + "/integrity/" + "file1.txt")
        biggerfile_hash = self.calculateHash(LOCALTEMPPATH + "/integrity/" + "biggerfile1.zip")
        self.assertEqual('7fac80f5d8c2d0baf6fca348bfca7ac2696b580b012481d40e171e2cfcc2d26b86f11b6046903e710e51aba474cf3e0347542d2fa50e4e2fa5194ed520148b81', file1_hash)
        self.assertEqual('b7c3db9e3ff6e3fe7b6d0f01a95d491fcf1d5f947878ab053c36dfd2549146ee40a3ed1ddd122b12410c11361cd3c94e1a959a11ce70888b2e31decee6388f33', biggerfile_hash)

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

    def test_ListFiles(self):
        lfs = LakeFsWrapper(configuration=self.get_config())
        files = lfs.get_filelist("main", BIGPIPELINEOPERATION, "20001212")

    def calculateHash(self, path: str) -> str:
        with open(path, "rb") as f:
            file_hash = hashlib.sha512()
            while chunk := f.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()


if __name__ == '__main__':
    unittest.main()
