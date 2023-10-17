from io import StringIO

import os
import shutil

import boto3
from lakefs_client import models
from lakefs_client.client import LakeFSClient
import lakefs_client

from typing import List

from lakefs_client.model.repository_creation import RepositoryCreation

from avalon.models.pipeline import Commit, Repository
from avalon.operations.files import create_dirs


class LakeFsWrapper:
    def __init__(self, configuration: lakefs_client.Configuration):
        os.environ.get('')
        self._config = configuration
        self._client = LakeFSClient(configuration=configuration)
        self._s3_client = boto3.client('s3',
                          endpoint_url=self._config.host,
                          aws_access_key_id=self._config.username,
                          aws_secret_access_key=self._config.password)

    def list_repo(self) -> list[Repository]:
        """
        Lists available repos
        :return: List[Repository].
        """
        repos = self._client.repositories_api.list_repositories().results
        res = []
        for r in repos:
            res.append(Repository(r["id"], r["storage_namespace"]))

        return res

    def create_repository(self, repo: Repository) -> None:
        """
        Creates repository
        :param repo: repository name
        """
        self._client.repositories_api.create_repository(
            repository_creation=RepositoryCreation(repo.Id, repo.StorageNamespace))

    def list_branches(self, repository_name: str):
        """
        List branches in a repo
        :param: repository_name Name of repo
        :return: List of branches
        """
        branches = self._client.branches_api.list_branches(repository=repository_name)
        return branches

    def list_commits(self, repository_name: str, branch_name: str):
        """
        List commits in a branch
        :param repository_name:
        :param branch_name:
        :return:
        """
        commits = self._client.commits_api.log_branch_commits(repository=repository_name, branch=branch_name)
        return commits

    def commit_files(self, commit: Commit):
        """
        Commits files to a branch
        :param commit:
        :return:
        """
        commit_creation = models.CommitCreation(commit.message, metadata=commit.metadata.dict(
            exclude={"args"}) if commit.metadata else {})
        response = self._client.commits_api.commit(
            branch=commit.branch,
            repository=commit.repo,
            commit_creation=commit_creation
        )
        return response

    def upload_files(self, branch: str, repository: str, files: List[str], dest_paths: List[str]):
        """
        This function uploads files
        """
        for i in range(len(files)):
            with open(files[i], 'rb') as stream:
                self._client.objects_api.upload_object(repository=repository,
                                                       branch=branch,
                                                       path=dest_paths[i],
                                                       content=stream)

    def upload_file(self, branch: str, repository: str, content: str, dest_path: str):
        """
        This function uploads str to file
        """
        self._client.objects_api.upload_object(repository=repository,
                                               branch=branch,
                                               path=dest_path,
                                               content=StringIO(content))

    def get_filelist(self, branch: str, repository: str, remote_path: str) -> List[str]:
        """
        Returns lists of files
        :param branch: branch name
        :param repository: repository name
        :param remote_path: path as in Lakefs
        :return:
        """
        objects = self._client.objects_api.list_objects(repository=repository, ref=branch)
        paths = []
        for obj in objects.results:
            paths.append(obj.path)
        matching_files = list(filter(lambda f: f.startswith(remote_path) or remote_path == '*', paths))
        return matching_files

    def get_changes(self, branch: str, repository: str, remote_path: str, commit_id: str) -> List[str]:
        """
        Returns list of remote paths that were changed since specified commit
        :param branch: branch name
        :param repository: repository name
        :param remote_path: path as in Lakefs
        :param commit_id: id of a commit
        :return: list ot remote paths in LakeFs
        """
        files_changed = []
        files_removed = []
        files_added = []

        commits = self.list_commits(repository_name=repository, branch_name=branch).results
        commits_to_proc = []

        for commit in commits:
            if commit.id != commit_id:
                commits_to_proc.append(commit)
            else:
                break

        for commit in commits_to_proc:
            parent_refs = commit.parents
            for parent_ref in parent_refs:
                files = self._client.refs_api.diff_refs(repository=repository, right_ref=commit.id,
                                                        left_ref=parent_ref).results
                for file in files:
                    if file.type == 'added':
                        files_added.append(file.path)
                    elif file.type == 'removed':
                        files_removed.append(file.path)
                    elif file.type == 'changed':
                        files_changed.append(file.path)

        paths = []
        paths.extend(files_changed)
        paths.extend(files_added)

        matching_files = list(filter(lambda f: f.startswith(remote_path), paths))
        return matching_files

    def download_files(self, remote_files: List[str], local_path: str, repository: str, branch: str) -> None:
        """
        Downloads files from LakeFs
        :param remote_files:  list ot remote paths in LakeFs
        :param local_path: local path, destination for files
        :param repository: repository name
        :param branch: branch name
        :return: None
        """
        dirs = set(map(lambda x: os.path.join(local_path, os.path.dirname(x)), remote_files))
        create_dirs(dirs)
        for location in remote_files:
            file_name = os.path.basename(location)
            dir_name = os.path.dirname(location)
            buffered_reader = self._client.objects_api.get_object(repository=repository, ref=branch, path=location)
            buffered_reader.close()
            src_path = buffered_reader.name
            dest_path = os.path.join(local_path, dir_name, file_name)
            shutil.move(src_path, dest_path)

    def create_branch(self, branch_name: str, repository_name: str, source_branch: str = "main"):
        """
        Creates new branch
        """
        branches = self.list_branches(repository_name=repository_name).results
        for b in branches:
            if b['id'] == branch_name:
                return b
        else:
            branch_creation = models.BranchCreation(name=branch_name, source=source_branch)
            commit_id = self._client.branches_api.create_branch(repository=repository_name,
                                                                branch_creation=branch_creation)
            return {"commit_id": commit_id, "id": branch_name}
