import json
import logging
import os
import urllib.parse
import requests

from lakefs_sdk.client import LakeFSClient
from lakefs_sdk import Configuration, CommitCreation, BranchCreation, exceptions, TagCreation

from typing import List

from lakefs_sdk.exceptions import NotFoundException
from lakefs_sdk.models.repository_creation import RepositoryCreation

from avalon.models.pipeline import Commit, Repository
from avalon.operations.files import create_dirs


class LakeFsWrapper:
    def __init__(self, configuration: Configuration):
        os.environ.get('')
        self._config = configuration
        self._client = LakeFSClient(configuration=configuration)

    def list_repo(self) -> list[Repository]:
        """
        Lists available repos
        :return: List[Repository].
        """
        repos = self._client.repositories_api.list_repositories().results
        res = []
        for r in repos:
            res.append(Repository(r.id, r.storage_namespace))

        return res

    def create_repository(self, repo: Repository) -> None:
        """
        Creates repository
        :param repo: repository name
        """
        self._client.repositories_api.create_repository(
            repository_creation=RepositoryCreation(name=repo.Id, storage_namespace=repo.StorageNamespace))

    def list_branches(self, repository_name: str):
        """
        List branches in a repo
        :param: repository_name Name of repo
        :return: List of branches
        """
        branches = self._client.branches_api.list_branches(repository=repository_name)
        return branches

    def list_commits(self, repository_name: str, branch_name: str, path: [str]):
        """
        List commits in a branch
        :param repository_name:
        :param branch_name:
        :param path: path
        :return:
        """
        commits = self._client.refs_api.log_commits(repository=repository_name, ref=branch_name, prefixes=path)
        return commits

    def commit_files(self, commit: Commit):
        """
        Commits files to a branch
        :param commit:
        :return:
        """
        commit_creation = CommitCreation(message=commit.message, metadata=commit.metadata.dict(exclude={"args"}))
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
        login_cookie = self._get_login_cookie()
        for i in range(len(files)):
            with open(files[i], 'rb') as f:
                url = f'{self._config.host}/repositories/{urllib.parse.quote_plus(repository)}/branches/{urllib.parse.quote_plus(branch)}/objects?path={urllib.parse.quote_plus(dest_paths[i])}'
                res = requests.post(url, data=f, cookies=login_cookie)
                if res.status_code != 201:
                    raise Exception(f"Failed to upload file to lakefs: {res.text}")
                logging.info(f'Upload file result: {res.text}')

    def _get_login_cookie(self):
        login_url = f"{self._config.host}/auth/login"
        auth_resp = requests.post(login_url, json={"access_key_id": self._config.username,
                                                   "secret_access_key": self._config.password})
        if auth_resp.status_code != 200:
            raise Exception(f"Authentication to lakefs failed: {auth_resp.status_code}")

        return auth_resp.cookies


    def upload_file(self, branch: str, repository: str, content: str, dest_path: str):
        """
        This function uploads str to file
        """
        self._client.objects_api.upload_object(repository=repository,
                                               branch=branch,
                                               path=dest_path,
                                               content=bytes(content, 'utf-8'))

    def get_filelist(self, branch: str, repository: str, remote_path: str) -> List[str]:
        """
        Returns lists of files
        :param branch: branch name
        :param repository: repository name
        :param remote_path: path as in Lakefs
        :return:
        """
        results = []
        has_results = True
        current = 0
        next_page = None
        while has_results:
            if not next_page:
                objects = self._client.objects_api.list_objects(repository=repository,
                                                                ref=branch,
                                                                amount=1000)
            else:
                objects = self._client.objects_api.list_objects(repository=repository,
                                                                ref=branch,
                                                                amount=1000,
                                                                after=next_page)
            results += objects.results
            has_results = objects.pagination.has_more
            next_page = objects.pagination.next_offset
        paths = []
        for obj in results:
            paths.append(obj.path)
        matching_files = list(filter(lambda f: f.startswith(remote_path) or remote_path == '*', paths))
        return matching_files

    def get_changes(self, branch: str, repository: str, remote_path: str, from_commit_id: str) -> List[str]:
        """
        Returns list of remote paths that were changed since specified commit
        :param branch: branch name
        :param repository: repository name
        :param remote_path: path as in Lakefs
        :param from_commit_id: id of a commit
        :return: list ot remote paths in LakeFs
        """
        files_changed = []
        files_removed = []
        files_added = []

        commits = self.list_commits(repository_name=repository, branch_name=branch, path=remote_path.split("/")).results
        commits_to_proc = []

        for commit in commits:
            if commit.id != from_commit_id:
                commits_to_proc.append(commit)
            else:
                break

        if len(commits_to_proc) == 0:
            raise NotFoundException()

        # for commit in commits_to_proc:
        files = self._client.refs_api.diff_refs(repository=repository, right_ref=commits_to_proc[0].id, left_ref=from_commit_id).results
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
            dest_path = os.path.join(local_path, dir_name, file_name)

            self.download_file(dest_path, branch, location, repository)

    def download_file(self, dest_path, branch, location, repository):
        logging.info("Downloading file: {0}, {1}, {2}".format(branch, location, repository))
        file_info = self._client.objects_api.stat_object(repository=repository, ref=branch, path=location)
        file_size = file_info.size_bytes
        logging.info("File size: {0}".format(file_size))
        chunk = 32 * 1024 * 1024
        current_pos = 0

        with open(dest_path, 'wb') as f:
            while current_pos < file_size:
                from_bytes = current_pos
                to_bytes = min(current_pos + chunk, file_size - 1)
                logging.info("Downloading bytes: {0} - {1}".format(from_bytes, to_bytes))
                obj_bytes = self._client.objects_api.get_object(repository=repository,
                                                                ref=branch,
                                                                path=location,
                                                                range="bytes={0}-{1}".format(from_bytes, to_bytes))
                f.write(obj_bytes)
                current_pos = to_bytes + 1

        logging.info("Downloading completed: {0}".format(current_pos - 1))


    def create_branch(self, branch_name: str, repository_name: str, source_branch: str = "main"):
        """
        Creates new branch
        """
        try:
            return self._client.branches_api.get_branch(repository=repository_name, branch=branch_name)
        except exceptions.NotFoundException as Ex:
            branch_creation = BranchCreation(name=branch_name, source=source_branch)
            commit_id = self._client.branches_api.create_branch(repository=repository_name,
                                                                branch_creation=branch_creation)
            return {"commit_id": commit_id, "id": branch_name}


    def create_tag(self,  repository_name: str, commit_id: str, tag_name: str):
        """
        Creates a new tag
        """
        logging.info("Creating new tag: {0}".format(tag_name))
        self._client.tags_api.create_tag(repository=repository_name, tag_creation=TagCreation(id=tag_name, ref=commit_id))
        logging.info("Creating of new tag completed")


    def get_tags(self, repository_name: str) -> dict[str, str]:
        """
        Returns list of tags for a given repository.
        """
        resp = self._client.tags_api.list_tags(repository=repository_name)
        res = dict[str, str]()
        for tag in resp.results:
            res[tag.id] = tag.commit_id
        return res