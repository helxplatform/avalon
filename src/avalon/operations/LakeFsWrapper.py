###
# LakeFs wrapper class
###
import boto3
import os
import ntpath
import fnmatch
import shutil
from functools import reduce

from lakefs_client import models
from lakefs_client.client import LakeFSClient
import lakefs_client

from typing import List
from avalon.models.pipeline import Commit, CommitMetaData, Task, ExecutionState, PipelineInstance


class LakeFsWrapper:
    def __init__(self, configuration: lakefs_client.Configuration):
        os.environ.get('')
        self._config = configuration
        self._client = LakeFSClient(configuration=configuration)
        self._s3_client = boto3.client('s3',
                          endpoint_url=self._config.host,
                          aws_access_key_id=self._config.username,
                          aws_secret_access_key=self._config.password)

    def list_repo(self):
        """
        Lists available repos
        :return: List[Repository].
        """
        repos: models.RepositoryList = self._client.repositories.list_repositories()
        return repos

    def list_branches(self, repository_name: str):
        """
        List branches in a repo
        :param: repository_name Name of repo
        :return: List of branches
        """
        branches = self._client.branches.list_branches(repository=repository_name)
        return branches

    def list_commits(self, repository_name: str, branch_name: str):
        """
        List commits in a branch
        :param repository_name:
        :param branch_name:
        :return:
        """
        commits = self._client.commits.log_branch_commits(repository=repository_name,
                                                          branch=branch_name)
        return commits

    def get_pipeline_commits(self, repository_name: str, branch_name: str):
        """
        :param repository_name:
        :param branch_name:
        :return:
        """
        commits = self.list_commits(repository_name=repository_name, branch_name=branch_name).results
        commits_by_pipeline_id = {}
        for commit in commits:
            # first check if metadata is present
            meta_data = commit.metadata
            if not meta_data:
                continue
            commits_by_pipeline_id[meta_data['pipeline_id']] = commits_by_pipeline_id.get(meta_data['pipeline_id'], [])
            commits_by_pipeline_id[meta_data['pipeline_id']].append(commit)
        for pipeline_id , pipeline_info in commits_by_pipeline_id.items():
            by_task = {}
            for commit in pipeline_info:
                # last commit per task id is the most recent one
                commit_metadata = CommitMetaData(**commit.metadata)
                # if by_task.get(commit_metadata.task_name):
                #     # task ID already exists so not the most recent commit for that pipeline
                #     # for that task
                #     continue
                # else:
                # lets find the files and make a task
                files_changed = []
                files_removed = []
                files_added = []
                parent_refs = commit.parents
                for parent_ref in parent_refs:
                    files = self._client.refs.diff_refs(
                        repository=repository_name,
                        right_ref=commit.id,
                        left_ref=parent_ref
                    ).results
                    for file in files:
                        if file.type == 'added':
                            files_added.append(file.path)
                        elif file.type == 'removed':
                            files_removed.append(file.path)
                        elif file.type == 'changed':
                            files_changed.append(file.path)
                pipeline_commit = Commit(
                    message=commit.message,
                    repo=repository_name,
                    branch=branch_name,
                    id=commit.id,
                    metadata=commit_metadata,
                    files_changed=files_changed,
                    files_removed=files_removed,
                    files_added=files_added,
                    commit_date=commit.creation_date,
                    committer=commit.committer
                )
                task = Task(
                    task_name=commit_metadata.task_name,
                    task_image=commit_metadata.task_image,
                    commit=pipeline_commit,
                    dependencies=[],
                    parameters=[],
                    # @TODO this should be coming from the execution
                    status=ExecutionState.success
                )
                by_task[commit_metadata.task_name] = by_task.get(commit_metadata.task_name, [])
                by_task[commit_metadata.task_name].append(task)
            tasks = reduce(lambda a, b: a + b, [by_task[task_id] for task_id in by_task] ,[])
            pipeline_instance = PipelineInstance(
                pipeline_definition_id=pipeline_id,
                id="0",
                tasks=tasks
            )
            return pipeline_instance

    def commit_files(self, commit: Commit, remote_path=None):
        """
        Uploads and commits files to a branch
        :param commit:
        :return:
        """
        self._upload_files(
            commit.branch,
            commit.repo,
            commit.files_added,
            remote_path=remote_path
        )
        commit_creation = models.CommitCreation(commit.message, metadata=commit.metadata.dict(exclude={"args"}) if commit.metadata else {})
        response = self._client.commits.commit(
            branch=commit.branch,
            repository=commit.repo,
            commit_creation=commit_creation
        )
        return response

    def _upload_files(self, branch: str, repository: str, files: List[str], remote_path=None):
        """

        :param branch:
        :param repository:
        :param files:
        :param remote_path:
        :return:
        """
        for f in files:
            base_file_name =  ntpath.basename(f)
            if remote_path:
                dest_file_path = ntpath.join(remote_path, base_file_name)
            else:
                dest_file_path = f
            with open(f, 'rb') as stream:
                self._client.objects.upload_object(repository=repository,
                                                   branch=branch,
                                                   path=dest_file_path,
                                                   content=stream)

    def get_as_s3_object(self, repository, branch, remote_file_path, local_file_path):
        list_resp = self._s3_client.list_objects_v2(Bucket=repository, Prefix=branch)
        for obj in list_resp['Contents']:
            print(obj['Key'])


    def get_object(self, branch: str, repository: str, remote_path: str,local_path: str):
        objects = self._client.objects.list_objects(repository=repository, ref=branch)
        paths = []
        for obj in objects.results:
            paths.append(obj.path)
        # get matching files
        matching_files = fnmatch.filter(paths, remote_path)
        # setup local dirs
        dirs = set(map(lambda x: ntpath.join(local_path, ntpath.dirname(x)), matching_files ))
        for dir in dirs:
            if not os.path.exists(dir):
                os.makedirs(dir)
        for location in matching_files:
            file_name = ntpath.basename(location)
            dir_name = ntpath.dirname(location)
            buffered_reader = self._client.objects.get_object(repository=repository, ref=branch, path=location)
            buffered_reader.close()
            shutil.move(ntpath.join(self._config.temp_folder_path, file_name), ntpath.join(
                local_path, dir_name, file_name
            ))
        return None

    def create_branch(self, branch_name: str, repository_name: str, source_branch: str="main"):
        branches = self.list_branches(repository_name=repository_name).results
        for b in branches:
            if b['id'] == branch_name:
                return b
        else:
            branch_creation = models.BranchCreation(name=branch_name, source=source_branch)
            commit_id = self._client.branches.create_branch(repository=repository_name,branch_creation=branch_creation)
            return {"commit_id": commit_id, "id": branch_name}