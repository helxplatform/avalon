import datetime
import logging
import os

from lakefs_sdk.client import LakeFSClient

from avalon.models.pipeline import CommitMetaData, Commit, Repository
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.operations.files import get_filepaths, get_dest_filepaths

logger = logging.Logger('avalon')


def get_files(local_path: str,
              remote_path: str,
              branch: str,
              task_name: str,
              pipeline_id: str,
              lake_fs_client: LakeFsWrapper,
              metafilename: str,
              changes_only: bool):
    repo = get_repo_name(pipeline_id, task_name)
    all_repos = [r.Id for r in lake_fs_client.list_repo()]

    if not os.path.exists(local_path):
        raise Exception("Error: local path does not exist")
    if not repo in all_repos:
        raise Exception("Error: repository does not exist")


    filelist = []

    if changes_only:
        try:
            commit_id = _get_last_commit_id(branch, lake_fs_client, local_path, metafilename, remote_path, repo)
            # if commit_id is empty nonexistant then we will get all files, not just changed ones
            filelist = lake_fs_client.get_changes(repository=repo, branch=branch, remote_path=remote_path, commit_id=commit_id)
        except LakeFSClient.exceptions.NotFoundException:
            filelist = lake_fs_client.get_filelist(repository=repo, branch=branch, remote_path=remote_path)
    else:
        filelist = lake_fs_client.get_filelist(repository=repo, branch=branch, remote_path=remote_path)

    lake_fs_client.download_files(remote_files=filelist, local_path=local_path, repository=repo, branch=branch)


def put_files(local_path: str,
              remote_path: str,
              branch: str,
              task_name: str,
              pipeline_id: str,
              task_docker_image: str,
              task_args,
              lake_fs_client: LakeFsWrapper,
              s3storage: bool,
              metafilename: str,
              commit_id: str):
    repo = get_repo_name(pipeline_id, task_name)

    _create_repositry_branch_IfNotExists(branch, lake_fs_client, repo, s3storage)

    files = get_filepaths(local_path)
    dest_paths = get_dest_filepaths(files, local_path, remote_path)
    metafilename_path = os.path.join(remote_path, metafilename)

    cmt_meta = CommitMetaData(
        pipeline_id=pipeline_id,
        task_name=task_name,
        task_image=task_docker_image,
        args=task_args
    )
    cmt = Commit(
        message=f"commit pushed by task: {task_name}, pipeline: {pipeline_id}",
        repo=repo,
        branch=branch,
        metadata=cmt_meta,
        files_added=files,
        committer="avalon",
        commit_date=datetime.datetime.now()
    )

    lake_fs_client.upload_files(cmt.branch, cmt.repo, files, dest_paths)
    lake_fs_client.upload_file(cmt.branch, cmt.repo, commit_id, metafilename_path)

    # if uploaded files are the same, it will cause an exception.
    # we can ignore such situation
    try:
        lake_fs_client.commit_files(cmt)
    except LakeFSClient.exceptions.ApiException as ex:
        if str.find(ex.body, 'commit: no changes') == -1:
            raise ex


def get_repo_name(pipeline_id: str, task_name: str):
    repo = str.lower(f"{pipeline_id}-{task_name}")
    return repo


def _create_repositry_branch_IfNotExists(branch, lake_fs_client, repo, s3storage):
    all_repos = [r.Id for r in lake_fs_client.list_repo()]

    if not repo in all_repos:
        r = Repository(repo, f"local://{repo}/")
        if s3storage:
            r = Repository(repo, f"s3://{repo}/")
        lake_fs_client.create_repository(r)
    if branch != "main":
        lake_fs_client.create_branch(branch, repo)


def _get_last_commit_id(branch, lake_fs_client, local_path, metafilename, remote_path, repo):
    metafilepath_remote = os.path.join(remote_path, metafilename)
    metafilepath_local = os.path.join(remote_path, metafilename)
    lake_fs_client.download_files([metafilepath_remote], local_path, repository=repo, branch=branch)
    commit_id = ""
    with open(metafilepath_local) as file:
        commit_id = file.read()
    if len(commit_id) != 64:
        logger.error("metafile content is not valid")
    return commit_id
