import datetime
import logging
import os

from lakefs_sdk.exceptions import NotFoundException, ApiException

from avalon.models.pipeline import CommitMetaData, Commit, Repository
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.operations.files import get_filepaths, get_dest_filepaths

logger = logging.Logger('avalon')

INPUT_COMMIT_ID = 'input_commit_id'

def get_files(local_path: str,
              remote_path: str,
              repo: str,
              branch: str,
              lake_fs_client: LakeFsWrapper,
              changes_only: bool,
              changes_from: str = None):
    all_repos = [r.Id for r in lake_fs_client.list_repo()]

    if not os.path.exists(local_path):
        raise Exception("Error: local path does not exist")
    if not repo in all_repos:
        raise Exception("Error: repository does not exist")


    filelist = []

    if changes_only:
        try:
            # if commit_id is empty nonexistant then we will get all files, not just changed ones
            filelist = lake_fs_client.get_changes(repository=repo, branch=branch, remote_path=remote_path, from_commit_id=changes_from)
        except NotFoundException:
            filelist = lake_fs_client.get_filelist(repository=repo, branch=branch, remote_path=remote_path)
    else:
        filelist = lake_fs_client.get_filelist(repository=repo, branch=branch, remote_path=remote_path)

    lake_fs_client.download_files(remote_files=filelist, local_path=local_path, repository=repo, branch=branch)


def put_files(local_path: str,
              remote_path: str,
              repo: str,
              branch: str,
              task_name: str,
              pipeline_id: str,
              task_docker_image: str,
              task_args,
              lake_fs_client: LakeFsWrapper,
              s3storage: bool,
              commit_id: str):
    _create_repositry_branch_IfNotExists(branch, lake_fs_client, repo, s3storage)

    files = get_filepaths(local_path)
    dest_paths = get_dest_filepaths(files, local_path, remote_path)

    cmt_meta = CommitMetaData(
        pipeline_id=pipeline_id,
        task_name=task_name,
        task_image=task_docker_image,
        input_commit_id=commit_id,
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

    # if uploaded files are the same, it will cause an exception.
    # we can ignore such situation
    try:
        lake_fs_client.commit_files(cmt)
    except ApiException as ex:
        if str.find(ex.body, 'commit: no changes') == -1:
            raise ex


def get_last_input_commit_id(branch, lake_fs_client, remote_path, repo):

    commits = lake_fs_client.list_commits(repository_name=repo, branch_name=branch, path=remote_path.split("/"))
    commit_id = ""

    i = 0
    while commit_id == "" and i < len(commits.results):
        commit_id = commits.results[i].metadata[INPUT_COMMIT_ID]
        i += 1

    if len(commit_id) != 64:
        logger.error("metafile content is not valid")
    return commit_id


def get_commit_id_by_input_commit_id(branch, lake_fs_client, remote_path, repo, input_commit_id):

    commits = lake_fs_client.list_commits(repository_name=repo, branch_name=branch, path=remote_path.split("/"))
    commit_id = ""

    for c in commits.results:
        cur_input_commit_id = c.metadata[INPUT_COMMIT_ID]
        if input_commit_id == cur_input_commit_id:
            commit_id = c.id
            break

    if len(commit_id) != 64:
        logger.error("metafile content is not valid")
    return commit_id


def _create_repositry_branch_IfNotExists(branch, lake_fs_client, repo, s3storage):
    all_repos = [r.Id for r in lake_fs_client.list_repo()]

    if not repo in all_repos:
        r = Repository(repo, f"local://{repo}/")
        if s3storage:
            r = Repository(repo, f"s3://{repo}/")
        lake_fs_client.create_repository(r)
    if branch != "main":
        lake_fs_client.create_branch(branch, repo)


