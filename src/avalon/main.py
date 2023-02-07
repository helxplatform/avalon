import argparse
import datetime
import glob
import os
import logging
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.models.pipeline import Commit,CommitMetaData
from avalon.utils.shared import init_client

logger = logging.Logger('avalon')


def get_files(local_path, remote_path, repo, branch, lake_fs_client: LakeFsWrapper):
    files = lake_fs_client.get_object(repository=repo, branch=branch, remote_path=remote_path, local_path=local_path)
    return files


def put_file(local_path,
             remote_path,
             repo,
             branch,
             task_name,
             pipeline_id,
             task_docker_image,
             task_args,
             lake_fs_client: LakeFsWrapper):
    files = glob.glob(local_path)

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
    lake_fs_client.commit_files(cmt, remote_path=remote_path)



def parse_env():
    return {
        "config_path": os.environ.get("LAKEFS_CONFIG_PATH", "../config/lakectl-lakefs.apps.renci.org.yaml"),
        "lakefs_repo": os.environ.get("LAKEFS_REPO", "test-repo"),
        "lakefs_branch": os.environ.get("LAKEFS_BRANCH", "develop"),
    }

def main(args):
    command = args.sub_command
    env_args = parse_env()
    client = init_client(env_args["config_path"], args.temp_dir)
    local_path = args.local_path
    remote_path = args.remote_path
    if command == "put":
        put_file(
            local_path=local_path,
            remote_path=remote_path,
            repo=env_args['lakefs_repo'],
            branch=env_args['lakefs_branch'],
            lake_fs_client=client,
            task_name=args.task_name,
            pipeline_id=args.pipeline_name,
            task_docker_image=args.task_image,
            task_args=args.task_args,
        )
    else:
        get_files(
            local_path=local_path,
            remote_path=remote_path,
            repo=env_args['lakefs_repo'],
            branch=env_args['lakefs_branch'],
            lake_fs_client=client
        )


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--temp-dir", help="Temporary Dir", default=None)
    sub_parsers = parser.add_subparsers(help="Sub commands", dest="sub_command")
    parser_get_file = sub_parsers.add_parser("get", help="Gets file from Lakefs repo", )
    parser_get_file.add_argument("-r", "--remote-path", help="Remote file / dir path")
    parser_get_file.add_argument("-l", "--local-path", help="Local output dir")

    parser_put_file = sub_parsers.add_parser("put", help="Puts file to Lakefs repo")
    parser_put_file.add_argument("-l", "--local-path", help="Local file to push")
    parser_put_file.add_argument("-r", "--remote-path", help="Remote Path to push")
    parser_put_file.add_argument("-p", "--pipeline-name", help="Pipeline name", default="default=pipeline")
    parser_put_file.add_argument("-t", "--task-name", help="Task name", default="default-task")
    parser_put_file.add_argument("-i", "--task-image", help="Docker image used to run task", default="helxplatform/roger")
    parser_put_file.add_argument("-a", "--task-args", help="Args used to run image", default=[])

    args = parser.parse_args()
    main(args)


if __name__ == '__main__':
    cli()