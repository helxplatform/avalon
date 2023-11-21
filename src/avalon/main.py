import argparse
import os

from avalon.mainoperations import get_files, put_files
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.config import Config


def parse_env():
    return {
        "config_path": os.environ.get("LAKEFS_CONFIG_PATH", "../config/lakectl-lakefs.apps.renci.org.yaml"),
        "lakefs_branch": os.environ.get("LAKEFS_BRANCH", "develop")
    }


def main(args):
    command = args.sub_command
    env_args = parse_env()
    config = Config(
        lakefs_conf_path=env_args["config_path"],
        temp_dir=args.temp_dir)
    client = LakeFsWrapper(configuration=config.get_config())

    if command == "put":
        put_files(
            local_path=args.local_path,
            remote_path=args.remote_path,
            branch=env_args['lakefs_branch'],
            lake_fs_client=client,
            task_name=args.task_name,
            pipeline_id=args.pipeline_name,
            task_docker_image=args.task_image,
            task_args=args.task_args,
            commit_id=args.commit_id
        )
    else:
        get_files(
            local_path=args.local_path,
            remote_path=args.remote_path,
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
    parser_get_file.add_argument("-c", "--changed-files-only", help="To get changed files only", default=False)

    parser_put_file = sub_parsers.add_parser("put", help="Puts file to Lakefs repo")
    parser_put_file.add_argument("-l", "--local-path", help="Local dir to push")
    parser_put_file.add_argument("-r", "--remote-path", help="Remote Path to push")
    parser_put_file.add_argument("-p", "--pipeline-name", help="Pipeline name", default="default=pipeline")
    parser_put_file.add_argument("-t", "--task-name", help="Task name", default="default-task")
    parser_put_file.add_argument("-i", "--task-image", help="Docker image used to run task", default="helxplatform/roger")
    parser_put_file.add_argument("-cid", "--commit-id", help="Commit id of input data", default=None)
    parser_put_file.add_argument("-a", "--task-args", help="Args used to run image", default=[])

    args = parser.parse_args()
    main(args)


if __name__ == '__main__':
    cli()
