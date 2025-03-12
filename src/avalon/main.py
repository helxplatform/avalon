import argparse

from avalon.mainoperations import get_files, put_files
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from avalon.config import Config



def main(args):
    command = args.sub_command

    config = Config(
        lakefs_conf_path=args.cred
    )
    client = LakeFsWrapper(configuration=config.get_config())

    if command == "put":
        put_files(
            local_path=args.local_path,
            remote_path=args.remote_path,
            s3storage=False,
            branch=args.branch,
            source_branch_name="",
            lake_fs_client=client,
            task_name="",
            pipeline_id="",
            task_docker_image="",
            task_args="",
            commit_id="",
            repo=args.repository
        )
    else:
        get_files(
            local_path=args.local_path,
            remote_path=args.remote_path,
            branch=args.branch,
            lake_fs_client=client,
            changes_only=False,
            repo=args.repository,

        )


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cred", help="Lakefs credentials yaml file path", required=True)

    sub_parsers = parser.add_subparsers(help="Sub commands", dest="sub_command")

    parser_get_file = sub_parsers.add_parser("get", help="Gets file from Lakefs repo", )
    parser_get_file.add_argument("-p", "--remote-path", help="Remote file / dir path to download")
    parser_get_file.add_argument("-l", "--local-path", help="Local output dir")
    parser_get_file.add_argument("-r", "--repository", help="repository to get data from")
    parser_get_file.add_argument("-b", "--branch", help="repository branch to get data from")

    parser_put_file = sub_parsers.add_parser("put", help="Puts file to Lakefs repo")
    parser_put_file.add_argument("-p", "--remote-path", default="", help="Remote destination path default "
                                                                         "is empty string refering to root of the repo, "
                                                                         "Please input relative paths "
                                                                         "eg `mypath/` ")
    parser_put_file.add_argument("-l", "--local-path", help="Local path to push (note this is a dir)")
    parser_put_file.add_argument("-r", "--repository", help="repository to push to")
    parser_put_file.add_argument("-b", "--branch", help="repository branch to push to")
    args = parser.parse_args()
    main(args)


if __name__ == '__main__':
    cli()
