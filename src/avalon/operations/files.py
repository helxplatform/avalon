import os
from typing import List, Set


def get_filepaths(directory: str) -> List[str]:
    """
    This function walks through the directories and return list of paths for files found
    """
    file_paths = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    return file_paths


def get_dest_filepaths(files: List[str], rootpath: str, root_destpath: str) -> List[str]:
    """
    This function transform local filepaths to destination file paths
    For example: /data/test1/dir2/file2.txt to
    dir2/file2.txt

    Note: @rootpath should include ending /. For example: /data/test1/
    Note: @root_destpath should include ending /. For example: lakefsfolder1/

    """
    if rootpath[-1] != '/':
        raise Exception("rootpath is not valid")

    if rootpath[-1] != '/':
        raise Exception("root_destpath is not valid")

    dest_path = []

    for f in files:
        path_wo_root_dir = f[len(rootpath):]
        path_wo_root_dir = root_destpath + path_wo_root_dir
        dest_path.append(path_wo_root_dir)

    return dest_path


def create_dirs(dirs: Set[str]) -> None:
    for d in dirs:
        if not os.path.exists(d):
            os.makedirs(d)
