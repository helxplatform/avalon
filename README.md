<img alt="Avalon" height="100px" src="https://github.com/helxplatform/roger/assets/45075777/227737ae-2833-490e-b390-9d4893835a86" width="100px"/>
# Avalon

## Overview
Avalon is a command-line tool for interacting with LakeFS repositories. It allows users to upload (put) and download (get) files between a local machine and a LakeFS repository.

## Installation
To install Avalon, use the following command:

```bash
pip install .
```

This will install Avalon as a command-line utility.

## Usage
After installation, you can use Avalon by running:

```bash
avalon --help
```

### Commands
Avalon provides two main commands: `put` and `get`.

### General Arguments
- `-c, --cred`: Path to the LakeFS credentials YAML file (required).

### Get Files from LakeFS
To download files from a LakeFS repository:

```bash
avalon get -c /path/to/credentials.yaml \
           -p remote/path/in/repo \
           -l /local/destination/path \
           -r repository_name \
           -b branch_name
```

#### Arguments:
- `-p, --remote-path`: Remote file/directory path in the LakeFS repository.
- `-l, --local-path`: Local directory to store the downloaded files.
- `-r, --repository`: Name of the repository in LakeFS.
- `-b, --branch`: The branch of the repository to download from.

### Put Files to LakeFS
To upload files to a LakeFS repository:

```bash
avalon put -c /path/to/credentials.yaml \
           -p remote/destination/path \
           -l /local/source/path \
           -r repository_name \
           -b branch_name
```

#### Arguments:
- `-p, --remote-path`: Remote destination path in the repository (default: root).
- `-l, --local-path`: Local directory to upload (must be a directory).
- `-r, --repository`: Name of the repository in LakeFS.
- `-b, --branch`: The branch of the repository to upload to.

## Example Usage
### Uploading a Directory
```bash
avalon put -c lakefs_creds.yaml -p data/ -l /home/user/data -r my-repo -b main
```

### Downloading a Directory
```bash
avalon get -c lakefs_creds.yaml -p data/ -l /home/user/data -r my-repo -b main
```

## License
This project is licensed under the MIT License.

