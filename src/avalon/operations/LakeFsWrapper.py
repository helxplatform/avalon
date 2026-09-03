import json
import logging
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lakefs_sdk.client import LakeFSClient
from lakefs_sdk import Configuration, CommitCreation, BranchCreation, exceptions, TagCreation

from typing import List

from lakefs_sdk.exceptions import NotFoundException
from lakefs_sdk.models.repository_creation import RepositoryCreation

from avalon.models.pipeline import Commit, Repository
from avalon.operations.files import create_dirs

# Import tqdm for progress bars
from tqdm import tqdm

# Objects are transferred one at a time by default, which is the wrong shape
# for the many-small-files case: a task staging 123k pickles spends its time
# waiting on round trips, not on bandwidth. These bound the transfer pools.
# Override with AVALON_TRANSFER_WORKERS. Keep at or below the sdk's
# connection_pool_maxsize (cpu_count() * 5) or threads queue on connections.
DEFAULT_TRANSFER_WORKERS = 16


def _transfer_workers(requested, count):
    """Worker count for a transfer of `count` objects."""
    if requested is None:
        requested = int(os.environ.get('AVALON_TRANSFER_WORKERS',
                                       DEFAULT_TRANSFER_WORKERS))
    return max(1, min(int(requested), count))

try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1


logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

class LakeFsWrapper:
    def __init__(self, configuration: Configuration):
        os.environ.get('')
        self._config = configuration
        self._client = LakeFSClient(configuration=configuration)
        self.session = self._create_session()

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

    def delete_repository(self, repo: Repository) -> None:
        """
        deletes repository
        :param repo: repository name
        """
        self._client.repositories_api.delete_repository(repository=repo.Id, storage_namespace=repo.StorageNamespace)

    def list_branches(self, repository_name: str):
        """
        List branches in a repo
        :param repository_name: Name of repo
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

    def _create_session(self):
        """Creates a session with retries for robust error handling."""
        session = requests.Session()
        # session.headers.update({'Transfer-Encoding': 'chunked'})
        retries = Retry(
            total=5,  # Number of retries
            backoff_factor=2,  # Exponential backoff (2s, 4s, 8s, etc.)
            status_forcelist=[500, 502, 503, 504],  # Retry only for these HTTP errors
            allowed_methods=["POST"],  # Apply retry only for POST
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        return session

    def upload_files(self, branch: str, repository: str, files: List[str], dest_paths: List[str],
                     max_workers: int = None):
        """
        This function uploads files with chunking and a progress bar

        Uploads run concurrently for the same reason downloads do: a task
        committing tens of thousands of small outputs is bound by round
        trips. Each upload targets its own path, so they share nothing but
        the login cookie and the retrying session, both of which are read
        only here.

        :param max_workers: concurrent uploads; defaults to
            AVALON_TRANSFER_WORKERS or DEFAULT_TRANSFER_WORKERS
        """
        login_cookie = self._get_login_cookie()
        chunk_size = 8 * 1024 * 1024  # 8 MB per chunk

        def upload_one(file_path, dest_path, show_progress):
            url = f'{self._config.host}/repositories/{urllib.parse.quote_plus(repository)}/branches/{urllib.parse.quote_plus(branch)}/objects?path={urllib.parse.quote_plus(dest_path)}'
            filesize = os.path.getsize(file_path)
            progress = tqdm(total=filesize, unit='B', unit_scale=True,
                            desc=f"Uploading {os.path.basename(file_path)}") if show_progress else None
            try:
                with open(file_path, 'rb') as f:
                    def read_in_chunks(file_object, chunk_size):
                        while True:
                            data = file_object.read(chunk_size)
                            if not data:
                                break
                            if progress is not None:
                                progress.update(len(data))
                            yield data

                    res = self.session.post(url, data=read_in_chunks(f, chunk_size), cookies=login_cookie,
                                            # headers={'Transfer-Encoding': 'chunked'}
                                            )
            finally:
                if progress is not None:
                    progress.close()
            if res.status_code != 201:
                raise Exception(f"Failed to upload file to lakefs: {res.text}")
            logging.debug('Upload file result: %s', res.text)

        workers = _transfer_workers(max_workers, len(files))
        logging.info("Uploading %d file(s) to %s with %d worker(s)",
                     len(files), repository, workers)

        if workers == 1:
            for file_path, dest_path in zip(files, dest_paths):
                upload_one(file_path, dest_path, True)
            return

        with tqdm(total=len(files), unit='file',
                  desc=f"Uploading to {repository}") as progress:
            with ThreadPoolExecutor(max_workers=workers,
                                    thread_name_prefix='lakefs-up') as pool:
                futures = [pool.submit(upload_one, file_path, dest_path, False)
                           for file_path, dest_path in zip(files, dest_paths)]
                for future in futures:
                    future.result()
                    progress.update(1)

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
        paths = [obj.path for obj in results]
        matching_files = list(filter(lambda f: f.startswith(remote_path) or remote_path == '*', paths))
        return matching_files

    def get_changes(self, branch: str, repository: str, remote_path: str, from_commit_id: str, to_commit_id: str = None) -> List[str]:
        """
        Returns list of remote paths that were changed since specified commit
        :param branch: branch name
        :param repository: repository name
        :param remote_path: path as in Lakefs
        :param from_commit_id: id of a commit
        :param to_commit_id: id of a commit (optional)
        :return: list of remote paths in LakeFs
        """
        files_changed = []
        files_removed = []
        files_added = []

        commits = self.list_commits(repository_name=repository, branch_name=branch, path=remote_path.split("/")).results
        commits_to_proc = []

        if to_commit_id is None:
            for commit in commits:
                if commit.id != from_commit_id:
                    commits_to_proc.append(commit)
                else:
                    break

            if len(commits_to_proc) == 0:
                raise NotFoundException()

            files = self._client.refs_api.diff_refs(repository=repository, right_ref=commits_to_proc[0].id, left_ref=from_commit_id).results
        else:
            files = self._client.refs_api.diff_refs(repository=repository, right_ref=to_commit_id, left_ref=from_commit_id).results

        for file in files:
            if file.type == 'added':
                files_added.append(file.path)
            elif file.type == 'removed':
                files_removed.append(file.path)
            elif file.type == 'changed':
                files_changed.append(file.path)

        paths = files_changed + files_added
        matching_files = list(filter(lambda f: f.startswith(remote_path), paths))
        return matching_files

    def download_files(self, remote_files: List[str], local_path: str, repository: str, branch_or_commit_id: str,
                       max_workers: int = None) -> None:
        """
        Downloads files from LakeFs
        :param remote_files:  list of remote paths in LakeFs
        :param local_path: local path, destination for files
        :param repository: repository name
        :param branch_or_commit_id: branch name or commit_id
        :param max_workers: concurrent downloads; defaults to
            AVALON_TRANSFER_WORKERS or DEFAULT_TRANSFER_WORKERS
        :return: None
        """
        dirs = set(map(lambda x: os.path.join(local_path, os.path.dirname(x)), remote_files))
        create_dirs(dirs)

        def target(location):
            file_name = os.path.basename(location)
            dir_name = os.path.dirname(location)
            return os.path.join(local_path, dir_name, file_name)

        workers = _transfer_workers(max_workers, len(remote_files))
        logging.info("Downloading %d file(s) from %s with %d worker(s)",
                     len(remote_files), repository, workers)

        if workers == 1:
            for location in remote_files:
                self.download_file(target(location), branch_or_commit_id, location, repository)
            return

        # Every object writes to its own path and create_dirs already ran, so
        # the downloads share no state. Per-file progress bars are suppressed
        # because concurrent bars interleave into noise; one bar counts files
        # instead. The first failure is re-raised rather than left in the
        # pool, so a partial staging still fails the caller.
        with tqdm(total=len(remote_files), unit='file',
                  desc=f"Downloading from {repository}") as progress:
            with ThreadPoolExecutor(max_workers=workers,
                                    thread_name_prefix='lakefs-dl') as pool:
                futures = [
                    pool.submit(self.download_file, target(location),
                                branch_or_commit_id, location, repository,
                                False)
                    for location in remote_files
                ]
                for future in futures:
                    future.result()
                    progress.update(1)

    def download_file(self, dest_path, branch_or_commit_id, location, repository, show_progress=True):
        """Downloads a single object.

        The four per-file log lines are at debug: at info they dominate the
        log of any task staging many objects -- a 12k-file download emitted
        48,740 lines, exactly four per file, and a 123k-file one wrote
        hundreds of megabytes of them to shared storage.
        """
        logging.debug("Downloading file: {0}, {1}, {2}".format(branch_or_commit_id, location, repository))
        file_info = self._client.objects_api.stat_object(repository=repository, ref=branch_or_commit_id, path=location)
        file_size = file_info.size_bytes
        logging.debug("File size: {0}".format(file_size))
        chunk = 32 * 1024 * 1024  # 32 MB per chunk
        current_pos = 0

        progress = tqdm(total=file_size, unit='B', unit_scale=True,
                        desc=f"Downloading {os.path.basename(dest_path)}") if show_progress else None
        try:
            with open(dest_path, 'wb') as f:
                while current_pos < file_size:
                    from_bytes = current_pos
                    to_bytes = min(current_pos + chunk, file_size - 1)
                    logging.debug("Downloading bytes: {0} - {1}".format(from_bytes, to_bytes))
                    obj_bytes = self._client.objects_api.get_object(repository=repository,
                                                                      ref=branch_or_commit_id,
                                                                      path=location,
                                                                      range="bytes={0}-{1}".format(from_bytes, to_bytes))
                    f.write(obj_bytes)
                    if progress is not None:
                        progress.update(len(obj_bytes))
                    current_pos = to_bytes + 1
        finally:
            if progress is not None:
                progress.close()

        logging.debug("Downloading completed: {0}".format(current_pos - 1))

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

    def create_tag(self, repository_name: str, commit_id: str, tag_name: str):
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
