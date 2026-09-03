"""Concurrent download/upload must match the serial result exactly.

These exercise LakeFsWrapper.download_files / upload_files against a fake
objects_api, so they run with no lakefs server. What they pin down is the
part that concurrency can break: that every object is fetched exactly once,
that bytes land in the right file even when responses arrive out of order,
and that one failure still fails the caller instead of leaving a silently
partial staging.
"""
import os
import threading

import pytest

from avalon.operations.LakeFsWrapper import LakeFsWrapper, _transfer_workers


class FakeStat:
    def __init__(self, size):
        self.size_bytes = size


class FakeObjectsApi:
    """Serves deterministic per-path content, slowest object first."""

    def __init__(self, contents, delay=0.0):
        self.contents = contents
        self.delay = delay
        self.stat_calls = []
        self.get_calls = []
        self._lock = threading.Lock()

    def stat_object(self, repository, ref, path):
        with self._lock:
            self.stat_calls.append(path)
        return FakeStat(len(self.contents[path]))

    def get_object(self, repository, ref, path, range=None):
        import time
        # invert latency by path order so later objects return first
        order = sorted(self.contents).index(path)
        time.sleep(self.delay * (len(self.contents) - order))
        with self._lock:
            self.get_calls.append(path)
        data = self.contents[path]
        if range:
            spec = range.split('=')[1]
            start, end = (int(x) for x in spec.split('-'))
            return data[start:end + 1]
        return data


def build_wrapper(objects_api):
    wrapper = LakeFsWrapper.__new__(LakeFsWrapper)   # skip real client setup
    wrapper._client = type('C', (), {'objects_api': objects_api})()
    return wrapper


CONTENTS = {
    f"prefix/dir{i}/concepts.txt": (f"payload-{i}-" * 40).encode()
    for i in range(25)
}


@pytest.mark.parametrize("workers", [1, 4, 16])
def test_download_files_matches_serial(workers, tmp_path):
    api = FakeObjectsApi(CONTENTS, delay=0.001)
    build_wrapper(api).download_files(
        remote_files=list(CONTENTS), local_path=str(tmp_path),
        repository="repo", branch_or_commit_id="main", max_workers=workers)

    # every object fetched exactly once
    assert sorted(api.get_calls) == sorted(CONTENTS)
    # and every byte landed in the right file
    for path, expected in CONTENTS.items():
        written = (tmp_path / path).read_bytes()
        assert written == expected, f"content mismatch for {path}"


def test_download_is_byte_identical_serial_vs_threaded(tmp_path):
    serial_dir = tmp_path / "serial"
    threaded_dir = tmp_path / "threaded"
    for d in (serial_dir, threaded_dir):
        d.mkdir()

    for target, workers in ((serial_dir, 1), (threaded_dir, 16)):
        api = FakeObjectsApi(CONTENTS, delay=0.001)
        build_wrapper(api).download_files(
            remote_files=list(CONTENTS), local_path=str(target),
            repository="repo", branch_or_commit_id="main",
            max_workers=workers)

    for path in CONTENTS:
        assert (serial_dir / path).read_bytes() == (threaded_dir / path).read_bytes()


def test_download_failure_propagates(tmp_path):
    class Boom(FakeObjectsApi):
        def get_object(self, repository, ref, path, range=None):
            if path.endswith("dir7/concepts.txt"):
                raise RuntimeError("lakefs 500")
            return super().get_object(repository, ref, path, range)

    with pytest.raises(RuntimeError, match="lakefs 500"):
        build_wrapper(Boom(CONTENTS)).download_files(
            remote_files=list(CONTENTS), local_path=str(tmp_path),
            repository="repo", branch_or_commit_id="main", max_workers=8)


def test_chunked_large_object_reassembles(tmp_path):
    """A file bigger than the 32MB chunk must still come back intact."""
    big = os.urandom(32 * 1024 * 1024 + 5000)
    contents = {"prefix/big.bin": big}
    api = FakeObjectsApi(contents)
    build_wrapper(api).download_files(
        remote_files=list(contents), local_path=str(tmp_path),
        repository="repo", branch_or_commit_id="main", max_workers=4)
    assert (tmp_path / "prefix/big.bin").read_bytes() == big


def test_worker_count_capped_by_object_count():
    assert _transfer_workers(16, 3) == 3
    assert _transfer_workers(16, 100) == 16
    assert _transfer_workers(1, 100) == 1
    assert _transfer_workers(None, 2) == 2


def test_worker_count_env_override(monkeypatch):
    monkeypatch.setenv('AVALON_TRANSFER_WORKERS', '7')
    assert _transfer_workers(None, 100) == 7
