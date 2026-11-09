"""apply_patch safety guards and job-name resolution, with git and Databricks mocked."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from self_healing.mcp import databricks_mcp as server


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("DATABRICKS_GIT_FOLDER", "/Workspace/Users/u/repo")
    git = MagicMock(return_value="abc123")
    monkeypatch.setattr(server, "_run_git", git)
    (tmp_path / "job.py").write_text("x = 1\ny = 2\ny = 2\n")
    return SimpleNamespace(root=tmp_path, git=git)


def test_refuses_path_outside_repo(repo):
    with pytest.raises(ValueError, match="outside the repo root"):
        server.apply_patch("../escape.py", "a", "b")
    repo.git.assert_not_called()


def test_refuses_zero_matches_without_writing(repo):
    with pytest.raises(ValueError, match="0 matches"):
        server.apply_patch("job.py", "missing", "b")
    assert (repo.root / "job.py").read_text() == "x = 1\ny = 2\ny = 2\n"
    repo.git.assert_not_called()


def test_refuses_multiple_matches_without_writing(repo):
    with pytest.raises(ValueError, match="matches 2 times"):
        server.apply_patch("job.py", "y = 2", "y = 3")
    assert (repo.root / "job.py").read_text() == "x = 1\ny = 2\ny = 2\n"
    repo.git.assert_not_called()


def test_requires_git_folder_env(repo, monkeypatch):
    monkeypatch.delenv("DATABRICKS_GIT_FOLDER")
    with pytest.raises(RuntimeError, match="DATABRICKS_GIT_FOLDER"):
        server.apply_patch("job.py", "x = 1", "x = 2")


def _workspace(head_commit: str) -> MagicMock:
    w = MagicMock()
    w.workspace.get_status.return_value = SimpleNamespace(object_id=7)
    w.repos.get.return_value = SimpleNamespace(head_commit_id=head_commit)
    return w


def test_success_requires_workspace_at_pushed_commit(repo, monkeypatch):
    monkeypatch.setattr(server, "_client", lambda: _workspace("abc123"))
    result = server.apply_patch("job.py", "x = 1", "x = 2", category="oom")
    assert result["commit_message"] == "fix: apply oom patch to job.py"
    assert result["workspace_sync"]["matches_pushed_commit"] is True
    assert (repo.root / "job.py").read_text().startswith("x = 2")


def test_sync_mismatch_raises(repo, monkeypatch):
    monkeypatch.setattr(server, "_client", lambda: _workspace("stale"))
    with pytest.raises(RuntimeError, match="sync did not take"):
        server.apply_patch("job.py", "x = 1", "x = 2")


def _job(job_id: int, name: str) -> SimpleNamespace:
    return SimpleNamespace(job_id=job_id, settings=SimpleNamespace(name=name))


def test_resolves_only_exact_name():
    w = MagicMock()
    w.jobs.list.return_value = [_job(1, "prod-a"), _job(2, "a")]
    assert server._resolve_job_id(w, "a") == 2


@pytest.mark.parametrize("jobs", [[], [_job(1, "a"), _job(2, "a")]])
def test_refuses_zero_or_duplicate_jobs(jobs):
    w = MagicMock()
    w.jobs.list.return_value = jobs
    with pytest.raises(ValueError, match="exactly one job"):
        server._resolve_job_id(w, "a")
