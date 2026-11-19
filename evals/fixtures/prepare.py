#!/usr/bin/env python3
"""Prepare a working copy of a fixture for one eval run.

    python3 evals/fixtures/prepare.py small  --work D:/cbm-evals/work   # -> <work>/eval-small
    python3 evals/fixtures/prepare.py medium --work D:/cbm-evals/work   # -> <work>/eval-medium
    python3 evals/fixtures/prepare.py both   --work D:/cbm-evals/work

Why a copy: the evals edit, add and delete files (delta sync, detect_changes,
freshness) and need a Git history of their own. The copy is regenerated from
make_fixture.py / gen_medium.py, so the committed eval-small/ is only for
reading, and the git-ignored generated/gen.py is always present in the copy.

Each working copy is a fresh Git repository with one commit on branch `main`,
user "cbm-evals", so detect_changes has a merge base and a deterministic
branch name.
"""
import argparse
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def git(cwd, *args):
    env = dict(os.environ, GIT_AUTHOR_NAME="cbm-evals", GIT_AUTHOR_EMAIL="evals@example.invalid",
               GIT_COMMITTER_NAME="cbm-evals", GIT_COMMITTER_EMAIL="evals@example.invalid")
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed in {cwd}:\n{r.stderr}")
    return r.stdout


def init_repo(path):
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "core.autocrlf", "false")
    git(path, "add", "-A")
    git(path, "commit", "-q", "-m", "fixture baseline")
    return git(path, "rev-parse", "HEAD").strip()


def prepare_small(work):
    import make_fixture
    out = os.path.join(work, "eval-small")
    if os.path.isdir(out):
        shutil.rmtree(out)
    n = make_fixture.write(out, clean=True)
    sha = init_repo(out)
    print(f"eval-small: {n} files -> {out} (commit {sha[:12]})")
    return out


def prepare_medium(work, files, hub_callers, seed):
    import gen_medium
    out = os.path.join(work, "eval-medium")
    if os.path.isdir(out):
        shutil.rmtree(out)
    stats = gen_medium.generate(out, files=files, hub_callers=hub_callers, seed=seed)
    sha = init_repo(out)
    print(f"eval-medium: {stats} -> {out} (commit {sha[:12]})")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("which", choices=["small", "medium", "both"])
    ap.add_argument("--work", required=True, help="parent directory for the working copies")
    ap.add_argument("--medium-files", type=int, default=20000)
    ap.add_argument("--hub-callers", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    os.makedirs(a.work, exist_ok=True)
    if a.which in ("small", "both"):
        prepare_small(a.work)
    if a.which in ("medium", "both"):
        prepare_medium(a.work, a.medium_files, a.hub_callers, a.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
