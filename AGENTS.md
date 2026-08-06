# QuantMaster repository instructions

## Verification environment

- Run Python validation through the project virtual environment: `./.venv/Scripts/python.exe`
  on Windows (or the platform-equivalent `.venv` interpreter). Do not use a system Python for
  project tests, linting, or scripts.
- Full-only suites require `--full`. In restricted Windows environments, use a writable
  workspace-local pytest base directory, for example
  `./.venv/Scripts/python.exe -m pytest --full --basetemp .pytest-tmp <target>`.

## Release bookkeeping

- Every repository modification, including documentation and test changes, must increment
  `VERSION` in `quantmaster/release.py` using semantic versioning and set `RELEASE_DATE` to
  the actual release date.
- Add the matching user-facing notes to `RELEASES` in `quantmaster/release.py` and to the top
  of `CHANGELOG.md` in the same change.
- `quantmaster/release.py` is the runtime version source. Do not hard-code another application
  version in Python, HTML, or JavaScript; `pyproject.toml` reads it dynamically.
- Git tags and GitHub Releases must use `v{VERSION}`. The release workflow verifies the tag and
  publishes `CHANGELOG.md` as the GitHub Release body, so those records must stay synchronized.

## Release synchronization

- Run `python tools/release_sync.py install` once after cloning. The tracked hooks validate every
  commit and automatically push version-incrementing commits made on `main` to `origin/main`.
- A failed push leaves a pending marker inside `.git` and blocks the next release commit. Recover
  with `python tools/release_sync.py push`; inspect the state with `python tools/release_sync.py status`.
- Auto-push is intentionally limited to `main`. Never enable it for the archived Claude branch or
  use a release commit to move that branch.
- Do not bypass the hooks for normal project work. Each commit remains an independently validated,
  versioned release and must reach GitHub before the next version is committed.
