# QuantMaster repository instructions

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
