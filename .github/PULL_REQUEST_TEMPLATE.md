# Pull Request

## Summary

<!-- One or two sentences: what does this change do and why? -->

## Linked issue

<!-- Closes #... / Refs #... (leave blank if none) -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor (no behaviour change)
- [ ] Documentation / chore
- [ ] Other: <!-- describe -->

## Test plan

- [ ] Fast suite passes locally: `PYTHONPATH="./src" pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q`
- [ ] Lint/format clean: `ruff check src tests && black --check src tests && isort --check-only src tests`
- [ ] For schema changes: migration tested against a real warehouse, counts verified
- [ ] For UI changes: manually verified in a running Streamlit instance; screenshot below

## Screenshots / output

<!-- Required for UI changes; optional for backend-only -->

## Changelog

- [ ] I added an entry to `CHANGELOG.md` under `## [Unreleased]` in the appropriate category, OR
- [ ] This change is too small to warrant a changelog entry (typos, formatting-only, etc.)

## Additional notes

<!-- Anything reviewers should pay special attention to: edge cases, follow-ups, decisions left open -->
