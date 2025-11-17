# Contributing

Thanks for your interest in contributing to IncrementalJoin. Please follow these guidelines to make contributions easy to review and include.

## Getting started (Windows / PowerShell)
1. Fork the repository and clone your fork:
   ```powershell
   git clone https://github.com/<your-username>/IncrementalJoin.git
   cd IncrementalJoin
   ```
2. Create and activate a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -e .
   ```
3. Install development/test deps (if provided):
   ```powershell
   pip install -r requirements-dev.txt
   ```

## Workflow
- Create a feature branch from `main`:
  ```powershell
  git checkout -b feat/short-description
  ```
- Make small, focused commits with clear messages.
- Rebase or merge `main` frequently to keep your branch up to date.
- Push and open a Pull Request against `main` on GitHub.

## Pull Request checklist
- [ ] The PR has a clear title and description explaining what and why.
- [ ] All tests pass: `pytest` (or project-specific test command).
- [ ] New code follows the repository's style and includes docstrings or README updates when needed.
- [ ] No sensitive data or large binaries are added.

## Reporting issues
- Open an issue with steps to reproduce, expected vs actual behavior, and relevant logs or stack traces.
- Use a minimal reproducible example where possible.

## Code style & tests
- Follow PEP 8 for Python code.
- Add unit tests for bug fixes and new features.
- Run tests locally before pushing:
  ```powershell
  pytest
  ```

## Commit messages
- Use concise, imperative-style messages (e.g., "Add incremental-join SQL helper").
- Reference issues when relevant: "Fixes #12".

## License
By contributing you agree that your contributions will be licensed under the project's LGPLv3+ license.

If you have questions or need help getting started, open an issue or contact the maintainers.