# Repository Governance

## Main branch

`main` accepts changes only through pull requests. Its protection policy requires:

- At least one approving review
- Dismissal of stale approvals after new commits
- Resolved review conversations
- Required checks: `Pull Request / Branch name`, `Pull Request / Lint`, and `Pull Request / Test`
- Strict required status checks
- Linear history
- Admin enforcement
- No force-pushes or branch deletion

Pull request source branches must start with `michael/`. The branch-name CI check enforces this convention before merge.

The protection settings are configured on GitHub rather than in the repository. Recheck them after repository ownership, default-branch, workflow, or required-check changes.

## Pull requests

Use a focused `michael/...` branch, keep the change small, and wait for all required checks and review approval before merging. The deployment workflow runs only after a change reaches `main`.
