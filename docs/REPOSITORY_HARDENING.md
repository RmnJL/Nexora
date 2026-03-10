# Repository Hardening Checklist (GitHub)

Owner: **RmnJL**

This file documents the exact GitHub settings to keep this repository owner-controlled.

## 1. Branch Protection (main)

Enable branch protection for `main` with:

- Require a pull request before merging
- Require approvals: `1`
- Require review from Code Owners
- Dismiss stale pull request approvals when new commits are pushed
- Require status checks to pass before merging
- Restrict who can push to matching branches: `RmnJL` only
- Do not allow force pushes
- Do not allow deletions

## 2. Repository Settings

- Disable auto-merge
- Disable squash/rebase merge methods if you want strict merge policy
- Restrict creation of new branches by collaborators (owner only)
- Keep repository private if you want maximum edit control

## 3. Collaboration Controls

- No write access for non-owner users
- No admin access for non-owner users
- Keep CODEOWNERS enabled: `* @RmnJL`

## 4. Operational Safety

- Enable Dependabot alerts
- Enable secret scanning
- Enable 2FA on owner account

