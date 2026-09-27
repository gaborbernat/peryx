# Changelog

Each release lists the change files merged since the previous one. Versions follow the calendar as `YYYY.MDD.N`: the
year, the month and zero-padded day, and a counter for several releases on one day.

## 2026.927.0 (2026-09-27)

### Changes

- 🐛 fix(web): hide the login link when it offers nothing (#2357)
- ✨ feat(serve): create an admin on first start (#2358)
- ✨ feat(web): sign in with a local password (#2359)
- 🔧 build(deps): drop dependencies builds never use (#2360)
- ✨ feat(web): change a local password from the UI (#2362)
- ⬆️ chore(deps): update every dependency and tool to latest (#2361)
- ✨ feat(identity): end sessions on a password change (#2363)
- 🔒 fix(storage): end sessions on any password write (#2364)
- 👷 ci(release): generate notes from merged pull requests (#2366)

## 2026.925.0 (2026-09-25)

### Features

#### First public release

peryx ships as one executable that caches and hosts PyPI packages and OCI images. Install it with the shell or
PowerShell installer from the GitHub release, or with `pip install peryx`.
