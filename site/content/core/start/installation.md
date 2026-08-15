+++
title = "Installation"
description = "Install a released peryx binary and keep it updated."
weight = 2
+++

Each channel installs the same executable with the shipped ecosystem owners and availability implementations.
Configuration selects index owners and the process availability mode after installation.

| Channel                     | Install                                                                                                    | Update              |
| --------------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------- |
| Installer script on Unix    | `curl -LsSf https://github.com/tox-dev/peryx/releases/latest/download/peryx-installer.sh \| sh`            | `peryx self update` |
| Installer script on Windows | `powershell -c "irm https://github.com/tox-dev/peryx/releases/latest/download/peryx-installer.ps1 \| iex"` | `peryx self update` |

GitHub releases provide checksummed binaries for the supported macOS, Linux, and Windows targets.
[Contributors can build from a checkout](@/contributing/build.md).

## Self-update ownership

`peryx self update` works for copies placed by an installer script because the installer writes an update receipt. A
binary installed by another tool has no receipt; update it with the tool that owns that file. Peryx refuses the
self-update rather than replacing a file managed elsewhere.
