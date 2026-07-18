<!-- SPDX-License-Identifier: Apache-2.0 -->

# daemonkit Claude Code skills

[Claude Code](https://code.claude.com) skills that help you **write** and
**audit** daemons and monitors built on
[daemonkit](https://github.com/automa-saga/daemonkit).

- **Author** — scaffold a `MonitorRunner`, wire `SupervisedMonitor`, and follow
  the panic-safety and concurrency contracts.
- **Audit** — scan an existing daemonkit-based repo for the anti-patterns that
  crash daemons or cause data races (bare `go func()`, monitors launched without
  `SupervisedMonitor`, unguarded `ConnectivityError`, raw `errgroup`, `Run`
  returning non-nil on ctx cancel) and apply the fixes.

## Install

Copy the skill into your project (checked in, shared with your team):

```bash
mkdir -p .claude/skills
cp -R "$(go env GOMODCACHE)"/github.com/automa-saga/daemonkit@*/skills/daemonkit-monitors .claude/skills/
# …or from a local clone:
cp -R path/to/daemonkit/skills/daemonkit-monitors .claude/skills/
```

Or for personal use across all your projects:

```bash
cp -R path/to/daemonkit/skills/daemonkit-monitors ~/.claude/skills/
```

Re-copy after upgrading daemonkit so the contract stays in sync with the version
you depend on.

## Use

Once installed, just ask Claude in that repo:

- *"Write a daemonkit monitor that …"* / *"scaffold a daemon with two monitors"*
- *"Audit this repo for daemonkit issues"* / *"scan my monitors for panic-safety
  bugs and fix them"*

Claude auto-selects the skill from the request; you can also invoke it by name
(`daemonkit-monitors`).

## Layout

```
daemonkit-monitors/
├── SKILL.md                   # entry point: rules + author/audit workflows
└── references/
    ├── contract.md            # MonitorRunner / SupervisedMonitor / Go / Group / probes
    ├── audit-checklist.md     # detect → why → fix, per anti-pattern
    └── templates.md           # copy-paste monitor + daemon skeletons
```

Licensed Apache-2.0, same as daemonkit.
