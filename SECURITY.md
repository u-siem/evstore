# Security policy

The store decodes its own files on every open and query: the WAL, the
manifest and memory-mapped segments. Those files can be damaged by crashes
and failing disks, or altered by anyone with access to the data directory,
so decoding bugs (panics, unbounded memory or CPU use, reading out of
bounds, accepting malformed data as valid) are security issues.

## Reporting a vulnerability

Please report privately through
[GitHub security advisories](https://github.com/u-siem/evstore/security/advisories/new),
not in a public issue. Include the version or commit, and if you can, the
file bytes or a test that reproduces the problem.

You should get an answer within a week. Once a fix is released, the advisory
is published and credited to you unless you prefer otherwise.

## Supported versions

Before 1.0, only the latest release gets fixes.
