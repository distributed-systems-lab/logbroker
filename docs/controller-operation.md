# Controller operation

Phase 3 uses three fixed voters. Each voter must run exactly one controller with its configured node ID and endpoint. The operator must prevent duplicate IDs across separate data roots: the root file lock only prevents concurrent use of the same directory.

Controller startup replays durable state and committed metadata before binding its listener. Uncommitted suffixes do not enter the local catalog. A storage or durability failure stops consensus participation and fails admin requests with `NODE_UNAVAILABLE`.

Strict controller durability requires Java 21 on Linux/WSL and a filesystem supporting directory `FileChannel.force`, such as the tested WSL ext4 filesystem. Windows native is not a supported controller durability platform. Startup probes support and fails explicitly rather than weakening persistence. Unit tests with simulated directory forcing are not platform acceptance evidence.

Shutdown drains accepted disk work before releasing files and the root lock. If disk I/O remains blocked past the shutdown deadline, close reports failure and retains ownership. Retry close after the worker finishes, or terminate the process; never launch another controller against that root while the first process retains it.
# Phase 4 extension

See [cluster operations](cluster-operation.md) for broker observer sessions, RF1 serving
permission, full v2 metadata views and safe client retry outcomes. Historical v1 examples
require explicit metadata version 1 after the default switch to version 2.
