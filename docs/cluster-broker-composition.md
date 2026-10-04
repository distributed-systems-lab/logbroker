# Cluster broker composition

Production startup is `BrokerMain --config <properties> --data <formatted-root>`.
Use `BrokerMain format --config <properties> --data <empty-root>` first. Formatting
publishes broker/storage identity last; startup does not format or migrate old roots.
Cluster properties include `cluster.id`, `broker.id`, `advertised.host`,
`advertised.port`, and `controller.bootstrap.servers` (comma-separated host:port).
Data-plane limits and CLI host/port overrides retain `BrokerConfig` validation.

The broker locks its root before opening the observer and partition inventory.
Binding the listener does not grant serving permission: each process registers a
fresh incarnation, catches up to a fixed recovery target and applies its committed
unfence grant. Controller absence alone does not revoke a RUNNING broker's grant.
CreateTopic forwards to the quorum with one absolute deadline; no local topic
metadata log is created. Committed assignments drive durable partition provisioning.

Shutdown closes admission, drains observer disk work and partition lanes, flushes
and closes storage, then releases the root lock. A drain failure retains ownership
because an outstanding worker may still use the files. Startup bind failure closes
opened resources before releasing the root.

This records cluster composition, not full Phase 4 acceptance. See
[data protocol v2](protocol-v2.md) for guarded I/O. Cluster clients and real cluster
process acceptance follow in Tasks 14–16. The cluster listener rejects v1 requests. Historical v1
tests use test-only `LegacyBrokerFixture`, `LegacyMetadataFixture` and
`LegacyBrokerMain`; these are not production startup paths.
