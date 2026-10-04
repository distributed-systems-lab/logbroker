# Cluster client contract

`ClusterClient` accepts bootstrap broker addresses, an optional expected cluster UUID,
and explicit connection, operation and byte limits. Without an expected UUID it pins
the first valid metadata response. A foreign cluster is rejected. Metadata refresh is
single flight; responses cannot replace a newer applied offset. Routes use the topic
UUID and committed owner/epochs. A reused topic name cannot redirect an old UUID.

The client owns endpoint transports and closes them on endpoint replacement or shutdown.
It borrows its deadline scheduler. Application futures complete outside the routing
monitor. Caller cancellation releases the operation reservation; a sent request may
still have executed. Closing does not wait for application callbacks.

Each operation uses one absolute deadline across metadata discovery, connection,
backoff and data requests. Produce results merge in input order and preserve successful
offsets alongside failures. Only explicit REJECTED results with retryable routing or
admission errors, or transport NOT_SENT, can retry. UNKNOWN never retries automatically.
CreateTopic timeout after sending is UNKNOWN. RF1 does not protect against disk loss.

Fetch allocates one global byte budget across sequential broker polls. Only the first
eligible data response may use the oversized first batch exception. Once that budget
is consumed, remaining partitions have an explicit admission error and unknown bounds;
the client does not invent offsets. `minBytes` applies to the combined operation, and
repeated polls retain the requested offsets. The response still obeys the absolute
protocol frame limit. Cluster Consumer returns the durable high watermark as its end
offset; callers choose the next offset explicitly.

Producer and Consumer accept `RequestClient`. Producer keeps one batch in flight per
partition through safe retries. Topic lookup retains its UUID for that producer lifetime.
Historical `BrokerClient` v1 fixtures remain available for compatibility tests; production
cluster brokers require the v2 routing protocol.
