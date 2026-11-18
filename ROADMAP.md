# Roadmap

The Redis Streams satellite of the messaging hook of ASpecification. The core keeps no broker client: it defines the hook (`IMessageTransport`, `IMessageSource`) and its in-memory reference, and each broker lives in a satellite package of its own.

## Milestones

- [x] **1.0.0:** `RedisStreamsTransport`, `RedisStreamsSource` and `RedisStreamsTopology` for the messaging hook of ASpecification 2.14.0: one stream per message name, the global order of the writes across streams, consumer groups, the deliveries given back first, the dead letter stream, the recovery of a dead process, invalid deliveries never dropped, no payload in any reason.
- [x] **1.0.1:** the contract tests run the public battery of ASpecification 2.15.0 (`MessagingContractTestCase`, with its property sequence of six seeds; see [Writing a satellite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/writing-a-satellite.md)) against Redis, through a bench of this package, instead of a private copy; the API is unchanged and the core is required at `^2.15`.

## On demand

Not planned; each one comes when a consumer needs it:

- [ ] Redis Cluster tested in the CI.
- [ ] A command-line tool for the dead letters: list, publish again, delete.
- [ ] A new consumer group created at a given entry id, besides the end and the start of the streams.
- [ ] A ready-made trimming of the dead letter stream by age.
