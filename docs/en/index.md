# Antevemus ASpecification Redis Streams

The official documentation of the package, one page per topic. The same pages exist in Portuguese (Brazil), in `docs/pt-BR`, and every PHP block of these pages is run by the test suite with the answers written in its comments.

> **The Redis Streams satellite of the messaging hook of [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) for PHP 8.2+**  
> `RedisStreamsTransport` is the broker the `OutboxRelay` of the core sends to, one stream per message name; `RedisStreamsSource` is what a `MessageConsumer` reads, one consumer group per consumer. At least once, the order of each ordering key kept across the streams of several names, a delivery given back before the later ones of its key, a dead letter stream, the recovery of what a dead process left pending, and no payload in any reason. Pure PHP over predis: no extension is required.

## Documentation Map

### Getting started

- [Installation and quick start](getting-started/index.md): Composer, the requirements, the Redis server and the Quick Start.

### Concepts

- [Delivery, order and failures](concepts/delivery.md): the keys in Redis, at least once with the inbox, the order across streams, the deliveries given back, the dead letters, the recovery of a dead process, the invalid deliveries and the secrecy of the reasons.

### API reference

- [RedisStreamsTopology](api/topology.md): the prefix of the keys, the cap of the streams, the idle time of the recovery and the longest block.
- [RedisStreamsTransport](api/transport.md): the hook out, what an entry holds, what is confirmed and what fails.
- [RedisStreamsSource](api/source.md): the hook in, the group and its members, `receive()`, `ack()`, `reject()` and `deadLetters()`.

### Quality

- [Tests](quality/tests.md): the contract battery run against the core's in-memory broker and a real Redis, the failure scenarios, the property over random interleavings and the mutation testing.

### Project

- [Roadmap](project/roadmap.md): the release shipped and what may come next.

## Where Is Each Thing

| What | Where |
| :--- | :--- |
| Overview and the Quick Start | [README.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/README.md) |
| What changed in each version | [CHANGELOG.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/CHANGELOG.md) |
| Milestones shipped and planned | [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/ROADMAP.md) |
| License and provenance | [LICENSE](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/LICENSE), [NOTICE.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/NOTICE.md) |
| The messaging hook of the core | [Messaging between modules](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/index.md) |
