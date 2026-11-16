# Roadmap

## Shipped

- **1.0.0:** `RedisStreamsTransport` (the hook out: one stream per message name, the global order of the writes, the approximate cap, a refused message failing alone and a lost connection failing the batch) and `RedisStreamsSource` (the hook in: one consumer group per consumer, the merge of the streams in the order written, a delivery given back before the later ones of its key, the dead letter stream, the recovery of a dead process, invalid deliveries never dropped), with `RedisStreamsTopology`, for the messaging hook of ASpecification 2.14.0.

## On demand

Not planned; each one comes when a consumer needs it:

- [ ] Redis Cluster tested in the CI (today the keys of a hash-tagged prefix live in one slot, untested).
- [ ] A command-line tool for the dead letters: list, publish again, delete.
- [ ] A new consumer group created at a given entry id, besides the end and the start of the streams.
- [ ] A ready-made trimming of the dead letter stream by age.

The milestone view is in [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/ROADMAP.md).
