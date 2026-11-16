# Tests

```bash
composer install
# without a Redis server: the tests that need one are skipped with the reason
vendor/bin/phpunit
# with a Redis 7.4 server for tests (opt-in; database 15 by default, each test under a prefix of its own)
docker run -d --name aspec-redis-streams-test -p 127.0.0.1:16379:6379 redis:7.4-alpine
ASPEC_REDIS_HOST=127.0.0.1 ASPEC_REDIS_PORT=16379 vendor/bin/phpunit
```

| Folder | What |
| :--- | :--- |
| `Unit/` | The topology (`RedisStreamsTopologyTest`), the order and the fields of the entries (`StreamEntriesTest`), the pipeline and the texts of a failure (`RedisCommandsTest`), and the documentation (`DocumentationTreeTest`, `DocumentationBlocksTest`; the five blocks that talk to Redis run against the test server). |
| `Contract/` | The contract battery, written once in `MessagingContractTestCase` (16 scenarios): `InMemoryContractTest` against the in-memory broker of the core, `RedisStreamsContractTest` against Redis, with the same transcripts. |
| `Integration/` | `RedisStreamsTransportTest` (9) and `RedisStreamsSourceTest` (45) against Redis: the fields, the order, the cap, the group, the reading depth, the blocking, the requeue, the dead letters, the recovery, the invalid deliveries and every refusal of Redis. |
| `Property/` | `MessagingPropertyTest`: 3 fixed seeds × 150 steps of random publish, relay and consume, with crashes of the relay and of the consumer and copies of the broker, against Redis and against the core's in-memory broker. |
| `Support/` | The two brokers of the battery (`InMemoryBroker`, `RedisStreamsBroker`), the path of a message through the core (`Harness`), the test server (`RedisServer`, `RedisTestCase`), and the faults (`CrashingOutbox`, `DyingSource`, `LostConnectionTransport`, `EffectLog`). |

1.0.0: 152 tests, 3 947 assertions with Redis (85 skipped without). Mutation testing: see `infection.json5`.
