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
| `Contract/` | `RedisStreamsContractTest`: the contract battery of the core (`MessagingContractTestCase` of `antevemus/aspecification` 2.15, 22 scenarios, the property sequence once per each of its 6 seeds: 27 tests) against Redis, over `RedisStreamsBench`. The core runs the same battery against its in-memory reference. How to write a bench: [Writing a satellite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/writing-a-satellite.md). |
| `Integration/` | `RedisStreamsTransportTest` (9) and `RedisStreamsSourceTest` (46) against Redis: the fields, the global order across streams, the cap, the group, the reading depth, the blocking, the requeue, the dead letters, the recovery, the invalid deliveries and every refusal of Redis. |
| `Support/` | The bench of the battery over Redis (`RedisStreamsBench`, an `IMessagingBench` of the core) and the test server (`RedisServer`, `RedisTestCase`). |

1.0.1: 142 tests, 4 548 assertions with Redis (94 skipped without); 1.0.0 had 152 tests and 3 947 assertions. Mutation testing: see `infection.json5`.
