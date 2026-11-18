# Antevemus ASpecification Redis Streams

[![PHP](https://img.shields.io/badge/PHP-8.2%2B-777bb4.svg)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/actions/workflows/tests.yml/badge.svg)](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/actions/workflows/tests.yml)

[Português (Brasil)](README.pt-BR.md)

**The Redis Streams satellite of the messaging hook of [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) for PHP 8.2+.** `RedisStreamsTransport` is the broker the core's `OutboxRelay` sends to (one stream per message name, `XADD` in one round trip per batch, confirmed by the id of the entry); `RedisStreamsSource` is what the core's `MessageConsumer` reads (one consumer group per consumer, `XREADGROUP` with `BLOCK`, `XACK`). At least once with the inbox of the core, the order of each ordering key kept across the streams of several names, a delivery given back before the later ones of its key, a dead letter stream, the recovery of what a dead process left pending (`XAUTOCLAIM`), invalid deliveries never dropped, and no payload in any reason. Never Pub/Sub. Pure PHP over predis: no extension is required.

Install with `composer require antevemus/aspecification-redis-streams`; it needs a Redis 7.0 or later server.

## Quick Start

<!-- tested: quick-start -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$redis = new Client('tcp://127.0.0.1:6379');

// The consumer group "agents" reads the stream of one message name: declare it before the relay runs.
$source = new RedisStreamsSource($redis, 'agents', ['underwriting.quote_submitted.v1'], 'agents-1');
$source->declare();

// Publish into the outbox (in production, PdoMessageOutbox inside the transaction of the write)...
$outbox = new InMemoryMessageOutbox();
(new MessagePublisher($outbox))->publish(new Message(
    'underwriting.quote_submitted.v1',
    ['quote_id' => 123, 'premium' => 1250.0],
    orderingKey: 'quote:123',
    tenant: 'acme'
));

// ...and relay it to Redis: an XADD to the stream aspec.underwriting.quote_submitted.v1.
$relay = new OutboxRelay($outbox, new RedisStreamsTransport($redis), maxAttempts: 10);
$relay->relayPending(100)->deliveredCount(); // 1

// Consume it once per consumer: the inbox absorbs a delivery that comes twice.
$tasks = [];
$handlers = (new MessageHandlerRegistry())->register('underwriting.quote_submitted.v1', function (Message $message) use (&$tasks): void {
    $tasks[] = $message->payload['quote_id'];
});
$consumer = new MessageConsumer('agents', $source, $handlers, new InMemoryProcessedMessageStore());
count($consumer->consume(100, waitMs: 1000)->handledIds()); // 1
$tasks; // [123]
```

## Documentation

The official documentation lives in [docs/en](docs/en/index.md) (and in Portuguese, in [docs/pt-BR](docs/pt-BR/index.md)); every PHP block of it runs in the test suite, against a real Redis.

| Page | What it covers |
| :--- | :--- |
| [Installation and quick start](docs/en/getting-started/index.md) | Composer, the requirements, the Redis server and the Quick Start. |
| [Delivery, order and failures](docs/en/concepts/delivery.md) | The keys in Redis, at least once with the inbox, the order across streams, the deliveries given back, the dead letters, the recovery of a dead process, the invalid deliveries and the secrecy of the reasons. |
| [RedisStreamsTopology](docs/en/api/topology.md) | The prefix of the keys, the cap of the streams, the idle time of the recovery, the longest block. |
| [RedisStreamsTransport](docs/en/api/transport.md) | The hook out: what an entry holds, what is confirmed and what fails. |
| [RedisStreamsSource](docs/en/api/source.md) | The hook in: the group and its members, `receive()`, `ack()`, `reject()`, `deadLetters()`. |
| [Tests](docs/en/quality/tests.md) | The contract battery of the core ([Writing a satellite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/writing-a-satellite.md)) run against a real Redis, with its property sequence, the tests of Redis itself and the mutation testing. |
| [Roadmap](docs/en/project/roadmap.md) | What shipped and what may come. |

## License

MIT, see [LICENSE](LICENSE). Provenance in [NOTICE.md](NOTICE.md); changes in [CHANGELOG.md](CHANGELOG.md).
