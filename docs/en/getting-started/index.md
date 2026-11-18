# Getting Started

## 📦 Installation

Install via Composer:

```bash
composer require antevemus/aspecification-redis-streams
```

Requirements: PHP 8.2 or later and a **Redis 7.0 or later** server (tested on 7.4). Installed with it: [`antevemus/aspecification`](https://github.com/antevemus-it/Antevemus.ASpecification) `^2.15` (the core, from the same author, whose messaging hook this package plugs into) and [`predis/predis`](https://github.com/predis/predis) `^2` (a Redis client in pure PHP). No PHP extension is required: `ext-redis` is not used.

What it is for: carrying the messages of the core's messaging hook between the modules of an application through Redis Streams. The application keeps publishing into the outbox of the core, inside its own transaction; the `OutboxRelay` of the core sends the outbox to Redis through `RedisStreamsTransport`, and each `MessageConsumer` of the core reads its own consumer group through `RedisStreamsSource`. Nothing here talks to Redis Pub/Sub, which loses what is published while a consumer is away.

## 🚀 Quick Start

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

The outbox and the inbox are the in-memory ones of the core so that the example stands alone; in an application they are `PdoMessageOutbox` and `PdoProcessedMessageStore`, over the tables of the core's `resources/sql/messaging.ansi.sql`, with a transaction manager so that the inbox row and the effect of the handler commit together. The consumer name of the core (`agents`) is also the name of the consumer group, and `agents-1` is this process in the group: give each worker a stable name, so that a worker started again takes at once what it left unanswered.

Next: [Delivery, order and failures](../concepts/delivery.md).
