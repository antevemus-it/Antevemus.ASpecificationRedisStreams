# RedisStreamsSource

## `new RedisStreamsSource(Predis\ClientInterface $redis, string $group, array $messageNames, ?string $consumerName = null, RedisStreamsTopology $topology = new RedisStreamsTopology())`

The `IMessageSource` of the core 2.14.0 (`final`) that a `MessageConsumer` reads.

| Parameter | What |
| :--- | :--- |
| `group` | The consumer group: the name of the consumer of the core (lowercase, at most 120 characters, as `MessageConsumer` requires), the same group on every stream it reads. |
| `messageNames` | The names whose streams the group reads (at least one; a name given twice is read once). A consumer reads only what it lists. |
| `consumerName` | This member of the group (`[A-Za-z0-9][A-Za-z0-9._:@-]{0,119}`); `null` = `<host>-<pid>`. Give each worker a stable name: started again, it takes at once what it left unanswered. |
| `topology` | The keys and the limits, the same as the transport's. |

`getGroup()`, `getConsumerName()`, `getStreams()` (one stream per name, in the order given) and `getTopology()` return what it was built with; `inFlightCount()` the deliveries handed out and not yet answered.

| Method | What |
| :--- | :--- |
| `declare(bool $fromStart = false): void` | Creates the group on every stream, and the stream when missing (`XGROUP CREATE ... $ MKSTREAM`); a group that exists is kept. A new group gets what is added from then on, or, with `fromStart`, what the streams still hold. The first `receive()` declares it; declare the consumers before the relay sends what they must get. |
| `receive(int $max, int $waitMs): list<ReceivedMessage>` | At most `$max` deliveries, in the global order of the writes: the deliveries of a dead member (idle longer than `recoverIdleMs`), the ones this member gave back or left unanswered, the entries read and not yet handed out, then new entries, blocking for at most `min($waitMs, maxBlockMs)` when there is nothing else. |
| `ack(ReceivedMessage $message): void` | `XACK`: it will not come again. |
| `reject(ReceivedMessage $message, bool $requeue, string $reason): void` | With `$requeue`, the delivery stays pending in this member and comes again first; without, a dead letter in `<prefix>.dead` and `XACK`, atomically. |
| `deadLetters(int $limit = 100): list<array>` | The dead letters of this group, oldest first: `brokerId`, `messageId` (`null` for an invalid delivery), `reason`, `deliveryCount`, `deadLetterId`. |

- The **broker id** of a delivery is `<stream>#<entry id>`, the same on every delivery of the entry.
- `ack()` or `reject()` of a delivery that is not in flight in this source is a `MessagingException`, as in the in-memory source of the core. A failure of the connection, or an error reply of Redis, is a `RedisStreamsException` (a `MessagingException`), whose message names the operation, the stream and the cause; the delivery stays in flight, so it can be answered again.

<!-- tested: source -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$redis = new Client('tcp://127.0.0.1:6379');
$names = ['crm.lead_created.v1', 'crm.lead_updated.v1'];
$source = new RedisStreamsSource($redis, 'agents', $names, 'agents-1');
$source->getStreams(); // ['aspec.crm.lead_created.v1', 'aspec.crm.lead_updated.v1']
$source->declare();

$transport = new RedisStreamsTransport($redis);
$transport->send([new Message('crm.lead_updated.v1', ['lead_id' => 9], orderingKey: 'lead:9')]);
$transport->send([new Message('crm.lead_created.v1', ['lead_id' => 10], orderingKey: 'lead:10')]);

$deliveries = $source->receive(10, waitMs: 500);
array_map(fn(ReceivedMessage $delivery): string => $delivery->message()->name, $deliveries); // ['crm.lead_updated.v1', 'crm.lead_created.v1']
str_starts_with($deliveries[0]->brokerId, 'aspec.crm.lead_updated.v1#'); // true
$source->ack($deliveries[0]);

// The process dies with the second delivery in flight, and starts again with the same member name:
$restarted = new RedisStreamsSource($redis, 'agents', $names, 'agents-1');
$again = $restarted->receive(10, 0);
$again[0]->message()->payload['lead_id']; // 10
$again[0]->deliveryCount; // 2
```

The two streams are merged in the order they were written, whatever their entry ids say.
