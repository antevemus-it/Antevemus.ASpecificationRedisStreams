# Delivery, Order and Failures

## The keys in Redis

Every key of the package starts with the prefix of the `RedisStreamsTopology` (default `aspec`):

| Key | What |
| :--- | :--- |
| `<prefix>.<message name>` | One stream per message name (`aspec.crm.lead_created.v1`): the transport appends there, and each consumer of the core reads it as a consumer group of the same name. |
| `<prefix>.dead` | The dead letters of every consumer group. |
| `<prefix>.seq` | The counter of the global order of the writes. |

A stream of a message always ends in `.vN`, so it never collides with the other two. A prefix in braces (`{aspec}`) is a hash tag: it keeps every key in one slot of a cluster. Never Pub/Sub: a stream keeps what is published while a consumer is away, and a consumer group remembers what each consumer has read and acknowledged.

## At least once, then once per consumer

A message is confirmed when Redis answered its `XADD` with the id of the new entry: the relay of the core marks it delivered only then. A relay that dies between the answer and the mark sends it again on its next run, and a consumer that dies between its commit and its `XACK` gets it again: delivery is **at least once**. The inbox of the core (`IProcessedMessageStore`) turns it into **once per consumer**: a message it already has is acknowledged as `duplicate`, and the handler does not run again. With a transaction manager over the connection of the handler, the inbox row and the effect commit together.

What "stored" means is the persistence of the server: with `appendonly yes` and `appendfsync everysec`, a crash of the server loses at most about a second of confirmed writes; with `appendfsync always`, none; without persistence, everything in memory. Configure the server for the guarantee the application needs.

## The order of an ordering key

The messages of an ordering key reach a consumer in the order they were published, also when they have different names and so live in different streams (`quote.created.v1` then `quote.sent.v1` of `quote:123`):

- the relay of the core sends a message of a key only after the previous one was confirmed;
- the transport appends each entry and increments `<prefix>.seq` in one atomic step (a Lua script), and writes the value in the field `seq` of the entry: the global order of the writes. The entry ids of two streams cannot be compared for that: they are timestamps in milliseconds with a sequence of their own per stream;
- the source merges the entries of its streams by `seq`, and reads each stream far enough (at least as many entries as it hands out, or all it has) that no entry left behind can come before one it hands out.

Order holds for one consumer per source, as in any broker: two members of a group share its messages, and each keeps the order of what it gets.

## A delivery given back comes back first

`MessageConsumer` rejects with requeue a delivery whose handler failed, and the later deliveries of the same key in that run (`held_back`). Redis Streams has no requeue of its own, so the source keeps the delivery **pending in its member** of the group and, on the next `receive()`, claims it again (`XCLAIM`, which also counts the delivery) before anything newer: it is older than everything not yet handed out, so it comes again before any later delivery of its key.

<!-- tested: requeue-order -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$redis = new Client('tcp://127.0.0.1:6379');
$source = new RedisStreamsSource($redis, 'billing', ['billing.invoice_overdue.v1'], 'billing-1');
$source->declare();
(new RedisStreamsTransport($redis))->send([
    new Message('billing.invoice_overdue.v1', ['invoice' => 1], orderingKey: 'customer:7'),
    new Message('billing.invoice_overdue.v1', ['invoice' => 2], orderingKey: 'customer:7'),
]);

$first = $source->receive(1, 0)[0];
$first->message()->payload['invoice']; // 1
$source->reject($first, true, 'the mail server is down'); // given back: still pending in billing-1

$again = $source->receive(10, 0);
$again[0]->message()->payload['invoice']; // 1
$again[0]->deliveryCount; // 2
$again[1]->message()->payload['invoice']; // 2
$again[1]->deliveryCount; // 1
```

The transport is called directly to keep the example short; in an application the relay calls it. The delivery count is the counter of Redis for the entry in the group (`XPENDING`): 1 when first read, plus one for each claim.

## Dead letters

`reject($requeue = false)` appends a dead letter to `<prefix>.dead` and acknowledges the original, in one atomic step. The dead letter keeps the origin (`stream`, `entry_id`), the `group`, the member (`consumer`), the `delivery_count`, the `reason`, the `message_id`, `name`, `ordering_key` and `tenant` of the message when the delivery was one, and the original `body`, so that an operator can inspect it and publish it again. `MessageConsumer` dead-letters a delivery when its handler refuses the payload (`InvalidMessagePayloadException`), when the handler failed on delivery `maxDeliveries`, when the count is already above it, and when the delivery is invalid.

<!-- tested: dead-letters -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Exceptions\InvalidMessagePayloadException;
use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$redis = new Client('tcp://127.0.0.1:6379');
$source = new RedisStreamsSource($redis, 'crm', ['crm.lead_created.v1'], 'crm-1');
$source->declare();
(new RedisStreamsTransport($redis))->send([new Message('crm.lead_created.v1', ['name' => 'Ana'])]);

$handlers = (new MessageHandlerRegistry())->register('crm.lead_created.v1', function (Message $message): void {
    throw new InvalidMessagePayloadException('lead_id is required'); // retrying would not change the answer
});
$report = (new MessageConsumer('crm', $source, $handlers, new InMemoryProcessedMessageStore()))->consume(10);
$report->entries()[0]->outcome->value; // 'dead_lettered'

$dead = $source->deadLetters();
count($dead); // 1
$dead[0]['deliveryCount']; // 1
str_contains($dead[0]['reason'], 'Ana'); // false (a reason never carries the payload)
array_keys(current($redis->xrange('aspec.dead', '-', '+'))); // ['stream', 'entry_id', 'group', 'consumer', 'delivery_count', 'reason', 'message_id', 'name', 'body']
```

`deadLetters()` scans the dead letter stream for the letters of the group of the source, oldest first. The dead letter stream is never trimmed by the package: delete what was handled with `XDEL` or `XTRIM`.

## The recovery of a dead process

A delivery stays pending in the member that read it until it is acknowledged. A member that dies leaves its deliveries pending: every `receive()` of any member of the group claims (`XAUTOCLAIM`) the deliveries pending for longer than `recoverIdleMs` (60 000 ms by default), walking the pending entries of the group a little at each call. A member started again **with the same name** does not wait: its first `receive()` takes what it left unanswered. Keep `recoverIdleMs` above the longest handling of a message: a delivery kept in flight longer than that counts as abandoned: another member may claim it, and its count grows.

<!-- tested: idle-recovery -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$redis = new Client('tcp://127.0.0.1:6379');
$topology = new RedisStreamsTopology(recoverIdleMs: 100);
$first = new RedisStreamsSource($redis, 'agents', ['crm.lead_created.v1'], 'agents-1', $topology);
$first->declare();
(new RedisStreamsTransport($redis, $topology))->send([new Message('crm.lead_created.v1', ['lead_id' => 9])]);
count($first->receive(10, 0)); // 1

// agents-1 dies here, before answering. Another member of the group takes over after 100 ms:
$second = new RedisStreamsSource($redis, 'agents', ['crm.lead_created.v1'], 'agents-2', $topology);
count($second->receive(10, 0)); // 0 (not idle for 100 ms yet)
usleep(150_000);
$recovered = $second->receive(10, 0);
$recovered[0]->message()->payload['lead_id']; // 9
$recovered[0]->deliveryCount; // 2
```

## Invalid deliveries

What the source cannot read back as a message is an invalid delivery (`ReceivedMessage::invalid()`), never dropped: a body that is not the wire form of format 1 of the core, an entry without a `body` field (another producer wrote to the stream), and an entry that was deleted while pending (trimmed by `MAXLEN`, or `XDEL`). `MessageConsumer` dead-letters it; for a deleted entry the dead letter has no body, and says the message is lost. Size `maxLen` well above the backlog a consumer may fall behind.

## Secrecy

No reason, exception or report of the package carries a value of the payload. A reason names the message (its name, id and ordering key), the stream and the cause: the error code of Redis (`WRONGTYPE`, `NOPERM`, `OOM`) and never the rest of its text, which may quote the arguments of the command; the class of the exception of the client and, for a failed connection, its message, which names the server. The payload lives only in the entries and in the dead letters, data at rest in the broker.
