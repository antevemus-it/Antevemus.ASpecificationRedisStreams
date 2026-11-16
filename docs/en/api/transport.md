# RedisStreamsTransport

## `new RedisStreamsTransport(Predis\ClientInterface $redis, RedisStreamsTopology $topology = new RedisStreamsTopology())`

The `IMessageTransport` of the core 2.14.0 (`final`): `OutboxRelay` hands it each batch. `getTopology()` returns the topology it writes to.

### `send(array $batch): TransportSendResult`

- Every message of the batch is appended to the stream of its name, `<prefix>.<name>` (with `MAXLEN ~` when the topology caps the streams), by one Lua script that also increments `<prefix>.seq`; the whole batch goes in one round trip.
- **Confirmed** = Redis answered with the id of the new entry, which is the broker id of the result (`brokerIdOf()`).
- The entry holds, in this order: `seq` (the global order of the writes), `message_id`, `name`, `ordering_key` and `tenant` (only when the message has them) and `body`, the wire form of the core (`Message::toJson()`, which carries the tenant and the headers too). None of the fields but `body` holds a value of the payload.
- **A message Redis refused** (the key holds another type, the server is out of memory, the user may not run scripts) fails alone, with a reason that names the message, the stream and the error code of Redis; the others of the batch are still confirmed.
- **A connection that fails**, before or during the batch, fails every message of the batch with one reason. What Redis stored before the connection dropped is sent again by the relay, and the inbox of the consumers absorbs the copy.
- An empty batch sends nothing.

<!-- tested: transport-failures -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Predis\Client;

$offline = new RedisStreamsTransport(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 0.5]));
$message = new Message('crm.lead_created.v1', ['card' => '4111 1111 1111 1111'], orderingKey: 'lead:9', messageId: '01JCZ0000000000000000000AB');
$result = $offline->send([$message]);
$result->isConfirmed($message->messageId); // false
$result->reasonOf($message->messageId); // 'Redis Streams did not confirm the batch of 1 message(s): ConnectionException: Connection refused [tcp://127.0.0.1:1].'
```

A refused message reads, for example, `Redis refused message "crm.lead_created.v1" (id 01JCZ0000000000000000000AB, ordering key "lead:9") on stream "aspec.crm.lead_created.v1" (WRONGTYPE).`

The client is any `Predis\ClientInterface` (a single server, or a replication or Sentinel setup, whose master predis selects); the transport sends raw commands through its connection, whatever its `exceptions` option. A Redis Cluster is not tested in 1.0.0: with a hash-tagged prefix every key of the package lives in one slot.
