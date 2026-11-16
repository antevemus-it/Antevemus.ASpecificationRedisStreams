# RedisStreamsTopology

## `new RedisStreamsTopology(string $prefix = 'aspec', ?int $maxLen = null, ?int $recoverIdleMs = 60000, int $maxBlockMs = 5000)`

One value shared by the transport and every source of an application, so that all of them name the same keys (`final`, its four properties `readonly`).

| Parameter | Default | What |
| :--- | :--- | :--- |
| `prefix` | `aspec` | The start of every key: 1 to 64 characters of `[A-Za-z0-9_.:{}-]`, not ending with a dot. A prefix in braces is a hash tag of a cluster. |
| `maxLen` | `null` | The approximate cap of each message stream, `XADD ... MAXLEN ~ maxLen` (at least 1); `null` = never trimmed. Redis trims whole nodes, so a stream may hold somewhat more. |
| `recoverIdleMs` | `60000` | A delivery pending for longer than this in a member of the group is claimed by the next `receive()` of any member (`XAUTOCLAIM`); `null` = no recovery (a member started again with the same name still takes its own). |
| `maxBlockMs` | `5000` | The longest a `receive()` blocks in `XREADGROUP ... BLOCK`, whatever wait it is given (at least 1); keep it below the read timeout of the connection (`read_write_timeout` of predis). |

| Method | Returns |
| :--- | :--- |
| `streamOf(string $messageName): string` | `<prefix>.<name>`; a name that is not `module.fact.vN` is refused (`InvalidArgumentException`). |
| `deadLetterStream(): string` | `<prefix>.dead` |
| `sequenceKey(): string` | `<prefix>.seq` |

<!-- tested: topology -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;

$topology = new RedisStreamsTopology(prefix: '{shop}', maxLen: 1_000_000, recoverIdleMs: 30_000, maxBlockMs: 2_000);
$topology->streamOf('crm.lead_created.v1'); // '{shop}.crm.lead_created.v1'
$topology->deadLetterStream(); // '{shop}.dead'
$topology->sequenceKey(); // '{shop}.seq'
new RedisStreamsTopology(prefix: 'shop.'); // throws InvalidArgumentException: The prefix must have 1 to 64 characters of [A-Za-z0-9_.:{}-] and not end with a dot.
```
