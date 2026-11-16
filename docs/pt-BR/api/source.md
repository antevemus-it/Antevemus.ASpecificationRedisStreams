# RedisStreamsSource

## `new RedisStreamsSource(Predis\ClientInterface $redis, string $group, array $messageNames, ?string $consumerName = null, RedisStreamsTopology $topology = new RedisStreamsTopology())`

O `IMessageSource` do núcleo 2.14.0 (`final`) que um `MessageConsumer` lê.

| Parâmetro | O quê |
| :--- | :--- |
| `group` | O grupo de consumidores: o nome do consumidor do núcleo (minúsculo, até 120 caracteres, como o `MessageConsumer` exige), o mesmo grupo em todo stream que ele lê. |
| `messageNames` | Os nomes cujos streams o grupo lê (pelo menos um; um nome repetido é lido uma vez). Um consumidor só lê o que lista. |
| `consumerName` | Este membro do grupo (`[A-Za-z0-9][A-Za-z0-9._:@-]{0,119}`); `null` = `<host>-<pid>`. Dê a cada worker um nome estável: reiniciado, ele pega na hora o que deixou sem resposta. |
| `topology` | As chaves e os limites, os mesmos do transporte. |

`getGroup()`, `getConsumerName()`, `getStreams()` (um stream por nome, na ordem dada) e `getTopology()` devolvem aquilo com que ela foi construída; `inFlightCount()`, as entregas feitas e ainda sem resposta.

| Método | O quê |
| :--- | :--- |
| `declare(bool $fromStart = false): void` | Cria o grupo em todo stream, e o stream quando falta (`XGROUP CREATE ... $ MKSTREAM`); um grupo que existe é mantido. Um grupo novo recebe o que for acrescentado dali em diante, ou, com `fromStart`, o que os streams ainda guardam. O primeiro `receive()` o declara; declare os consumidores antes de o relay enviar o que eles precisam receber. |
| `receive(int $max, int $waitMs): list<ReceivedMessage>` | No máximo `$max` entregas, na ordem global das escritas: as entregas de um membro morto (paradas há mais de `recoverIdleMs`), as que este membro devolveu ou deixou sem resposta, as entradas lidas e ainda não entregues e então entradas novas, bloqueando no máximo `min($waitMs, maxBlockMs)` quando não há mais nada. |
| `ack(ReceivedMessage $message): void` | `XACK`: ela não volta. |
| `reject(ReceivedMessage $message, bool $requeue, string $reason): void` | Com `$requeue`, a entrega continua pendente neste membro e volta primeiro; sem, uma carta morta em `<prefixo>.dead` e `XACK`, atomicamente. |
| `deadLetters(int $limit = 100): list<array>` | As cartas mortas deste grupo, das mais antigas para as mais novas: `brokerId`, `messageId` (`null` para uma entrega inválida), `reason`, `deliveryCount`, `deadLetterId`. |

- O **id do broker** de uma entrega é `<stream>#<id da entrada>`, o mesmo em toda entrega da entrada.
- `ack()` ou `reject()` de uma entrega que não está em voo nesta fonte é uma `MessagingException`, como na fonte em memória do núcleo. Uma falha da conexão, ou uma resposta de erro do Redis, é uma `RedisStreamsException` (uma `MessagingException`), cuja mensagem cita a operação, o stream e a causa; a entrega continua em voo, para poder ser respondida de novo.

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

// O processo morre com a segunda entrega em voo, e reinicia com o mesmo nome de membro:
$restarted = new RedisStreamsSource($redis, 'agents', $names, 'agents-1');
$again = $restarted->receive(10, 0);
$again[0]->message()->payload['lead_id']; // 10
$again[0]->deliveryCount; // 2
```

Os dois streams são juntados na ordem em que foram escritos, digam o que disserem os seus ids de entrada.
