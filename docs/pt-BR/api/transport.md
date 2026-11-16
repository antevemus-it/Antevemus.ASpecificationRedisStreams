# RedisStreamsTransport

## `new RedisStreamsTransport(Predis\ClientInterface $redis, RedisStreamsTopology $topology = new RedisStreamsTopology())`

O `IMessageTransport` do núcleo 2.14.0 (`final`): o `OutboxRelay` entrega a ele cada lote. `getTopology()` devolve a topologia em que ele escreve.

### `send(array $batch): TransportSendResult`

- Cada mensagem do lote é acrescentada ao stream do seu nome, `<prefixo>.<nome>` (com `MAXLEN ~` quando a topologia põe teto nos streams), por um script Lua que também incrementa `<prefixo>.seq`; o lote inteiro vai numa única ida e volta.
- **Confirmada** = o Redis respondeu com o id da nova entrada, que é o id do broker no resultado (`brokerIdOf()`).
- A entrada guarda, nesta ordem: `seq` (a ordem global das escritas), `message_id`, `name`, `ordering_key` e `tenant` (só quando a mensagem os tem) e `body`, a forma de fio do núcleo (`Message::toJson()`, que também leva o tenant e os headers). Nenhum campo além do `body` guarda um valor do payload.
- **Uma mensagem que o Redis recusou** (a chave guarda outro tipo, o servidor está sem memória, o usuário não pode rodar scripts) falha sozinha, com um motivo que cita a mensagem, o stream e o código de erro do Redis; as outras do lote continuam confirmadas.
- **Uma conexão que falha**, antes ou durante o lote, faz falhar todas as mensagens do lote com um único motivo. O que o Redis gravou antes de a conexão cair é enviado de novo pelo relay, e a inbox dos consumidores absorve a cópia.
- Um lote vazio não envia nada.

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

Uma mensagem recusada fica, por exemplo, `Redis refused message "crm.lead_created.v1" (id 01JCZ0000000000000000000AB, ordering key "lead:9") on stream "aspec.crm.lead_created.v1" (WRONGTYPE).`

O cliente é qualquer `Predis\ClientInterface` (um servidor único, ou uma replicação ou um Sentinel, cujo master o predis escolhe); o transporte envia comandos crus pela conexão dele, qualquer que seja a sua opção `exceptions`. Um Redis Cluster não é testado na 1.0.0: com um prefixo com hash tag, todas as chaves do pacote ficam num único slot.
