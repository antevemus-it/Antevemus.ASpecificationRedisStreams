# Antevemus ASpecification Redis Streams

[![PHP](https://img.shields.io/badge/PHP-8.2%2B-777bb4.svg)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/actions/workflows/tests.yml/badge.svg)](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/actions/workflows/tests.yml)

[English](README.md)

**O satélite Redis Streams do gancho de mensageria da [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) para PHP 8.2+.** O `RedisStreamsTransport` é o broker para onde o `OutboxRelay` do núcleo envia (um stream por nome de mensagem, `XADD` numa única ida e volta por lote, confirmado pelo id da entrada); o `RedisStreamsSource` é o que o `MessageConsumer` do núcleo lê (um grupo de consumidores por consumidor, `XREADGROUP` com `BLOCK`, `XACK`). Pelo menos uma vez com a inbox do núcleo, a ordem de cada chave de ordem mantida entre os streams de vários nomes, uma entrega devolvida antes das posteriores da sua chave, um stream de fila morta, a recuperação do que um processo morto deixou pendente (`XAUTOCLAIM`), entregas inválidas nunca descartadas e nenhum payload em motivo algum. Nunca Pub/Sub. PHP puro sobre o predis: nenhuma extensão é exigida.

Instale com `composer require antevemus/aspecification-redis-streams`; ele precisa de um servidor Redis 7.0 ou posterior.

## Início Rápido

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

// O grupo de consumidores "agents" lê o stream de um nome de mensagem: declare-o antes de o relay rodar.
$source = new RedisStreamsSource($redis, 'agents', ['underwriting.quote_submitted.v1'], 'agents-1');
$source->declare();

// Publique na outbox (em produção, a PdoMessageOutbox dentro da transação da escrita)...
$outbox = new InMemoryMessageOutbox();
(new MessagePublisher($outbox))->publish(new Message(
    'underwriting.quote_submitted.v1',
    ['quote_id' => 123, 'premium' => 1250.0],
    orderingKey: 'quote:123',
    tenant: 'acme'
));

// ...e retransmita ao Redis: um XADD no stream aspec.underwriting.quote_submitted.v1.
$relay = new OutboxRelay($outbox, new RedisStreamsTransport($redis), maxAttempts: 10);
$relay->relayPending(100)->deliveredCount(); // 1

// Consuma uma vez por consumidor: a inbox absorve uma entrega que chega duas vezes.
$tasks = [];
$handlers = (new MessageHandlerRegistry())->register('underwriting.quote_submitted.v1', function (Message $message) use (&$tasks): void {
    $tasks[] = $message->payload['quote_id'];
});
$consumer = new MessageConsumer('agents', $source, $handlers, new InMemoryProcessedMessageStore());
count($consumer->consume(100, waitMs: 1000)->handledIds()); // 1
$tasks; // [123]
```

## Documentação

A documentação oficial fica em [docs/pt-BR](docs/pt-BR/index.md) (e em inglês, em [docs/en](docs/en/index.md)); todo bloco PHP dela roda na suíte de testes, contra um Redis real.

| Página | O que cobre |
| :--- | :--- |
| [Instalação e início rápido](docs/pt-BR/getting-started/index.md) | O Composer, os requisitos, o servidor Redis e o Início Rápido. |
| [Entrega, ordem e falhas](docs/pt-BR/concepts/delivery.md) | As chaves no Redis, pelo menos uma vez com a inbox, a ordem entre streams, as entregas devolvidas, a fila morta, a recuperação de um processo morto, as entregas inválidas e o sigilo dos motivos. |
| [RedisStreamsTopology](docs/pt-BR/api/topology.md) | O prefixo das chaves, o teto dos streams, o tempo ocioso da recuperação, o bloqueio mais longo. |
| [RedisStreamsTransport](docs/pt-BR/api/transport.md) | O gancho de saída: o que uma entrada guarda, o que é confirmado e o que falha. |
| [RedisStreamsSource](docs/pt-BR/api/source.md) | O gancho de entrada: o grupo e seus membros, `receive()`, `ack()`, `reject()`, `deadLetters()`. |
| [Testes](docs/pt-BR/quality/tests.md) | A bateria de contrato do núcleo ([Escrevendo um satélite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/pt-BR/messaging/writing-a-satellite.md)) rodada contra um Redis real, com a sua sequência de propriedade, os testes do próprio Redis e o teste de mutação. |
| [Roadmap](docs/pt-BR/project/roadmap.md) | O que foi entregue e o que pode vir. |

## Licença

MIT, veja [LICENSE](LICENSE). Procedência em [NOTICE.md](NOTICE.md); mudanças em [CHANGELOG.md](CHANGELOG.md).
