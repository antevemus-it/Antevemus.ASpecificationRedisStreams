# Primeiros Passos

## 📦 Instalação

Instale pelo Composer:

```bash
composer require antevemus/aspecification-redis-streams
```

Requisitos: PHP 8.2 ou posterior e um servidor **Redis 7.0 ou posterior** (testado no 7.4). Instalados junto: [`antevemus/aspecification`](https://github.com/antevemus-it/Antevemus.ASpecification) `^2.15` (o núcleo, do mesmo autor, em cujo gancho de mensageria este pacote se encaixa) e [`predis/predis`](https://github.com/predis/predis) `^2` (um cliente Redis em PHP puro). Nenhuma extensão do PHP é exigida: a `ext-redis` não é usada.

Para que serve: levar as mensagens do gancho de mensageria do núcleo entre os módulos de uma aplicação pelo Redis Streams. A aplicação continua publicando na outbox do núcleo, dentro da sua própria transação; o `OutboxRelay` do núcleo envia a outbox ao Redis pelo `RedisStreamsTransport`, e cada `MessageConsumer` do núcleo lê o seu próprio grupo de consumidores pelo `RedisStreamsSource`. Nada aqui usa o Pub/Sub do Redis, que perde o que é publicado enquanto um consumidor está fora.

## 🚀 Início Rápido

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

A outbox e a inbox são as em memória do núcleo, para o exemplo ficar de pé sozinho; numa aplicação elas são a `PdoMessageOutbox` e o `PdoProcessedMessageStore`, sobre as tabelas do `resources/sql/messaging.ansi.sql` do núcleo, com um gerenciador de transação para a linha da inbox e o efeito do handler confirmarem juntos. O nome do consumidor do núcleo (`agents`) também é o nome do grupo de consumidores, e `agents-1` é este processo no grupo: dê a cada worker um nome estável, para que um worker reiniciado pegue na hora o que deixou sem resposta.

Próximo: [Entrega, ordem e falhas](../concepts/delivery.md).
