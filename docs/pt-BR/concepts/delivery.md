# Entrega, Ordem e Falhas

## As chaves no Redis

Toda chave do pacote começa com o prefixo da `RedisStreamsTopology` (padrão `aspec`):

| Chave | O quê |
| :--- | :--- |
| `<prefixo>.<nome da mensagem>` | Um stream por nome de mensagem (`aspec.crm.lead_created.v1`): o transporte acrescenta ali, e cada consumidor do núcleo o lê como um grupo de consumidores de mesmo nome. |
| `<prefixo>.dead` | A fila morta de todos os grupos de consumidores. |
| `<prefixo>.seq` | O contador da ordem global das escritas. |

Um stream de mensagem sempre termina em `.vN`, então nunca colide com os outros dois. Um prefixo entre chaves (`{aspec}`) é uma hash tag: mantém todas as chaves num único slot de um cluster. Nunca Pub/Sub: um stream guarda o que é publicado enquanto um consumidor está fora, e um grupo de consumidores lembra o que cada consumidor leu e confirmou.

## Pelo menos uma vez, depois uma vez por consumidor

Uma mensagem está confirmada quando o Redis respondeu o seu `XADD` com o id da nova entrada: só então o relay do núcleo a marca como entregue. Um relay que morre entre a resposta e a marcação a envia de novo na rodada seguinte, e um consumidor que morre entre o seu commit e o seu `XACK` a recebe de novo: a entrega é **pelo menos uma vez**. A inbox do núcleo (`IProcessedMessageStore`) faz disso **uma vez por consumidor**: uma mensagem que ela já tem é confirmada como `duplicate`, e o handler não roda de novo. Com um gerenciador de transação sobre a conexão do handler, a linha da inbox e o efeito confirmam juntos.

O que "gravado" quer dizer é a persistência do servidor: com `appendonly yes` e `appendfsync everysec`, uma queda do servidor perde no máximo cerca de um segundo de escritas confirmadas; com `appendfsync always`, nenhuma; sem persistência, tudo o que estava na memória. Configure o servidor para a garantia de que a aplicação precisa.

## A ordem de uma chave de ordem

As mensagens de uma chave de ordem chegam a um consumidor na ordem em que foram publicadas, também quando têm nomes diferentes e por isso vivem em streams diferentes (`quote.created.v1` e depois `quote.sent.v1` de `quote:123`):

- o relay do núcleo só envia uma mensagem de uma chave depois que a anterior foi confirmada;
- o transporte acrescenta cada entrada e incrementa `<prefixo>.seq` num passo atômico (um script Lua), e grava o valor no campo `seq` da entrada: a ordem global das escritas. Os ids de entrada de dois streams não servem para isso: são carimbos em milissegundos com uma sequência própria por stream;
- a fonte junta as entradas dos seus streams por `seq`, e lê cada stream o bastante (pelo menos tantas entradas quantas entrega, ou tudo o que ele tem) para que nenhuma entrada deixada para trás possa vir antes de uma que ela entrega.

A ordem vale para um consumidor por fonte, como em qualquer broker: dois membros de um grupo dividem as suas mensagens, e cada um mantém a ordem do que recebe.

## Uma entrega devolvida volta primeiro

O `MessageConsumer` rejeita com requeue uma entrega cujo handler falhou, e as entregas posteriores da mesma chave naquela rodada (`held_back`). O Redis Streams não tem requeue próprio, então a fonte mantém a entrega **pendente no seu membro** do grupo e, no `receive()` seguinte, a reivindica de novo (`XCLAIM`, que também conta a entrega) antes de qualquer coisa mais nova: ela é mais antiga do que tudo o que ainda não foi entregue, então volta antes de qualquer entrega posterior da sua chave.

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
$source->reject($first, true, 'the mail server is down'); // devolvida: continua pendente em billing-1

$again = $source->receive(10, 0);
$again[0]->message()->payload['invoice']; // 1
$again[0]->deliveryCount; // 2
$again[1]->message()->payload['invoice']; // 2
$again[1]->deliveryCount; // 1
```

O transporte é chamado direto para encurtar o exemplo; numa aplicação quem o chama é o relay. O número de entregas é o contador do Redis para a entrada no grupo (`XPENDING`): 1 na primeira leitura, mais um a cada reivindicação.

## Fila morta

O `reject($requeue = false)` acrescenta uma carta morta em `<prefixo>.dead` e confirma a original, num passo atômico. A carta morta guarda a origem (`stream`, `entry_id`), o `group`, o membro (`consumer`), o `delivery_count`, o `reason`, o `message_id`, `name`, `ordering_key` e `tenant` da mensagem quando a entrega era uma, e o `body` original, para que um operador possa inspecioná-la e publicá-la de novo. O `MessageConsumer` manda à fila morta uma entrega cujo handler recusa o payload (`InvalidMessagePayloadException`), cujo handler falhou na entrega `maxDeliveries`, cujo número já chega acima dele, e a entrega inválida.

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
    throw new InvalidMessagePayloadException('lead_id is required'); // tentar de novo não mudaria a resposta
});
$report = (new MessageConsumer('crm', $source, $handlers, new InMemoryProcessedMessageStore()))->consume(10);
$report->entries()[0]->outcome->value; // 'dead_lettered'

$dead = $source->deadLetters();
count($dead); // 1
$dead[0]['deliveryCount']; // 1
str_contains($dead[0]['reason'], 'Ana'); // false (um motivo nunca carrega o payload)
array_keys(current($redis->xrange('aspec.dead', '-', '+'))); // ['stream', 'entry_id', 'group', 'consumer', 'delivery_count', 'reason', 'message_id', 'name', 'body']
```

O `deadLetters()` varre o stream da fila morta atrás das cartas do grupo da fonte, das mais antigas para as mais novas. O pacote nunca apara o stream da fila morta: apague o que foi tratado com `XDEL` ou `XTRIM`.

## A recuperação de um processo morto

Uma entrega fica pendente no membro que a leu até ser confirmada. Um membro que morre deixa as suas entregas pendentes: todo `receive()` de qualquer membro do grupo reivindica (`XAUTOCLAIM`) as entregas pendentes há mais de `recoverIdleMs` (60 000 ms por padrão), percorrendo um pouco das entradas pendentes do grupo a cada chamada. Um membro reiniciado **com o mesmo nome** não espera: o seu primeiro `receive()` pega o que ele deixou sem resposta. Mantenha `recoverIdleMs` acima do tratamento mais longo de uma mensagem: uma entrega em voo por mais tempo do que isso conta como abandonada: outro membro pode reivindicá-la, e o seu número de entregas cresce.

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

// agents-1 morre aqui, antes de responder. Outro membro do grupo assume depois de 100 ms:
$second = new RedisStreamsSource($redis, 'agents', ['crm.lead_created.v1'], 'agents-2', $topology);
count($second->receive(10, 0)); // 0 (ainda não ficou parada 100 ms)
usleep(150_000);
$recovered = $second->receive(10, 0);
$recovered[0]->message()->payload['lead_id']; // 9
$recovered[0]->deliveryCount; // 2
```

## Entregas inválidas

O que a fonte não consegue ler de volta como mensagem é uma entrega inválida (`ReceivedMessage::invalid()`), nunca descartada: um corpo que não é a forma de fio de formato 1 do núcleo, uma entrada sem o campo `body` (outro produtor escreveu no stream) e uma entrada apagada enquanto pendente (aparada pelo `MAXLEN`, ou `XDEL`). O `MessageConsumer` a manda à fila morta; para uma entrada apagada a carta morta não tem corpo, e diz que a mensagem se perdeu. Dimensione o `maxLen` bem acima do atraso que um consumidor pode acumular.

## Sigilo

Nenhum motivo, exceção ou relatório do pacote carrega um valor do payload. Um motivo cita a mensagem (o nome, o id e a chave de ordem), o stream e a causa: o código de erro do Redis (`WRONGTYPE`, `NOPERM`, `OOM`) e nunca o resto do texto, que pode citar os argumentos do comando; a classe da exceção do cliente e, para uma conexão que falhou, a sua mensagem, que cita o servidor. O payload vive só nas entradas e nas cartas mortas, dado em repouso no broker.
