# RedisStreamsTopology

## `new RedisStreamsTopology(string $prefix = 'aspec', ?int $maxLen = null, ?int $recoverIdleMs = 60000, int $maxBlockMs = 5000)`

Um valor compartilhado pelo transporte e por todas as fontes de uma aplicação, para que todos nomeiem as mesmas chaves (`final`, as suas quatro propriedades `readonly`).

| Parâmetro | Padrão | O quê |
| :--- | :--- | :--- |
| `prefix` | `aspec` | O início de toda chave: 1 a 64 caracteres de `[A-Za-z0-9_.:{}-]`, sem terminar em ponto. Um prefixo entre chaves é uma hash tag de cluster. |
| `maxLen` | `null` | O teto aproximado de cada stream de mensagem, `XADD ... MAXLEN ~ maxLen` (pelo menos 1); `null` = nunca aparado. O Redis apara nós inteiros, então um stream pode guardar um pouco mais. |
| `recoverIdleMs` | `60000` | Uma entrega pendente há mais do que isso num membro do grupo é reivindicada pelo próximo `receive()` de qualquer membro (`XAUTOCLAIM`); `null` = sem recuperação (um membro reiniciado com o mesmo nome continua pegando as suas). |
| `maxBlockMs` | `5000` | O máximo que um `receive()` bloqueia no `XREADGROUP ... BLOCK`, qualquer que seja a espera pedida (pelo menos 1); mantenha-o abaixo do tempo limite de leitura da conexão (`read_write_timeout` do predis). |

| Método | Devolve |
| :--- | :--- |
| `streamOf(string $messageName): string` | `<prefixo>.<nome>`; um nome que não é `módulo.fato.vN` é recusado (`InvalidArgumentException`). |
| `deadLetterStream(): string` | `<prefixo>.dead` |
| `sequenceKey(): string` | `<prefixo>.seq` |

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
