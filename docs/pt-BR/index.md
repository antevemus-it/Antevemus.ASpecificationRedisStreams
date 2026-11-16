# Antevemus ASpecification Redis Streams

A documentação oficial do pacote, uma página por assunto. As mesmas páginas existem em inglês, em `docs/en`, e todo bloco PHP destas páginas é executado pela suíte de testes com as respostas escritas nos seus comentários.

> **O satélite Redis Streams do gancho de mensageria da [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) para PHP 8.2+**  
> O `RedisStreamsTransport` é o broker para onde o `OutboxRelay` do núcleo envia, um stream por nome de mensagem; o `RedisStreamsSource` é o que um `MessageConsumer` lê, um grupo de consumidores por consumidor. Pelo menos uma vez, a ordem de cada chave de ordem mantida entre os streams de vários nomes, uma entrega devolvida antes das posteriores da sua chave, um stream de fila morta, a recuperação do que um processo morto deixou pendente e nenhum payload em motivo algum. PHP puro sobre o predis: nenhuma extensão é exigida.

## Mapa da Documentação

### Primeiros passos

- [Instalação e início rápido](getting-started/index.md): o Composer, os requisitos, o servidor Redis e o Início Rápido.

### Conceitos

- [Entrega, ordem e falhas](concepts/delivery.md): as chaves no Redis, pelo menos uma vez com a inbox, a ordem entre streams, as entregas devolvidas, a fila morta, a recuperação de um processo morto, as entregas inválidas e o sigilo dos motivos.

### Referência da API

- [RedisStreamsTopology](api/topology.md): o prefixo das chaves, o teto dos streams, o tempo ocioso da recuperação e o bloqueio mais longo.
- [RedisStreamsTransport](api/transport.md): o gancho de saída, o que uma entrada guarda, o que é confirmado e o que falha.
- [RedisStreamsSource](api/source.md): o gancho de entrada, o grupo e seus membros, `receive()`, `ack()`, `reject()` e `deadLetters()`.

### Qualidade

- [Testes](quality/tests.md): a bateria de contrato rodada contra o broker em memória do núcleo e um Redis real, os cenários de falha, a propriedade sobre intercalações aleatórias e o teste de mutação.

### Projeto

- [Roadmap](project/roadmap.md): a versão entregue e o que pode vir depois.

## Onde Está Cada Coisa

| O quê | Onde |
| :--- | :--- |
| Visão geral e o Início Rápido | [README.pt-BR.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/README.pt-BR.md) |
| O que mudou em cada versão | [CHANGELOG.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/CHANGELOG.md) |
| Marcos entregues e planejados | [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/ROADMAP.md) |
| Licença e procedência | [LICENSE](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/LICENSE), [NOTICE.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/NOTICE.md) |
| O gancho de mensageria do núcleo | [Mensageria entre módulos](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/pt-BR/messaging/index.md) |
