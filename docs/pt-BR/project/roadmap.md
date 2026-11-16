# Roadmap

## Entregue

- **1.0.0:** o `RedisStreamsTransport` (o gancho de saída: um stream por nome de mensagem, a ordem global das escritas, o teto aproximado, uma mensagem recusada falhando sozinha e uma conexão perdida fazendo falhar o lote) e o `RedisStreamsSource` (o gancho de entrada: um grupo de consumidores por consumidor, a junção dos streams na ordem escrita, uma entrega devolvida antes das posteriores da sua chave, o stream da fila morta, a recuperação de um processo morto, entregas inválidas nunca descartadas), com a `RedisStreamsTopology`, para o gancho de mensageria da ASpecification 2.14.0.

## Sob demanda

Não planejado; cada item vem quando um consumidor precisar:

- [ ] Redis Cluster testado na CI (hoje as chaves de um prefixo com hash tag ficam num único slot, sem teste).
- [ ] Uma ferramenta de linha de comando para a fila morta: listar, publicar de novo, apagar.
- [ ] Um grupo de consumidores novo criado a partir de um id de entrada dado, além do fim e do começo dos streams.
- [ ] Uma poda pronta do stream da fila morta por idade.

A visão por marcos está no [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRedisStreams/blob/main/ROADMAP.md).
