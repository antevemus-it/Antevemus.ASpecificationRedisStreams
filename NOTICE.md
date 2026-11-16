Antevemus ASpecification Redis Streams
Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
License: MIT (SPDX: MIT) — see LICENSE.

Website: https://antevemus.com.br
Email: contato@antevemus.com.br

-------------------------------------------------------------------------------
1. SCOPE & PURPOSE
-------------------------------------------------------------------------------

Antevemus ASpecification Redis Streams is the Redis Streams satellite of the
messaging hook of Antevemus ASpecification (antevemus/aspecification 2.14.0)
for PHP 8.2+. RedisStreamsTransport implements the IMessageTransport of the
core over Redis Streams (XADD, one stream per message name), and
RedisStreamsSource implements its IMessageSource (consumer groups, XREADGROUP,
XACK, XCLAIM, XAUTOCLAIM and a dead letter stream).

-------------------------------------------------------------------------------
2. PROVENANCE — NOTHING PORTED
-------------------------------------------------------------------------------

No source code of any third-party project was copied, ported or translated
into this package. It is an independent implementation, written by Antevemus
Soluções Inovadoras em TI Ltda. on the public API of its runtime dependencies
(section 4) and on the public documentation of the Redis commands it sends
(XADD, XGROUP, XREADGROUP, XPENDING, XCLAIM, XAUTOCLAIM, XACK, XRANGE, EVAL);
the only code it did not write for this package is first-party code of the
same author, copied on purpose (section 3).

-------------------------------------------------------------------------------
3. FIRST-PARTY CODE COPIED ON PURPOSE
-------------------------------------------------------------------------------

The documentation guards tests/Unit/DocumentationTreeTest.php and
tests/Unit/DocumentationBlocksTest.php are copied from Antevemus ASpecification
BR (antevemus/aspecification-br 1.1.0), itself copied from Antevemus
ASpecification Revolt (antevemus/aspecification-revolt 1.0.0), by the same
author and under the same MIT License, adapted to the pages of this package
and to the blocks that talk to a Redis server.

    Origin: DocumentationTreeTest, DocumentationBlocksTest (antevemus/aspecification-br 1.1.0)
    Originator: Antevemus Soluções Inovadoras em TI Ltda.
    License: MIT License
    Website: https://github.com/antevemus-it/Antevemus.ASpecificationBr

-------------------------------------------------------------------------------
4. THIRD-PARTY COMPONENTS & LICENSES
-------------------------------------------------------------------------------

Runtime: antevemus/aspecification (MIT, by the same author) and predis/predis
(MIT, Copyright (c) 2009-2020 Daniele Alessandri, (c) 2021-2024 Till Krüss),
a Redis client in pure PHP; neither is distributed with the package, Composer
installs them. Development only: phpunit/phpunit (BSD-3-Clause) and
infection/infection (BSD-3-Clause), with their own dependencies. The tests and
the CI run against the official Redis image (redis:7.4-alpine; Redis is
licensed by Redis Ltd.), which is neither distributed nor required by the
package: any Redis 7.0 or later server works.
