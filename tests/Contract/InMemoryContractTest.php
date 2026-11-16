<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Contract;

use Antevemus\ASpecificationRedisStreams\Tests\Support\BrokerFixture;
use Antevemus\ASpecificationRedisStreams\Tests\Support\InMemoryBroker;

/**
 * The contract battery against the reference of the core: InMemoryMessageTransport and
 * InMemoryMessageSource.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Contract
 */
final class InMemoryContractTest extends MessagingContractTestCase
{
    protected function broker(): BrokerFixture
    {
        return new InMemoryBroker();
    }
}
