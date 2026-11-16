<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Contract;

use Antevemus\ASpecificationRedisStreams\Tests\Support\BrokerFixture;
use Antevemus\ASpecificationRedisStreams\Tests\Support\RedisStreamsBroker;
use PHPUnit\Framework\Attributes\Group;

/**
 * The contract battery against Redis Streams on a real server (skipped without one: see RedisServer):
 * RedisStreamsTransport and RedisStreamsSource, the same scenarios and the same transcripts as the
 * reference of the core.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Contract
 */
#[Group('redis')]
final class RedisStreamsContractTest extends MessagingContractTestCase
{
    protected function broker(): BrokerFixture
    {
        return new RedisStreamsBroker();
    }
}
