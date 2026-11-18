<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Contract;

use Antevemus\ASpecification\Messaging\Testing\IMessagingBench;
use Antevemus\ASpecification\Messaging\Testing\MessagingContractTestCase;
use Antevemus\ASpecificationRedisStreams\Tests\Support\RedisStreamsBench;
use PHPUnit\Framework\Attributes\Group;

/**
 * The contract battery of the core (MessagingContractTestCase of antevemus/aspecification 2.15) against
 * Redis Streams on a real server (skipped without one: see RedisServer): RedisStreamsTransport and
 * RedisStreamsSource, every scenario of the hook and the property sequence of its six seeds. The core runs
 * the same battery against its in-memory reference; what is specific to Redis is tested in Integration/.
 *
 * @version    1.0.1
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Contract
 */
#[Group('redis')]
final class RedisStreamsContractTest extends MessagingContractTestCase
{
    protected function createBench(): IMessagingBench
    {
        return new RedisStreamsBench();
    }
}
