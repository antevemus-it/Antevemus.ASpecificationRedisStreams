<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use PHPUnit\Framework\TestCase;
use Predis\Client;

/**
 * A test against the Redis server of the tests (RedisServer): skipped without one, its keys under a
 * prefix of its own, deleted at the end.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
abstract class RedisTestCase extends TestCase
{
    protected Client $redis;

    protected string $prefix;

    protected RedisStreamsTopology $topology;

    /** @var list<string> ACL users created by the test */
    private array $users = [];

    protected function setUp(): void
    {
        RedisServer::requireServer();
        $this->redis = RedisServer::client();
        $this->prefix = RedisServer::prefix();
        $this->topology = new RedisStreamsTopology(prefix: $this->prefix);
    }

    protected function tearDown(): void
    {
        if (!isset($this->redis)) {
            return;
        }
        foreach ($this->users as $user) {
            $this->redis->executeRaw(['ACL', 'DELUSER', $user]);
        }
        RedisServer::cleanup($this->prefix);
        $this->redis->disconnect();
    }

    /**
     * A topology of the prefix of the test.
     */
    protected function topology(?int $maxLen = null, ?int $recoverIdleMs = RedisStreamsTopology::DEFAULT_RECOVER_IDLE_MS, int $maxBlockMs = RedisStreamsTopology::DEFAULT_MAX_BLOCK_MS): RedisStreamsTopology
    {
        return new RedisStreamsTopology($this->prefix, $maxLen, $recoverIdleMs, $maxBlockMs);
    }

    /**
     * The entries of a stream: entry id => fields.
     *
     * @return array<string, array<string, string>>
     */
    protected function entries(string $stream): array
    {
        $entries = [];
        foreach ($this->redis->executeRaw(['XRANGE', $stream, '-', '+']) as [$id, $flat]) {
            $fields = [];
            for ($i = 0; $i < count($flat); $i += 2) {
                $fields[$flat[$i]] = $flat[$i + 1];
            }
            $entries[$id] = $fields;
        }

        return $entries;
    }

    /**
     * The pending entries of a group: entry id => [member, delivery count].
     *
     * @return array<string, array{string, int}>
     */
    protected function pending(string $stream, string $group): array
    {
        $pending = [];
        foreach ($this->redis->executeRaw(['XPENDING', $stream, $group, '-', '+', '1000']) as [$id, $member, , $count]) {
            $pending[$id] = [$member, (int) $count];
        }

        return $pending;
    }

    /**
     * A client of the server logged in as a user of its own, with every command but the ones refused.
     *
     * @param list<string> $refused Commands the user may not run (lowercase)
     */
    protected function clientWithout(array $refused): Client
    {
        $user = $this->prefix . '-user-' . count($this->users);
        $this->users[] = $user;
        $this->redis->executeRaw(['ACL', 'SETUSER', $user, 'reset', 'on', 'nopass', '~*', '&*', '+@all', ...array_map(static fn(string $command): string => '-' . $command, $refused)]);
        return RedisServer::client(['username' => $user, 'password' => 'any']);
    }
}
