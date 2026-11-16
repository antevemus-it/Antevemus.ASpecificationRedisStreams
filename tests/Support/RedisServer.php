<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use PHPUnit\Framework\Assert;
use PHPUnit\Framework\SkippedWithMessageException;
use Predis\Client;
use Throwable;

/**
 * The Redis server of the tests that need one (a Redis 7.4 or later).
 *
 * Opt-in, so that the suite never writes to a server by surprise: the tests that need Redis run only when
 * ASPEC_REDIS_HOST is set (ASPEC_REDIS_PORT, default 6379; ASPEC_REDIS_DATABASE, default 15), and are
 * skipped with the reason otherwise, or when the server does not answer. With ASPEC_REDIS_REQUIRED=1 (the
 * CI) a server that does not answer fails them instead. Every test writes under a prefix of its own
 * (`t` + 8 hexadecimal digits) and deletes its keys at the end; nothing else of the database is touched.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class RedisServer
{
    public const HOST_ENV = 'ASPEC_REDIS_HOST';

    public const PORT_ENV = 'ASPEC_REDIS_PORT';

    public const DATABASE_ENV = 'ASPEC_REDIS_DATABASE';

    public const REQUIRED_ENV = 'ASPEC_REDIS_REQUIRED';

    private static bool $probed = false;

    private static ?string $unavailable = null;

    /** Why the tests that need Redis cannot run here; null when the server answers. */
    public static function unavailableReason(): ?string
    {
        if (!self::$probed) {
            self::$probed = true;
            if (getenv(self::HOST_ENV) === false || getenv(self::HOST_ENV) === '') {
                self::$unavailable = sprintf('Redis tests are opt-in: set %s (and %s, default 6379; %s, default 15) to a Redis 7.4 server for tests.', self::HOST_ENV, self::PORT_ENV, self::DATABASE_ENV);
            } else {
                try {
                    $info = self::client()->executeRaw(['INFO', 'server']);
                    preg_match('/redis_version:(\d+)\.(\d+)/', (string) $info, $version);
                    if ($version === [] || [(int) $version[1], (int) $version[2]] < [7, 0]) {
                        self::$unavailable = sprintf('The Redis server at %s is older than 7.0.', self::url());
                    }
                } catch (Throwable $e) {
                    self::$unavailable = sprintf('The Redis server at %s did not answer (%s).', self::url(), $e->getMessage());
                }
            }
        }

        return self::$unavailable;
    }

    /** Skips the running test (or fails it, with ASPEC_REDIS_REQUIRED=1) when Redis is unavailable. */
    public static function requireServer(): void
    {
        $reason = self::unavailableReason();
        if ($reason === null) {
            return;
        }
        if (getenv(self::REQUIRED_ENV) === '1' && getenv(self::HOST_ENV) !== false) {
            Assert::fail($reason);
        }
        throw new SkippedWithMessageException($reason);
    }

    /**
     * A new client of the test server.
     *
     * @param array<string, mixed> $parameters More connection parameters (a user, a timeout)
     */
    public static function client(array $parameters = []): Client
    {
        return new Client($parameters + [
            'scheme' => 'tcp',
            'host' => self::host(),
            'port' => self::port(),
            'database' => self::database(),
            'read_write_timeout' => 10,
        ]);
    }

    /** The address of the test server, as a predis URL. */
    public static function url(): string
    {
        return sprintf('tcp://%s:%d?database=%d', self::host(), self::port(), self::database());
    }

    /** A prefix of keys of its own. */
    public static function prefix(): string
    {
        return 't' . bin2hex(random_bytes(4));
    }

    /** Deletes every key under the prefix. */
    public static function cleanup(string $prefix): void
    {
        if (self::unavailableReason() !== null) {
            return;
        }
        $client = self::client();
        $cursor = '0';
        do {
            [$cursor, $keys] = $client->executeRaw(['SCAN', $cursor, 'MATCH', $prefix . '.*', 'COUNT', '1000']);
            if ($keys !== []) {
                $client->executeRaw(['DEL', ...$keys]);
            }
        } while ($cursor !== '0');
        $client->disconnect();
    }

    private static function host(): string
    {
        return (string) getenv(self::HOST_ENV);
    }

    private static function port(): int
    {
        $port = getenv(self::PORT_ENV);

        return $port === false || $port === '' ? 6379 : (int) $port;
    }

    private static function database(): int
    {
        $database = getenv(self::DATABASE_ENV);

        return $database === false || $database === '' ? 15 : (int) $database;
    }
}
