<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use PHPUnit\Framework\Assert;
use Predis\Client;

/**
 * Redis Streams under the contract battery: the transport and the sources of this package against the
 * test server (RedisServer), under a prefix of keys of its own.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class RedisStreamsBroker implements BrokerFixture
{
    /** The idle time after which the member of abandon() recovers the deliveries of a dead one. */
    public const RECOVER_IDLE_MS = 40;

    public readonly Client $redis;

    public readonly RedisStreamsTopology $topology;

    public function __construct(?int $maxLen = null)
    {
        RedisServer::requireServer();
        $this->redis = RedisServer::client();
        $this->topology = new RedisStreamsTopology(prefix: RedisServer::prefix(), maxLen: $maxLen);
    }

    public function transport(): IMessageTransport
    {
        return new RedisStreamsTransport($this->redis, $this->topology);
    }

    public function brokenTransport(): IMessageTransport
    {
        return new RedisStreamsTransport(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]), $this->topology);
    }

    public function source(string $group, array $names, string $member = 'm1', ?RedisStreamsTopology $topology = null): IMessageSource
    {
        $source = new RedisStreamsSource($this->redis, $group, $names, $member, $topology ?? $this->topology);
        $source->declare();

        return $source;
    }

    public function storeRaw(string $name, string $body): void
    {
        $this->redis->executeRaw(['XADD', $this->topology->streamOf($name), '*', 'body', $body]);
    }

    public function deadLetters(IMessageSource $source): array
    {
        Assert::assertInstanceOf(RedisStreamsSource::class, $source);

        return array_map(
            static fn(array $dead): array => ['messageId' => $dead['messageId'], 'reason' => $dead['reason'], 'deliveryCount' => $dead['deliveryCount']],
            $source->deadLetters(1000)
        );
    }

    public function abandon(IMessageSource $source, string $group, array $names): IMessageSource
    {
        Assert::assertInstanceOf(RedisStreamsSource::class, $source);
        usleep((self::RECOVER_IDLE_MS + 30) * 1000);

        return $this->source($group, $names, $source->getConsumerName() . '-next', new RedisStreamsTopology(
            prefix: $this->topology->prefix,
            maxLen: $this->topology->maxLen,
            recoverIdleMs: self::RECOVER_IDLE_MS
        ));
    }

    public function restart(IMessageSource $source, string $group, array $names): IMessageSource
    {
        Assert::assertInstanceOf(RedisStreamsSource::class, $source);

        return $this->source($group, $names, $source->getConsumerName());
    }

    public function close(): void
    {
        RedisServer::cleanup($this->topology->prefix);
        $this->redis->disconnect();
    }
}
