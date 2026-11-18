<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Testing\IMessagingBench;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use LogicException;
use Predis\Client;

/**
 * Redis Streams under the contract battery of the core (IMessagingBench of antevemus/aspecification 2.15):
 * the transport and the sources of this package against the test server (RedisServer), under a prefix of
 * keys of its own per bench, deleted by close().
 *
 * - source() is a RedisStreamsSource of the group `$consumer` and the member `$member`, declared at once
 *   (`XGROUP CREATE ... $ MKSTREAM`), so that it gets every message sent after it returns;
 * - successor() waits past an idle time of RECOVER_IDLE_MS and returns another member of the same group,
 *   whose topology recovers what is pending for longer than that (`XAUTOCLAIM`): the deliveries the dead
 *   member held, read and not handed out, or gave back;
 * - restarted() returns a new source of the same member (and the same topology), which takes its pending
 *   deliveries at once (`XPENDING` and `XCLAIM`);
 * - transportWithLostConnection() is a RedisStreamsTransport over a client of a port where nothing
 *   listens: the connection fails, the whole batch fails with one reason;
 * - storeRaw() appends an entry of another producer (`XADD <stream> * body <body>`, no field of the
 *   transport); deadLetters() reads the dead letter stream of the group (RedisStreamsSource::deadLetters()).
 *
 * @version    1.0.1
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class RedisStreamsBench implements IMessagingBench
{
    /** The idle time after which the member of successor() recovers the deliveries of a dead one. */
    public const RECOVER_IDLE_MS = 40;

    public readonly Client $redis;

    public readonly RedisStreamsTopology $topology;

    /** @var array<int, array{source: RedisStreamsSource, names: list<string>, root: string}> object id => a source of this bench and the member of the first source of its line */
    private array $sources = [];

    private int $successors = 0;

    private bool $closed = false;

    public function __construct()
    {
        RedisServer::requireServer();
        $this->redis = RedisServer::client();
        $this->topology = new RedisStreamsTopology(prefix: RedisServer::prefix());
    }

    public function transport(): IMessageTransport
    {
        return new RedisStreamsTransport($this->redis, $this->topology);
    }

    public function transportWithLostConnection(): IMessageTransport
    {
        return new RedisStreamsTransport(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]), $this->topology);
    }

    public function source(string $consumer, array $names, string $member = 'm1'): IMessageSource
    {
        return $this->open($consumer, $names, $member, $member, $this->topology);
    }

    public function successor(IMessageSource $source): IMessageSource
    {
        $own = $this->own($source);
        usleep((self::RECOVER_IDLE_MS + 30) * 1000);

        return $this->open($own['source']->getGroup(), $own['names'], sprintf('%s-next%d', $own['root'], ++$this->successors), $own['root'], new RedisStreamsTopology(
            prefix: $this->topology->prefix,
            maxLen: $this->topology->maxLen,
            recoverIdleMs: self::RECOVER_IDLE_MS
        ));
    }

    public function restarted(IMessageSource $source): IMessageSource
    {
        $own = $this->own($source);

        return $this->open($own['source']->getGroup(), $own['names'], $own['source']->getConsumerName(), $own['root'], $own['source']->getTopology());
    }

    public function storeRaw(string $name, string $body): void
    {
        $this->redis->executeRaw(['XADD', $this->topology->streamOf($name), '*', 'body', $body]);
    }

    public function deadLetters(IMessageSource $source): array
    {
        return array_map(
            static fn(array $dead): array => ['messageId' => $dead['messageId'], 'reason' => $dead['reason'], 'deliveryCount' => $dead['deliveryCount']],
            $this->own($source)['source']->deadLetters(PHP_INT_MAX)
        );
    }

    public function receiveWaitMs(): int
    {
        return 0; // an entry is readable as soon as XADD answered
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->sources = [];
        RedisServer::cleanup($this->topology->prefix);
        $this->redis->disconnect();
    }

    /**
     * @param list<string> $names
     * @param string $root The member of the first source of the line (a successor is named after it)
     */
    private function open(string $consumer, array $names, string $member, string $root, RedisStreamsTopology $topology): RedisStreamsSource
    {
        $source = new RedisStreamsSource($this->redis, $consumer, $names, $member, $topology);
        $source->declare();
        $this->sources[spl_object_id($source)] = ['source' => $source, 'names' => $names, 'root' => $root];

        return $source;
    }

    /**
     * @return array{source: RedisStreamsSource, names: list<string>, root: string}
     * @throws LogicException When the source was not created by this bench
     */
    private function own(IMessageSource $source): array
    {
        return $this->sources[spl_object_id($source)] ?? throw new LogicException(sprintf('The source (%s) was not created by this bench.', $source::class));
    }
}
