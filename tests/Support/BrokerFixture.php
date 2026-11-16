<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;

/**
 * One broker under the contract battery: the in-memory reference of the core, or Redis Streams.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
interface BrokerFixture
{
    /** The transport the relay sends to. */
    public function transport(): IMessageTransport;

    /** A transport whose every batch fails as a lost connection does. */
    public function brokenTransport(): IMessageTransport;

    /**
     * A source of a consumer (declared: it gets every message relayed from now on).
     *
     * @param list<string> $names
     */
    public function source(string $group, array $names, string $member = 'm1'): IMessageSource;

    /** Stores a body that is not written by the transport (another producer) on the stream of the name. */
    public function storeRaw(string $name, string $body): void;

    /**
     * The dead letters of the consumer of a source, oldest first.
     *
     * @return list<array{messageId: ?string, reason: string, deliveryCount: int}>
     */
    public function deadLetters(IMessageSource $source): array;

    /**
     * The member of the source died with deliveries in flight: the source of another member of the group
     * that gets them again (by idle recovery, in Redis).
     *
     * @param list<string> $names
     */
    public function abandon(IMessageSource $source, string $group, array $names): IMessageSource;

    /**
     * The process of the source died: the source of the same member, started again.
     *
     * @param list<string> $names
     */
    public function restart(IMessageSource $source, string $group, array $names): IMessageSource;

    /** Releases what the fixture holds (the keys of Redis). */
    public function close(): void;
}
