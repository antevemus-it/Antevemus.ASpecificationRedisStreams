<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams;

use Antevemus\ASpecification\Messaging\Message;
use InvalidArgumentException;

/**
 * RedisStreamsTopology - Where the Messages Live in Redis and How Long a Delivery May Wait
 *
 * One value shared by the transport and the sources of an application, so that both name the same
 * keys:
 * - `prefix` (default `aspec`): every key of the package starts with it. A message of name
 *   `crm.lead_created.v1` goes to the stream `<prefix>.crm.lead_created.v1` (one stream per message
 *   name); the dead letters of every consumer go to the stream `<prefix>.dead`; the global order of
 *   the writes is the counter `<prefix>.seq`. A stream of a message always ends in `.vN`, so it never
 *   collides with the other two. 1 to 64 characters of `[A-Za-z0-9_.:{}-]`, not ending with a dot; a
 *   prefix in braces (`{aspec}`) is a hash tag, which keeps every key in one slot of a cluster.
 * - `maxLen` (default null = never trimmed): the approximate cap of each message stream
 *   (`XADD ... MAXLEN ~ maxLen`). Redis trims whole nodes, so a stream may hold somewhat more. An entry
 *   trimmed before every consumer group acknowledged it is lost for the groups that had not read it,
 *   and reported as an invalid delivery to a group that had it pending; size it well above the
 *   backlog a consumer may fall behind.
 * - `recoverIdleMs` (default 60 000; null = no recovery): a delivery pending for longer than this in a
 *   consumer of the group (a process that died, or one still handling it) is claimed (`XAUTOCLAIM`) by
 *   the next receive() of any source of the group. Keep it above the longest handling of a message.
 * - `maxBlockMs` (default 5 000): the longest a receive() blocks in `XREADGROUP ... BLOCK`, whatever
 *   wait it is given; keep it below the read timeout of the Redis connection.
 *
 * <code>
 * $topology = new RedisStreamsTopology(prefix: 'shop', maxLen: 1_000_000, recoverIdleMs: 30_000);
 * $topology->streamOf('crm.lead_created.v1'); // 'shop.crm.lead_created.v1'
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RedisStreamsTopology
{
    /** The default prefix of every key. */
    public const DEFAULT_PREFIX = 'aspec';

    /** The default idle time after which a pending delivery is recovered, in milliseconds. */
    public const DEFAULT_RECOVER_IDLE_MS = 60000;

    /** The default longest block of one receive(), in milliseconds. */
    public const DEFAULT_MAX_BLOCK_MS = 5000;

    /** A prefix: 1 to 64 characters, not ending with a dot. */
    public const PREFIX_PATTERN = '/^[A-Za-z0-9_.:{}-]{0,63}[A-Za-z0-9_:{}-]$/D';

    /** Suffix of the dead letter stream. */
    public const DEAD_LETTER_SUFFIX = '.dead';

    /** Suffix of the counter of the global order. */
    public const SEQUENCE_SUFFIX = '.seq';

    /**
     * @param string $prefix Prefix of every key
     * @param int|null $maxLen Approximate cap of each message stream (null = never trimmed)
     * @param int|null $recoverIdleMs Idle time after which a pending delivery is recovered (null = never)
     * @param int $maxBlockMs Longest block of one receive(), in milliseconds
     * @throws InvalidArgumentException When a value is out of its range
     */
    public function __construct(
        public readonly string $prefix = self::DEFAULT_PREFIX,
        public readonly ?int $maxLen = null,
        public readonly ?int $recoverIdleMs = self::DEFAULT_RECOVER_IDLE_MS,
        public readonly int $maxBlockMs = self::DEFAULT_MAX_BLOCK_MS
    ) {
        if (preg_match(self::PREFIX_PATTERN, $prefix) !== 1) {
            throw new InvalidArgumentException('The prefix must have 1 to 64 characters of [A-Za-z0-9_.:{}-] and not end with a dot.');
        }
        if ($maxLen !== null && $maxLen < 1) {
            throw new InvalidArgumentException(sprintf('maxLen must be null or at least 1, %d given.', $maxLen));
        }
        if ($recoverIdleMs !== null && $recoverIdleMs < 1) {
            throw new InvalidArgumentException(sprintf('recoverIdleMs must be null or at least 1, %d given.', $recoverIdleMs));
        }
        if ($maxBlockMs < 1) {
            throw new InvalidArgumentException(sprintf('maxBlockMs must be at least 1, %d given.', $maxBlockMs));
        }
    }

    /**
     * The stream of the messages of one name.
     *
     * @throws InvalidArgumentException When the name is not a valid message name (`module.fact.vN`)
     */
    public function streamOf(string $messageName): string
    {
        if (!Message::isValidName($messageName)) {
            throw new InvalidArgumentException('A stream is named after a message name: versioned and lowercase, module.fact.vN, at most 120 characters.');
        }

        return $this->prefix . '.' . $messageName;
    }

    /** The stream of the dead letters of every consumer group. */
    public function deadLetterStream(): string
    {
        return $this->prefix . self::DEAD_LETTER_SUFFIX;
    }

    /** The counter that orders every write of the transport across the streams. */
    public function sequenceKey(): string
    {
        return $this->prefix . self::SEQUENCE_SUFFIX;
    }
}
