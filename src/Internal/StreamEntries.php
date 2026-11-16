<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Internal;

/**
 * StreamEntries - The Pure Rules of RedisStreamsSource Over the Entries of a Stream
 *
 * - fields(): the fields of an entry from the flat list Redis answers (`[field, value, field, ...]`).
 * - compare(): the order in which the candidates of a receive() are handed out: the global order of
 *   the writes of RedisStreamsTransport (the field `seq`; an entry without a valid one, written by
 *   another producer or deleted, first), then the stream, then the entry id (milliseconds, then
 *   sequence, both as numbers).
 * - brokerId(): `<stream>#<entry id>`.
 * - consumerName(): the default member of a group, `<host>-<pid>`, the host reduced to
 *   `[A-Za-z0-9._-]` and cut to 120 characters.
 *
 * @internal
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @subpackage Internal
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class StreamEntries
{
    /** Separator of the stream and the entry id in a broker id. */
    public const BROKER_ID_SEPARATOR = '#';

    /** Longest member name of a group. */
    public const MAX_CONSUMER_NAME_LENGTH = 120;

    private function __construct()
    {
    }

    /**
     * The fields of an entry, from the flat list of Redis (a trailing field without value is left out).
     *
     * @return array<string, string>
     */
    public static function fields(mixed $flat): array
    {
        $fields = [];
        foreach (array_chunk(is_array($flat) ? $flat : [], 2) as $pair) {
            if (count($pair) === 2) {
                $fields[$pair[0]] = $pair[1];
            }
        }

        return $fields;
    }

    /**
     * The order of two candidates (see the class description).
     *
     * @param array{stream: string, id: string, fields: array<string, string>|null} $a
     * @param array{stream: string, id: string, fields: array<string, string>|null} $b
     */
    public static function compare(array $a, array $b): int
    {
        return [self::sequence($a['fields']), $a['stream'], ...self::idParts($a['id'])]
            <=> [self::sequence($b['fields']), $b['stream'], ...self::idParts($b['id'])];
    }

    /**
     * The global order of an entry: its field `seq`, or 0 without a valid one.
     *
     * @param array<string, string>|null $fields
     */
    public static function sequence(?array $fields): int
    {
        $seq = $fields['seq'] ?? '';

        return ctype_digit($seq) ? (int) $seq : 0;
    }

    /**
     * The two numbers of an entry id (`<milliseconds>-<sequence>`).
     *
     * @return array{int, int}
     */
    public static function idParts(string $id): array
    {
        [$milliseconds, $sequence] = explode('-', $id);

        return [(int) $milliseconds, (int) $sequence];
    }

    /** The broker id of an entry. */
    public static function brokerId(string $stream, string $id): string
    {
        return $stream . self::BROKER_ID_SEPARATOR . $id;
    }

    /** The default member name for a host and a process. */
    public static function consumerName(string $host, int $pid): string
    {
        $host = trim((string) preg_replace('/[^A-Za-z0-9._-]+/', '-', $host), '._-');

        return substr(($host === '' ? 'host' : $host) . '-' . $pid, 0, self::MAX_CONSUMER_NAME_LENGTH);
    }
}
