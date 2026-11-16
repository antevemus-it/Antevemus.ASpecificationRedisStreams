<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecificationRedisStreams\Internal\RedisCommands;
use Throwable;

/**
 * RedisStreamsException - Redis Could Not Be Reached, or Refused a Command of the Source
 *
 * Thrown by RedisStreamsSource (a MessagingException of the core, so a consumer that catches those
 * catches it too) when the connection fails or Redis answers a command with an error. Its message
 * names the operation, the stream and the cause: the class of the exception of the client (and, for a
 * failed connection, its message, which names the server), or the error code of Redis (`WRONGTYPE`,
 * `NOGROUP`, `OOM`), never the rest of the error text, which may quote the arguments of the command,
 * and never a value of a message (RN-05). The exception of the client is kept as the previous one.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RedisStreamsException extends MessagingException
{
    /** The client failed (the connection, the protocol) while it ran the operation. */
    public static function unreachable(string $operation, Throwable $error): self
    {
        return new self(sprintf('Redis Streams could not %s: %s.', $operation, RedisCommands::describe($error)), 0, $error);
    }

    /** Redis answered the operation on a stream with an error, or with something else than expected. */
    public static function refused(string $operation, string $stream, mixed $reply): self
    {
        return new self(sprintf('Redis refused to %s on stream "%s" (%s).', $operation, $stream, RedisCommands::errorCode($reply)));
    }
}
