<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Internal;

use Predis\ClientInterface;
use Predis\Command\RawCommand;
use Predis\Connection\ConnectionException;
use Predis\Connection\Replication\ReplicationInterface;
use Predis\Response\ErrorInterface;
use Throwable;

/**
 * RedisCommands - Pipelined Raw Commands and Secret-Free Descriptions of Their Failures
 *
 * The transport and the source send raw commands (RawCommand: the reply of Redis as it comes, with no
 * parsing of predis) through the connection of the client, all written before any reply is read (one
 * round trip), whatever the `exceptions` option of the client: an error reply of Redis comes back as
 * a Predis\Response\ErrorInterface in its place, so that one refused command does not hide the
 * replies of the others. A replicated connection is switched to the master first, as predis does for
 * its own pipelines.
 *
 * describe() and errorCode() are the only texts of a failure that reach a reason or an exception
 * (RN-05): the short class of the exception of the client, plus its message only for a failed
 * connection (it names the server); the first word of an error reply of Redis (`WRONGTYPE`, `OOM`),
 * never the rest, which may quote the arguments of the command (`unknown command ..., with args
 * beginning with: ...`).
 *
 * @internal
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @subpackage Internal
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RedisCommands
{
    /** The shape of an error code of Redis. */
    private const ERROR_CODE_PATTERN = '/^[A-Z][A-Z0-9_]{0,31}$/D';

    private function __construct()
    {
    }

    /**
     * Sends the commands in one round trip and returns their replies, in order.
     *
     * @param array<array-key, list<string|int>> $commands Each command: its name, then its arguments (an integer is sent as its decimal text)
     * @return list<mixed> The reply of each command (an ErrorInterface for an error reply)
     * @throws \Predis\PredisException When the connection or the protocol fails
     */
    public static function pipeline(ClientInterface $redis, array $commands): array
    {
        $connection = $redis->getConnection();
        if ($connection instanceof ReplicationInterface) {
            $connection->switchToMaster();
        }
        $sent = [];
        foreach ($commands as $arguments) {
            $command = new RawCommand(array_shift($arguments), $arguments);
            $connection->writeRequest($command);
            $sent[] = $command;
        }
        $replies = [];
        foreach ($sent as $command) {
            $replies[] = $connection->readResponse($command);
        }

        return $replies;
    }

    /** The cause of a failure of the client, without anything a command carried. */
    public static function describe(Throwable $error): string
    {
        $class = substr(strrchr('\\' . $error::class, '\\'), 1);

        return $error instanceof ConnectionException ? sprintf('%s: %s', $class, $error->getMessage()) : $class;
    }

    /** The error code of an error reply (its first word), or what else came. */
    public static function errorCode(mixed $reply): string
    {
        if (!$reply instanceof ErrorInterface) {
            return sprintf('unexpected reply of type %s', get_debug_type($reply));
        }
        $code = $reply->getErrorType();

        return preg_match(self::ERROR_CODE_PATTERN, $code) === 1 ? $code : 'error reply';
    }
}
