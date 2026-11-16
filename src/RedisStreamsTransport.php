<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams;

use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\TransportSendResult;
use Antevemus\ASpecificationRedisStreams\Internal\RedisCommands;
use Predis\ClientInterface;
use Predis\PredisException;

/**
 * RedisStreamsTransport - The Hook Out of the Core, Over Redis Streams
 *
 * The IMessageTransport of antevemus/aspecification 2.14.0 that OutboxRelay hands each batch to. Every
 * message is appended (`XADD`, with `MAXLEN ~` when the topology caps the streams) to the stream of
 * its name, `<prefix>.<name>`; the whole batch goes in one round trip. A message is confirmed when
 * Redis answered its `XADD` with the id of the new entry, which is the broker id of the result: Redis
 * has then stored it (durably as far as the persistence of the server goes: AOF with
 * `appendfsync everysec` loses at most a second on a crash of the server, `always` nothing).
 *
 * The entry holds the wire form of the core, `Message::toJson()`, in the field `body` (the tenant and
 * the headers travel in it), and, for the tools of the broker side and for the dead letters, the
 * fields `seq`, `message_id`, `name`, `ordering_key` and `tenant` (the last two only when the message
 * has them); none of them holds a value of the payload. `seq` is the global order of the writes: one
 * Lua script increments the counter `<prefix>.seq` and appends the entry in one atomic step, so that
 * RedisStreamsSource can merge the streams of several names in the order they were written, and the
 * messages of an ordering key published under two names keep their order.
 *
 * Failures: a message Redis refused (the key holds another type, the server is out of memory, ...) is
 * failed with a reason that names the message (Message::describe()), the stream and the error code of
 * Redis, never the error text nor the payload (RN-05), and the others of the batch are still
 * answered. A connection that fails, before or during the batch, fails the whole batch with one
 * reason; whatever Redis stored before the connection dropped is sent again by the relay and absorbed
 * by the inbox of the consumers (at-least-once).
 *
 * <code>
 * $transport = new RedisStreamsTransport(new \Predis\Client('tcp://127.0.0.1:6379'));
 * $relay = new OutboxRelay($outbox, $transport, maxAttempts: 10);
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RedisStreamsTransport implements IMessageTransport
{
    /**
     * KEYS[1] the stream, KEYS[2] the counter; ARGV[1] the cap ('' = none), ARGV[2..] the field pairs.
     * Increments the counter and appends the entry with it as the field `seq`, atomically.
     */
    public const APPEND_SCRIPT = <<<'LUA'
        local seq = redis.call('INCR', KEYS[2])
        local args = {KEYS[1]}
        if ARGV[1] ~= '' then
            args[#args + 1] = 'MAXLEN'
            args[#args + 1] = '~'
            args[#args + 1] = ARGV[1]
        end
        args[#args + 1] = '*'
        args[#args + 1] = 'seq'
        args[#args + 1] = tostring(seq)
        for i = 2, #ARGV do
            args[#args + 1] = ARGV[i]
        end
        return redis.call('XADD', unpack(args))
        LUA;

    /** The id of a stream entry. */
    private const ENTRY_ID_PATTERN = '/^\d+-\d+$/D';

    public function __construct(
        private readonly ClientInterface $redis,
        private readonly RedisStreamsTopology $topology = new RedisStreamsTopology()
    ) {
    }

    /** The topology the transport writes to. */
    public function getTopology(): RedisStreamsTopology
    {
        return $this->topology;
    }

    /** {@inheritdoc} */
    public function send(array $batch): TransportSendResult
    {
        $batch = array_values($batch);
        $commands = [];
        foreach ($batch as $message) {
            $commands[] = $this->append($message);
        }
        try {
            $replies = RedisCommands::pipeline($this->redis, $commands);
        } catch (PredisException $e) {
            return TransportSendResult::allFailed($batch, sprintf(
                'Redis Streams did not confirm the batch of %d message(s): %s.',
                count($batch),
                RedisCommands::describe($e)
            ));
        }
        $result = new TransportSendResult();
        foreach ($batch as $i => $message) {
            $reply = $replies[$i];
            if (is_string($reply) && preg_match(self::ENTRY_ID_PATTERN, $reply) === 1) {
                $result = $result->withConfirmed($message->messageId, $reply);
                continue;
            }
            $result = $result->withFailed($message->messageId, sprintf(
                'Redis refused %s on stream "%s" (%s).',
                $message->describe(),
                $this->topology->streamOf($message->name),
                RedisCommands::errorCode($reply)
            ));
        }

        return $result;
    }

    /**
     * The EVAL of the append script for one message.
     *
     * @return list<string|int>
     */
    private function append(Message $message): array
    {
        $command = [
            'EVAL',
            self::APPEND_SCRIPT,
            2,
            $this->topology->streamOf($message->name),
            $this->topology->sequenceKey(),
            $this->topology->maxLen ?? '',
            'message_id',
            $message->messageId,
            'name',
            $message->name,
        ];
        if ($message->orderingKey !== null) {
            $command[] = 'ordering_key';
            $command[] = $message->orderingKey;
        }
        if ($message->tenant !== null) {
            $command[] = 'tenant';
            $command[] = $message->tenant;
        }
        $command[] = 'body';
        $command[] = $message->toJson();

        return $command;
    }
}
