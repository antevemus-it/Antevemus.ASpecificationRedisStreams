<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\Inbox\ProcessedMessage;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRedisStreams\Internal\RedisCommands;
use Antevemus\ASpecificationRedisStreams\Internal\StreamEntries;
use InvalidArgumentException;
use Predis\ClientInterface;
use Predis\PredisException;
use Predis\Response\ErrorInterface;

/**
 * RedisStreamsSource - The Hook In of the Core: One Consumer Group of the Streams of Some Names
 *
 * The IMessageSource of antevemus/aspecification 2.14.0 that MessageConsumer reads. The consumer group
 * is the consumer of the core (its name, `agents`, names the group on every stream it reads), and
 * the source is one member of it, the Redis consumer `consumerName` (one per process; two processes of
 * one group compete for the messages, as two consumers of one queue do). It reads the streams of an
 * explicit list of message names, `<prefix>.<name>`, never Pub/Sub, which loses what is published
 * while a consumer is away.
 *
 * - declare() creates the group on each stream (`XGROUP CREATE <stream> <group> $ MKSTREAM`; a group
 *   that exists is left as it is), so that the group gets every message added from then on; receive()
 *   calls it the first time. Declare the consumers before the relay sends what they must get
 *   (`declare(fromStart: true)` gives a new group what the streams still hold).
 * - receive($max, $waitMs) gathers, in this order: the deliveries of any member of the group pending
 *   for longer than `recoverIdleMs` (`XAUTOCLAIM`: a process that died), the deliveries of this member
 *   that it gave back with reject($requeue = true) or that a previous process of the same
 *   `consumerName` left unanswered (read from `XPENDING` and claimed again with `XCLAIM`), the
 *   entries read and not yet handed out, and new entries (`XREADGROUP ... >`, blocking for at most
 *   min($waitMs, maxBlockMs) when there is nothing else). It merges them by the global order of the
 *   transport (the field `seq`) and hands out the first $max; the others stay pending in this member
 *   and come first next time. Every stream is read far enough (at least $max entries known per stream,
 *   or all it has) that no entry left behind can come before one handed out, so the deliveries of an
 *   ordering key come in the order they were written, across the streams of several names, and a
 *   delivery given back comes again BEFORE any later delivery of its key: it is older than anything
 *   not yet handed out.
 * - The delivery count is the counter of Redis for the entry in the group (`XPENDING`): 1 when first
 *   read, plus one for each claim (`XCLAIM`, `XAUTOCLAIM`). A delivery kept in flight longer than
 *   `recoverIdleMs` counts as abandoned: any member may claim it, and its count grows.
 * - The broker id is `<stream>#<entry id>`. ack() is `XACK`; reject($requeue = true) only forgets the
 *   delivery in this process (it stays pending in Redis, to be claimed again first); reject($requeue =
 *   false) appends a dead letter to `<prefix>.dead` and acknowledges the original in one atomic step:
 *   the stream, the entry id, the group, the member, the delivery count, the reason (never a value of
 *   the payload, RN-05), the message id, name, ordering key and tenant when the delivery was a message,
 *   and the original `body`, so that an operator can inspect and replay it (data at rest in the broker,
 *   like the original entry). ack() or reject() of a delivery that is not in flight in this source is a
 *   MessagingException, as in the in-memory source of the core.
 * - What cannot be read back as a message is an invalid delivery (ReceivedMessage::invalid()), never
 *   dropped: a body that is not the wire form of format 1 (ReceivedMessage::decode()), an entry without
 *   a `body` field (another producer), and an entry that was trimmed or deleted while pending.
 * - A failure of the connection, or an error reply of Redis, is a RedisStreamsException (a
 *   MessagingException); nothing in flight is lost: it stays pending in the group.
 *
 * <code>
 * $source = new RedisStreamsSource($redis, 'agents', ['crm.lead_created.v1', 'livechat.message_received.v1'], 'agents-1');
 * $consumer = new MessageConsumer('agents', $source, $handlers, $inbox, $transactions);
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RedisStreamsSource implements IMessageSource
{
    /** A member of a group (a Redis consumer). */
    public const CONSUMER_NAME_PATTERN = '/^[A-Za-z0-9][A-Za-z0-9._:@-]{0,119}$/D';

    /** Separator of the stream and the entry id in a broker id. */
    public const BROKER_ID_SEPARATOR = StreamEntries::BROKER_ID_SEPARATOR;

    /** Longest reason kept with a dead letter, in characters. */
    public const MAX_REASON_LENGTH = 2000;

    /**
     * KEYS[1] the original stream, KEYS[2] the dead letter stream; ARGV[1] the group, ARGV[2] the
     * entry id, ARGV[3..] the field pairs of the dead letter. Appends the dead letter, then
     * acknowledges the original, atomically.
     */
    public const DEAD_LETTER_SCRIPT = <<<'LUA'
        local id = redis.call('XADD', KEYS[2], '*', unpack(ARGV, 3))
        redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
        return id
        LUA;

    private readonly string $group;

    private readonly string $consumerName;

    /** @var list<string> */
    private readonly array $streams;

    private bool $declared = false;

    /** @var array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> */
    private array $inFlight = [];

    /** @var array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> */
    private array $buffered = [];

    /** @var array<string, string> stream => cursor of XAUTOCLAIM */
    private array $cursors = [];

    /**
     * @param ClientInterface $redis The connection
     * @param string $group The consumer group: the name of the consumer of the core (lowercase, at most 120 characters)
     * @param list<string> $messageNames The names whose streams the group reads (at least one)
     * @param string|null $consumerName This member of the group (null = `<host>-<pid>`); keep it stable per worker so that a restart takes its pending deliveries at once
     * @param RedisStreamsTopology $topology The keys and the limits (the same as the transport's)
     * @throws InvalidArgumentException When the group, a name or the member is invalid, or no name is given
     */
    public function __construct(
        private readonly ClientInterface $redis,
        string $group,
        array $messageNames,
        ?string $consumerName = null,
        private readonly RedisStreamsTopology $topology = new RedisStreamsTopology()
    ) {
        $this->group = ProcessedMessage::assertConsumer($group);
        if ($messageNames === []) {
            throw new InvalidArgumentException('A source reads the streams of at least one message name.');
        }
        $streams = [];
        foreach ($messageNames as $name) {
            $streams[] = $topology->streamOf($name);
        }
        $this->streams = array_values(array_unique($streams));
        $consumerName ??= StreamEntries::consumerName((string) gethostname(), (int) getmypid());
        if (preg_match(self::CONSUMER_NAME_PATTERN, $consumerName) !== 1) {
            throw new InvalidArgumentException(sprintf('The consumer name of a source must match %s.', self::CONSUMER_NAME_PATTERN));
        }
        $this->consumerName = $consumerName;
    }

    /** The consumer group (the consumer of the core). */
    public function getGroup(): string
    {
        return $this->group;
    }

    /** This member of the group. */
    public function getConsumerName(): string
    {
        return $this->consumerName;
    }

    /**
     * The streams read, in the order of the names given.
     *
     * @return list<string>
     */
    public function getStreams(): array
    {
        return $this->streams;
    }

    /** The topology the source reads. */
    public function getTopology(): RedisStreamsTopology
    {
        return $this->topology;
    }

    /**
     * Creates the group on every stream (and the stream when missing); a group that exists is kept.
     *
     * @param bool $fromStart A new group gets what the streams hold (`0`) instead of only what comes next (`$`)
     * @throws RedisStreamsException When Redis cannot be reached or refuses
     */
    public function declare(bool $fromStart = false): void
    {
        $commands = [];
        foreach ($this->streams as $stream) {
            $commands[] = ['XGROUP', 'CREATE', $stream, $this->group, $fromStart ? '0' : '$', 'MKSTREAM'];
        }
        foreach ($this->run($commands, 'create the consumer group') as $i => $reply) {
            if ($reply instanceof ErrorInterface && $reply->getErrorType() !== 'BUSYGROUP') {
                throw RedisStreamsException::refused(sprintf('create the consumer group "%s"', $this->group), $this->streams[$i], $reply);
            }
        }
        $this->declared = true;
    }

    /** {@inheritdoc} */
    public function receive(int $max, int $waitMs): array
    {
        if ($max < 1) {
            throw new InvalidArgumentException(sprintf('receive() takes at least 1 delivery, %d given.', $max));
        }
        if ($waitMs < 0) {
            throw new InvalidArgumentException(sprintf('The wait cannot be negative, %d given.', $waitMs));
        }
        if (!$this->declared) {
            $this->declare();
        }
        $candidates = $this->buffered;
        $this->buffered = [];
        $this->recoverPending($max, $candidates);
        $this->readNew($max, $waitMs, $candidates);
        uasort($candidates, StreamEntries::compare(...));

        $received = [];
        foreach (array_slice($candidates, 0, $max, true) as $brokerId => $candidate) {
            $this->inFlight[$brokerId] = $candidate;
            $received[] = self::delivery($brokerId, $candidate);
        }
        $this->buffered = array_slice($candidates, $max, null, true);

        return $received;
    }

    /** {@inheritdoc} */
    public function ack(ReceivedMessage $message): void
    {
        $entry = $this->mustBeInFlight($message);
        $reply = $this->run([['XACK', $entry['stream'], $this->group, $entry['id']]], 'acknowledge ' . $message->describe())[0];
        if (!is_int($reply)) {
            throw RedisStreamsException::refused('acknowledge ' . $message->describe(), $entry['stream'], $reply);
        }
        unset($this->inFlight[$message->brokerId]);
    }

    /** {@inheritdoc} */
    public function reject(ReceivedMessage $message, bool $requeue, string $reason): void
    {
        $entry = $this->mustBeInFlight($message);
        if ($requeue) {
            // Still pending in this member: the next receive() claims it again, before anything newer.
            unset($this->inFlight[$message->brokerId]);

            return;
        }
        $fields = [
            'stream', $entry['stream'],
            'entry_id', $entry['id'],
            'group', $this->group,
            'consumer', $this->consumerName,
            'delivery_count', $message->deliveryCount,
            'reason', mb_substr($reason, 0, self::MAX_REASON_LENGTH),
        ];
        if ($message->isValid()) {
            $delivered = $message->message();
            $fields = [...$fields, 'message_id', $delivered->messageId, 'name', $delivered->name];
            if ($delivered->orderingKey !== null) {
                $fields = [...$fields, 'ordering_key', $delivered->orderingKey];
            }
            if ($delivered->tenant !== null) {
                $fields = [...$fields, 'tenant', $delivered->tenant];
            }
        }
        if (isset($entry['fields']['body'])) {
            $fields = [...$fields, 'body', $entry['fields']['body']];
        }
        $operation = 'dead-letter ' . $message->describe();
        $reply = $this->run([['EVAL', self::DEAD_LETTER_SCRIPT, 2, $entry['stream'], $this->topology->deadLetterStream(), $this->group, $entry['id'], ...$fields]], $operation)[0];
        if (!is_string($reply)) {
            throw RedisStreamsException::refused($operation, $entry['stream'], $reply);
        }
        unset($this->inFlight[$message->brokerId]);
    }

    /**
     * The dead letters of this group, oldest first (a scan of `<prefix>.dead`, for operators and tests).
     *
     * @param int $limit Maximum number of dead letters returned (>= 1)
     * @return list<array{brokerId: string, messageId: ?string, reason: string, deliveryCount: int, deadLetterId: string}>
     * @throws InvalidArgumentException When $limit < 1
     * @throws RedisStreamsException When Redis cannot be reached or refuses
     */
    public function deadLetters(int $limit = 100): array
    {
        if ($limit < 1) {
            throw new InvalidArgumentException(sprintf('deadLetters() returns at least 1 dead letter, %d asked.', $limit));
        }
        $stream = $this->topology->deadLetterStream();
        $dead = [];
        $from = '-';
        do {
            $reply = $this->run([['XRANGE', $stream, $from, '+', 'COUNT', 100]], 'read the dead letters')[0];
            if (!is_array($reply)) {
                throw RedisStreamsException::refused('read the dead letters', $stream, $reply);
            }
            foreach ($reply as [$id, $flat]) {
                $from = '(' . $id;
                $fields = StreamEntries::fields($flat);
                if (($fields['group'] ?? null) !== $this->group) {
                    continue;
                }
                $dead[] = [
                    'brokerId' => ($fields['stream'] ?? '') . self::BROKER_ID_SEPARATOR . ($fields['entry_id'] ?? ''),
                    'messageId' => $fields['message_id'] ?? null,
                    'reason' => $fields['reason'] ?? '',
                    'deliveryCount' => (int) ($fields['delivery_count'] ?? 0),
                    'deadLetterId' => $id,
                ];
                if (count($dead) === $limit) {
                    return $dead;
                }
            }
        } while (count($reply) === 100);

        return $dead;
    }

    /** Deliveries handed out by this source and not yet answered. */
    public function inFlightCount(): int
    {
        return count($this->inFlight);
    }

    /**
     * Adds the pending deliveries to the candidates: those of any member idle for longer than
     * recoverIdleMs (XAUTOCLAIM), then those of this member that are neither in flight nor already
     * candidates (given back, or left by a previous process of the same member), claimed again. The
     * pending entries of this member are listed before the recovery, so that the count of an entry
     * it claims from itself (or finds deleted) is the one it had plus one.
     *
     * @param array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> $candidates
     */
    private function recoverPending(int $max, array &$candidates): void
    {
        // Listed per stream: what is in flight or known there, plus room for $max deliveries given back.
        $listed = array_fill_keys($this->streams, $max);
        foreach ($this->inFlight as $entry) {
            $listed[$entry['stream']]++;
        }
        foreach ($candidates as $entry) {
            $listed[$entry['stream']]++;
        }
        $commands = [];
        foreach ($this->streams as $stream) {
            $commands[] = ['XPENDING', $stream, $this->group, '-', '+', $listed[$stream], $this->consumerName];
        }
        $recover = $this->topology->recoverIdleMs !== null;
        if ($recover) {
            foreach ($this->streams as $stream) {
                $commands[] = ['XAUTOCLAIM', $stream, $this->group, $this->consumerName, $this->topology->recoverIdleMs, $this->cursors[$stream] ?? '0-0', 'COUNT', $max];
            }
        }
        $replies = $this->run($commands, 'read the pending deliveries');
        $pending = [];
        foreach ($this->streams as $stream) {
            $reply = array_shift($replies);
            if (!is_array($reply)) {
                throw RedisStreamsException::refused('read the pending deliveries', $stream, $reply);
            }
            foreach ($reply as [$id, , , $count]) {
                $pending[StreamEntries::brokerId($stream, $id)] = [$stream, $id, $count];
            }
        }
        $claimed = [];
        if ($recover) {
            foreach ($this->streams as $stream) {
                $reply = array_shift($replies);
                if (!is_array($reply)) {
                    throw RedisStreamsException::refused('recover the idle deliveries', $stream, $reply);
                }
                [$this->cursors[$stream], $entries, $deleted] = $reply;
                foreach ($entries as [$id, $flat]) {
                    $claimed[StreamEntries::brokerId($stream, $id)] = [$stream, $id, StreamEntries::fields($flat)];
                }
                foreach ($deleted as $id) {
                    $claimed[StreamEntries::brokerId($stream, $id)] = [$stream, $id, null];
                }
            }
        }
        $this->addClaimed($claimed, $pending, $candidates);
        $ready = [];
        foreach ($pending as $brokerId => [$stream, $id, $count]) {
            if (!isset($this->inFlight[$brokerId]) && !isset($candidates[$brokerId]) && count($ready[$stream] ?? []) < $max) {
                $ready[$stream][$id] = $count;
            }
        }
        $this->claimReady($ready, $candidates);
    }

    /**
     * Adds the entries claimed by the recovery (but those in flight here), with their count: the one
     * this member had plus one, or the one Redis has now for an entry of another member.
     *
     * @param array<string, array{string, string, array<string, string>|null}> $claimed
     * @param array<string, array{string, string, int}> $pending
     * @param array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> $candidates
     */
    private function addClaimed(array $claimed, array $pending, array &$candidates): void
    {
        $lookups = [];
        foreach ($claimed as $brokerId => [$stream, $id]) {
            if (!isset($pending[$brokerId])) {
                $lookups[$brokerId] = ['XPENDING', $stream, $this->group, $id, $id, 1];
            }
        }
        $counts = [];
        $replies = $this->run($lookups, 'count the deliveries of the recovered entries');
        foreach (array_keys($lookups) as $i => $brokerId) {
            $counts[$brokerId] = $replies[$i][0][3] ?? 1; // a deleted entry is no longer pending: its count is unknown
        }
        foreach ($claimed as $brokerId => [$stream, $id, $fields]) {
            if (isset($this->inFlight[$brokerId])) {
                continue; // still being handled here: it stays in flight
            }
            $count = isset($pending[$brokerId]) ? $pending[$brokerId][2] + 1 : $counts[$brokerId];
            $candidates[$brokerId] = ['stream' => $stream, 'id' => $id, 'fields' => $fields, 'count' => $count];
        }
    }

    /**
     * Claims again the pending deliveries of this member that are ready (XCLAIM increments the count).
     *
     * @param array<string, array<string, int>> $ready stream => entry id => delivery count so far
     * @param array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> $candidates
     */
    private function claimReady(array $ready, array &$candidates): void
    {
        $commands = [];
        foreach ($ready as $stream => $ids) {
            $commands[] = ['XCLAIM', $stream, $this->group, $this->consumerName, 0, ...array_keys($ids)];
        }
        $replies = $this->run($commands, 'claim the deliveries given back');
        foreach (array_keys($ready) as $i => $stream) {
            $reply = $replies[$i];
            if (!is_array($reply)) {
                throw RedisStreamsException::refused('claim the deliveries given back', $stream, $reply);
            }
            $found = [];
            foreach ($reply as $entry) {
                if (is_array($entry)) {
                    $found[$entry[0]] = StreamEntries::fields($entry[1]);
                }
            }
            foreach ($ready[$stream] as $id => $count) {
                $candidates[StreamEntries::brokerId($stream, $id)] = ['stream' => $stream, 'id' => $id, 'fields' => $found[$id] ?? null, 'count' => $count + 1];
            }
        }
    }

    /**
     * Reads new entries from every stream of which fewer than $max entries are known, blocking when
     * there is nothing else to hand out.
     *
     * @param array<string, array{stream: string, id: string, fields: array<string, string>|null, count: int}> $candidates
     */
    private function readNew(int $max, int $waitMs, array &$candidates): void
    {
        $known = array_fill_keys($this->streams, 0);
        foreach ($candidates as $candidate) {
            $known[$candidate['stream']]++;
        }
        $streams = array_keys(array_filter($known, static fn(int $count): bool => $count < $max));
        if ($streams === []) {
            return;
        }
        $command = ['XREADGROUP', 'GROUP', $this->group, $this->consumerName, 'COUNT', $max - min($known)];
        if ($candidates === [] && $waitMs > 0) {
            $command = [...$command, 'BLOCK', min($waitMs, $this->topology->maxBlockMs)];
        }
        $command = [...$command, 'STREAMS', ...$streams, ...array_map(static fn(): string => '>', $streams)];
        $reply = $this->run([$command], 'read new entries')[0];
        if ($reply === null) {
            return;
        }
        if (!is_array($reply)) {
            throw RedisStreamsException::refused('read new entries', implode('", "', $streams), $reply);
        }
        foreach ($reply as [$stream, $entries]) {
            foreach ($entries as [$id, $flat]) {
                $candidates[StreamEntries::brokerId($stream, $id)] = ['stream' => $stream, 'id' => $id, 'fields' => StreamEntries::fields($flat), 'count' => 1];
            }
        }
    }

    /**
     * Sends commands, a failure of the client turned into a RedisStreamsException.
     *
     * @param array<array-key, list<string|int>> $commands
     * @return list<mixed>
     * @throws RedisStreamsException
     */
    private function run(array $commands, string $operation): array
    {
        try {
            return RedisCommands::pipeline($this->redis, $commands);
        } catch (PredisException $e) {
            throw RedisStreamsException::unreachable($operation, $e);
        }
    }

    /**
     * @return array{stream: string, id: string, fields: array<string, string>|null, count: int}
     * @throws MessagingException When the delivery is not in flight here
     */
    private function mustBeInFlight(ReceivedMessage $message): array
    {
        return $this->inFlight[$message->brokerId] ?? throw new MessagingException(sprintf(
            'Delivery %s is not in flight on this source: it was already answered, or it came from another one.',
            $message->brokerId
        ));
    }

    /**
     * The delivery of a candidate: the message, or an invalid delivery that says why not.
     *
     * @param array{stream: string, id: string, fields: array<string, string>|null, count: int} $candidate
     */
    private static function delivery(string $brokerId, array $candidate): ReceivedMessage
    {
        if ($candidate['fields'] === null) {
            return ReceivedMessage::invalid($candidate['count'], $brokerId, sprintf(
                'Entry %s of stream "%s" was deleted (trimmed by MAXLEN, or XDEL) while it was pending: its message is lost.',
                $candidate['id'],
                $candidate['stream']
            ));
        }
        if (!isset($candidate['fields']['body'])) {
            return ReceivedMessage::invalid($candidate['count'], $brokerId, sprintf(
                'Entry %s of stream "%s" has no "body" field: it was not written by RedisStreamsTransport.',
                $candidate['id'],
                $candidate['stream']
            ));
        }

        return ReceivedMessage::decode($candidate['fields']['body'], $candidate['count'], $brokerId);
    }
}
