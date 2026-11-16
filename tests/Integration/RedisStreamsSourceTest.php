<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Integration;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRedisStreams\RedisStreamsException;
use Antevemus\ASpecificationRedisStreams\RedisStreamsSource;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Antevemus\ASpecificationRedisStreams\Tests\Support\RedisTestCase;
use PHPUnit\Framework\Attributes\Group;
use Predis\Client;
use Predis\ClientInterface;

/**
 * RedisStreamsSource against the server: the group, the order across streams and the entries read and
 * not handed out, the delivery count, the requeue, the dead letters, the idle recovery, the invalid
 * deliveries and every failure of Redis.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Integration
 */
#[Group('redis')]
final class RedisStreamsSourceTest extends RedisTestCase
{
    private const A = 'crm.lead_created.v1';

    private const B = 'crm.lead_updated.v1';

    private const C = 'crm.lead_deleted.v1';

    public function testTheGroupIsCreatedOnEveryStreamAndKept(): void
    {
        $source = $this->source([self::A, self::A, self::B]);
        $this->assertSame([$this->prefix . '.' . self::A, $this->prefix . '.' . self::B], $source->getStreams(), 'one stream per name, in the order given');
        $source->declare();
        $source->declare();
        foreach ($source->getStreams() as $stream) {
            $groups = $this->redis->executeRaw(['XINFO', 'GROUPS', $stream]);
            $this->assertCount(1, $groups);
            $this->assertSame('agents', $groups[0][1]);
        }
    }

    public function testAGroupGetsWhatIsAddedAfterItExistsUnlessDeclaredFromTheStart(): void
    {
        $transport = $this->transport();
        $transport->send([$this->message(self::A, 'before')]);
        $late = $this->source([self::A], group: 'late');
        $late->declare();
        $history = $this->source([self::A], group: 'history');
        $history->declare(fromStart: true);
        $transport->send([$this->message(self::A, 'after')]);

        $this->assertSame(['after'], $this->labels($late->receive(10, 0)));
        $this->assertSame(['before', 'after'], $this->labels($history->receive(10, 0)));
    }

    public function testTheFirstReceiveDeclaresTheGroup(): void
    {
        $source = $this->source([self::A]);
        $this->assertSame([], $source->receive(10, 0));
        $this->transport()->send([$this->message(self::A, 'one')]);
        $this->assertSame(['one'], $this->labels($source->receive(10, 0)));
    }

    public function testTheMembersOfAGroupShareItsMessagesAndEachGroupGetsThemAll(): void
    {
        $first = $this->source([self::A], member: 'w1');
        $second = $this->source([self::A], member: 'w2');
        $other = $this->source([self::A], group: 'audit');
        foreach ([$first, $second, $other] as $source) {
            $source->declare();
        }
        $this->transport()->send([$this->message(self::A, 'm1'), $this->message(self::A, 'm2'), $this->message(self::A, 'm3')]);

        $this->assertSame(['m1', 'm2'], $this->labels($first->receive(2, 0)));
        $this->assertSame(['m3'], $this->labels($second->receive(2, 0)));
        $this->assertSame(['m1', 'm2', 'm3'], $this->labels($other->receive(10, 0)));
        $this->assertSame(['w1', 'w1', 'w2'], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 0));
    }

    public function testTheStreamsAreMergedInTheOrderTheyWereWritten(): void
    {
        $source = $this->source([self::A, self::B, self::C]);
        $source->declare();
        $transport = $this->transport();
        foreach ([[self::C, 'c1'], [self::A, 'a1'], [self::B, 'b1'], [self::A, 'a2'], [self::C, 'c2'], [self::B, 'b2']] as [$name, $label]) {
            $transport->send([$this->message($name, $label)]);
        }
        $this->assertSame(['c1', 'a1', 'b1', 'a2', 'c2', 'b2'], $this->labels($source->receive(10, 0)));
    }

    public function testTheOrderIsTheFieldSeqNotTheEntryIds(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $a = $this->message(self::A, 'second');
        $b = $this->message(self::B, 'first');
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::A, '5-0', 'seq', '2', 'body', $a->toJson()]);
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::B, '9-0', 'seq', '1', 'body', $b->toJson()]);
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::B, '9-1', 'seq', 'x', 'body', 'foreign']);
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::A, '7-3', 'body', 'foreign']);
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::A, '7-12', 'body', 'foreign']);
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::B, '10-0', 'body', 'foreign']);

        $deliveries = $source->receive(10, 0);
        $this->assertSame(
            [$this->prefix . '.' . self::A . '#7-3', $this->prefix . '.' . self::A . '#7-12', $this->prefix . '.' . self::B . '#9-1', $this->prefix . '.' . self::B . '#10-0', $this->prefix . '.' . self::B . '#9-0', $this->prefix . '.' . self::A . '#5-0'],
            array_map(static fn(ReceivedMessage $d): string => $d->brokerId, $deliveries),
            'without a seq first, by stream and entry id; then by seq'
        );
    }

    public function testEntriesReadAndNotHandedOutComeFirstNextTimeWithTheirCount(): void
    {
        $source = $this->source([self::A, self::B, self::C]);
        $source->declare();
        $transport = $this->transport();
        $transport->send([$this->message(self::A, 'a1')]);
        $transport->send([$this->message(self::B, 'b1')]);
        $transport->send([$this->message(self::C, 'c1')]);
        $transport->send([$this->message(self::A, 'a2')]);

        $first = $source->receive(1, 0);
        $this->assertSame(['a1'], $this->labels($first));
        $this->assertSame(1, $source->inFlightCount());
        $this->assertCount(1, $this->pending($this->prefix . '.' . self::B, 'agents'), 'b1 was read: pending in this member, not handed out');
        $source->ack($first[0]);

        $second = $source->receive(2, 0);
        $this->assertSame(['b1', 'c1'], $this->labels($second));
        $this->assertSame([1, 1], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $second), 'a first delivery, though read before');
        $third = $source->receive(5, 2000);
        $this->assertSame(['a2'], $this->labels($third));
    }

    public function testNothingLeftBehindComesBeforeWhatIsHandedOut(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $transport = $this->transport();
        for ($i = 1; $i <= 5; $i++) {
            $transport->send([$this->message(self::A, "a{$i}")]);
        }
        $transport->send([$this->message(self::B, 'b1')]);
        $transport->send([$this->message(self::A, 'a6')]);

        $seen = [];
        do {
            $batch = $source->receive(2, 0);
            foreach ($batch as $delivery) {
                $seen[] = $delivery->message()->payload['n'];
                $source->ack($delivery);
            }
        } while ($batch !== []);
        $this->assertSame(['a1', 'a2', 'a3', 'a4', 'a5', 'b1', 'a6'], $seen);
    }

    public function testBlockingWaitsAtMostTheWaitAndTheCap(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $start = microtime(true);
        $this->assertSame([], $source->receive(5, 300));
        $waited = (microtime(true) - $start) * 1000;
        $this->assertGreaterThanOrEqual(250, $waited);
        $this->assertLessThan(1500, $waited);

        $capped = $this->source([self::A], topology: $this->topology(maxBlockMs: 50));
        $start = microtime(true);
        $this->assertSame([], $capped->receive(5, 3000));
        $this->assertLessThan(1000, (microtime(true) - $start) * 1000, 'the cap of the topology');

        $start = microtime(true);
        $this->assertSame([], $source->receive(5, 0));
        $this->assertLessThan(200, (microtime(true) - $start) * 1000, 'no wait');
    }

    public function testNothingBlocksWhileThereIsSomethingToHandOut(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1'), $this->message(self::A, 'a2')]);
        $this->assertSame(['a1'], $this->labels($source->receive(1, 0)));
        $start = microtime(true);
        $this->assertSame(['a2'], $this->labels($source->receive(5, 3000)));
        $this->assertLessThan(1000, (microtime(true) - $start) * 1000);
    }

    public function testAckRemovesTheDeliveryForGood(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm')]);
        [$delivery] = $source->receive(10, 0);
        $source->ack($delivery);
        $this->assertSame(0, $source->inFlightCount());
        $this->assertSame([], $this->pending($this->prefix . '.' . self::A, 'agents'));
        $this->assertSame([], $source->receive(10, 0));
        $this->assertNotInFlight(static fn() => $source->ack($delivery), $delivery);
        $this->assertNotInFlight(static fn() => $source->reject($delivery, true, 'again'), $delivery);
        $this->assertNotInFlight(static fn() => $source->reject($delivery, false, 'again'), $delivery);
    }

    public function testADeliveryOfAnotherSourceIsNotInFlightHere(): void
    {
        $source = $this->source([self::A]);
        $foreign = ReceivedMessage::of($this->message(self::A, 'm'), 1, $this->prefix . '.' . self::A . '#1-0');
        $this->assertNotInFlight(static fn() => $source->ack($foreign), $foreign);
    }

    public function testARequeuedDeliveryStaysPendingAndComesAgainWithAHigherCount(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm1'), $this->message(self::A, 'm2')]);
        $deliveries = $source->receive(1, 0);
        $source->reject($deliveries[0], true, 'later');
        $this->assertSame(0, $source->inFlightCount());
        $pending = $this->pending($this->prefix . '.' . self::A, 'agents');
        $this->assertSame([['w1', 1]], array_values($pending), 'm1 given back: still pending in this member (m2 not read yet)');

        $again = $source->receive(10, 0);
        $this->assertSame(['m1', 'm2'], $this->labels($again));
        $this->assertSame([2, 1], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $again));
        $this->assertSame($deliveries[0]->brokerId, $again[0]->brokerId, 'the same broker id on every delivery');
        $source->reject($again[0], true, 'later');
        $source->reject($again[1], true, 'later');
        $this->assertSame([3, 2], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $source->receive(10, 0)));
    }

    public function testADeliveryGivenBackComesAgainAtOnce(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm1')]);
        [$delivery] = $source->receive(1, 0);
        for ($i = 2; $i <= 100; $i++) {
            $source->reject($delivery, true, 'later');
            [$delivery] = $source->receive(1, 0);
            $this->assertTrue($delivery->isValid(), "delivery {$i}: claimed again in the same millisecond too");
            $this->assertSame($i, $delivery->deliveryCount);
        }
    }

    public function testADeadLetterKeepsTheOriginAndTheReasonAndAcknowledgesTheOriginal(): void
    {
        $source = $this->source([self::A], member: 'w1');
        $source->declare();
        $message = new Message(self::A, ['n' => 'm', 'card' => '4111'], orderingKey: 'lead:1', tenant: 'acme');
        $this->transport()->send([$message]);
        [$delivery] = $source->receive(10, 0);

        $source->reject($delivery, false, str_repeat('r', 2500));

        $this->assertSame(0, $source->inFlightCount());
        $this->assertSame([], $this->pending($this->prefix . '.' . self::A, 'agents'), 'the original is acknowledged');
        $dead = $this->entries($this->prefix . '.dead');
        $this->assertCount(1, $dead);
        [, $entryId] = explode('#', $delivery->brokerId, 2);
        $this->assertSame([
            'stream' => $this->prefix . '.' . self::A,
            'entry_id' => $entryId,
            'group' => 'agents',
            'consumer' => 'w1',
            'delivery_count' => '1',
            'reason' => str_repeat('r', 2000),
            'message_id' => $message->messageId,
            'name' => self::A,
            'ordering_key' => 'lead:1',
            'tenant' => 'acme',
            'body' => $message->toJson(),
        ], reset($dead));
        $this->assertSame([[
            'brokerId' => $delivery->brokerId,
            'messageId' => $message->messageId,
            'reason' => str_repeat('r', 2000),
            'deliveryCount' => 1,
            'deadLetterId' => (string) array_key_first($dead),
        ]], $source->deadLetters());
    }

    public function testADeadLetterOfAMessageWithoutKeyNorTenantLeavesThemOut(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm')]);
        [$delivery] = $source->receive(10, 0);
        $source->reject($delivery, false, 'refused');
        $this->assertSame(['stream', 'entry_id', 'group', 'consumer', 'delivery_count', 'reason', 'message_id', 'name', 'body'], array_keys((array) current($this->entries($this->prefix . '.dead'))));
    }

    public function testADeadLetterOfAnInvalidDeliveryHasNoMessageFields(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::A, '*', 'body', 'not json']);
        [$delivery] = $source->receive(10, 0);
        $this->assertFalse($delivery->isValid());
        $source->reject($delivery, false, (string) $delivery->invalidReason());
        $dead = (array) current($this->entries($this->prefix . '.dead'));
        $this->assertSame(['stream', 'entry_id', 'group', 'consumer', 'delivery_count', 'reason', 'body'], array_keys($dead));
        $this->assertSame('not json', $dead['body']);
        $this->assertSame([null], array_column($source->deadLetters(), 'messageId'));
    }

    public function testTheDeadLettersOfAGroupAreItsOwnInOrderAndLimited(): void
    {
        $source = $this->source([self::A]);
        $other = $this->source([self::A], group: 'audit');
        $source->declare();
        $other->declare();
        $messages = [];
        for ($i = 0; $i < 130; $i++) {
            $messages[] = $this->message(self::A, "m{$i}");
        }
        $this->transport()->send($messages);
        [$foreign] = $other->receive(1, 0);
        $other->reject($foreign, false, 'audit refused it');
        foreach ($source->receive(200, 0) as $delivery) {
            $source->reject($delivery, false, 'refused ' . $delivery->message()->payload['n']);
        }

        $all = $source->deadLetters(1000);
        $this->assertCount(130, $all);
        $this->assertSame('refused m0', $all[0]['reason']);
        $this->assertSame('refused m129', $all[129]['reason']);
        $this->assertSame(array_map(static fn(Message $m): string => $m->messageId, $messages), array_column($all, 'messageId'));
        $this->assertCount(120, $source->deadLetters(120));
        $this->assertSame(array_slice($all, 0, 1), $source->deadLetters(1));
        $this->assertCount(100, $source->deadLetters());
        $this->assertSame(['audit refused it'], array_column($other->deadLetters(), 'reason'));
        $this->assertSame([], $this->source([self::A], group: 'nobody')->deadLetters());
    }

    public function testDeadLettersTakeAPositiveLimit(): void
    {
        $this->expectExceptionObject(new \InvalidArgumentException('deadLetters() returns at least 1 dead letter, 0 asked.'));
        $this->source([self::A])->deadLetters(0);
    }

    public function testTheDeliveriesOfADeadMemberAreRecoveredAfterTheIdleTime(): void
    {
        $dead = $this->source([self::A, self::B], member: 'w1');
        $dead->declare();
        $this->transport()->send([$this->message(self::A, 'a1'), $this->message(self::B, 'b1'), $this->message(self::A, 'a2'), $this->message(self::A, 'a3'), $this->message(self::B, 'b2')]);
        $this->assertCount(5, $dead->receive(10, 0));

        $next = $this->source([self::A, self::B], member: 'w2', topology: $this->topology(recoverIdleMs: 60));
        $this->assertSame([], $next->receive(10, 0), 'not idle yet');
        usleep(90000);
        $recovered = $next->receive(2, 0);
        $this->assertSame(['a1', 'b1'], $this->labels($recovered));
        $this->assertSame([2, 2], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $recovered));
        $rest = [...$next->receive(2, 0), ...$next->receive(2, 0)];
        $this->assertSame(['a2', 'a3', 'b2'], $this->labels($rest));
        $this->assertSame([2, 2, 2], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $rest));
        $this->assertSame(['w2', 'w2', 'w2'], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 0));
    }

    public function testWithoutRecoveryAnIdleDeliveryStaysWithItsMember(): void
    {
        $dead = $this->source([self::A], member: 'w1');
        $dead->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        $this->assertCount(1, $dead->receive(10, 0));
        usleep(30000);
        $next = $this->source([self::A], member: 'w2', topology: $this->topology(recoverIdleMs: null));
        $this->assertSame([], $next->receive(10, 0));
        $this->assertSame(['w1'], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 0));
    }

    public function testRecoveryNeverHandsOutAgainWhatIsInFlightHere(): void
    {
        $source = $this->source([self::A], member: 'w1', topology: $this->topology(recoverIdleMs: 20));
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        [$delivery] = $source->receive(10, 0);
        usleep(40000);
        $this->assertSame([], $source->receive(10, 0));
        $this->assertSame(1, $source->inFlightCount());
        $source->ack($delivery);
        $this->assertSame([], $this->pending($this->prefix . '.' . self::A, 'agents'));
    }

    public function testARecoveredEntryKeepsTheCountOfRedisAndItsPlace(): void
    {
        $source = $this->source([self::A, self::B], member: 'w1', topology: $this->topology(recoverIdleMs: 20));
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1'), $this->message(self::B, 'b1'), $this->message(self::A, 'a2')]);
        $this->assertSame(['a1'], $this->labels($source->receive(1, 0)), 'b1 and a2 read, not handed out');
        usleep(40000);
        $later = $source->receive(1, 0);
        $this->assertSame(['b1'], $this->labels($later));
        $this->assertSame([2], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $later), 'claimed again by its own member: the counter of Redis');
    }

    public function testARestartedMemberTakesItsPendingDeliveriesAtOnce(): void
    {
        $first = $this->source([self::A], member: 'w1');
        $first->declare();
        $this->transport()->send([$this->message(self::A, 'a1'), $this->message(self::A, 'a2')]);
        $this->assertCount(2, $first->receive(10, 0));

        $again = $this->source([self::A], member: 'w1');
        $deliveries = $again->receive(10, 0);
        $this->assertSame(['a1', 'a2'], $this->labels($deliveries));
        $this->assertSame([2, 2], array_map(static fn(ReceivedMessage $d): int => $d->deliveryCount, $deliveries));
    }

    public function testAnEntryDeletedWhilePendingComesBackAsAnInvalidDelivery(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        [$delivery] = $source->receive(10, 0);
        $source->reject($delivery, true, 'later');
        [, $id] = explode('#', $delivery->brokerId, 2);
        $this->redis->executeRaw(['XDEL', $this->prefix . '.' . self::A, $id]);

        [$lost] = $source->receive(10, 0);
        $this->assertFalse($lost->isValid());
        $this->assertSame($delivery->brokerId, $lost->brokerId);
        $this->assertSame(2, $lost->deliveryCount);
        $this->assertSame(sprintf('Entry %s of stream "%s.%s" was deleted (trimmed by MAXLEN, or XDEL) while it was pending: its message is lost.', $id, $this->prefix, self::A), $lost->invalidReason());
        $source->reject($lost, false, (string) $lost->invalidReason());
        $this->assertSame(['stream', 'entry_id', 'group', 'consumer', 'delivery_count', 'reason'], array_keys((array) current($this->entries($this->prefix . '.dead'))));
        $this->assertSame([], $source->receive(10, 0));
    }

    public function testAnEntryDeletedWhilePendingInADeadMemberIsReportedByTheRecovery(): void
    {
        $dead = $this->source([self::A], member: 'w1');
        $dead->declare();
        $this->transport()->send([$this->message(self::A, 'a1'), $this->message(self::A, 'a2')]);
        [$delivery] = $dead->receive(10, 0);
        [, $id] = explode('#', $delivery->brokerId, 2);
        $this->redis->executeRaw(['XDEL', $this->prefix . '.' . self::A, $id]);
        usleep(40000);

        $next = $this->source([self::A], member: 'w2', topology: $this->topology(recoverIdleMs: 20));
        $deliveries = $next->receive(10, 0);
        $this->assertCount(2, $deliveries);
        $this->assertFalse($deliveries[0]->isValid());
        $this->assertSame(1, $deliveries[0]->deliveryCount);
        $this->assertStringStartsWith("Entry {$id} of stream", (string) $deliveries[0]->invalidReason());
        $this->assertSame('a2', $deliveries[1]->message()->payload['n']);
        $this->assertSame(2, $deliveries[1]->deliveryCount);
    }

    public function testAnEntryWithoutBodyIsAnInvalidDelivery(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $id = $this->redis->executeRaw(['XADD', $this->prefix . '.' . self::A, '*', 'payload', 'x']);
        [$delivery] = $source->receive(10, 0);
        $this->assertFalse($delivery->isValid());
        $this->assertSame(sprintf('Entry %s of stream "%s.%s" has no "body" field: it was not written by RedisStreamsTransport.', $id, $this->prefix, self::A), $delivery->invalidReason());
    }

    public function testAnUnreachableServerIsARedisStreamsException(): void
    {
        $source = new RedisStreamsSource(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]), 'agents', [self::A], 'w1', $this->topology);
        $this->expectExceptionObject(new RedisStreamsException('Redis Streams could not create the consumer group: ConnectionException: Connection refused [tcp://127.0.0.1:1].'));
        $source->receive(10, 0);
    }

    public function testALostConnectionWhileAnsweringIsARedisStreamsException(): void
    {
        $client = $this->client();
        $source = $this->source([self::A], client: $client);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        [$delivery] = $source->receive(10, 0);
        $this->kill($client);
        try {
            $source->ack($delivery);
            $this->fail('the connection is gone');
        } catch (RedisStreamsException $e) {
            $this->assertStringStartsWith(sprintf('Redis Streams could not acknowledge message "%s" (id %s), delivery %s: ', self::A, $delivery->message()->messageId, $delivery->brokerId), $e->getMessage());
        }
        $this->assertSame(1, $source->inFlightCount(), 'still in flight: it can be answered again');
        $source->ack($delivery);
        $this->assertSame(0, $source->inFlightCount());
    }

    public function testEveryRefusalOfRedisIsARedisStreamsExceptionWithTheCode(): void
    {
        $stream = $this->prefix . '.' . self::A;
        $this->redis->executeRaw(['SET', $stream, 'a string']);
        $source = $this->source([self::A]);
        $this->expectExceptionObject(new RedisStreamsException(sprintf('Redis refused to create the consumer group "agents" on stream "%s" (WRONGTYPE).', $stream)));
        $source->declare();
    }

    public function testRefusalsOfEachStepOfAReceive(): void
    {
        $stream = $this->prefix . '.' . self::A;
        $this->source([self::A])->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        $cases = [
            ['xautoclaim', sprintf('Redis refused to recover the idle deliveries on stream "%s" (NOPERM).', $stream), $this->topology],
            ['xpending', sprintf('Redis refused to read the pending deliveries on stream "%s" (NOPERM).', $stream), $this->topology(recoverIdleMs: null)],
            ['xreadgroup', sprintf('Redis refused to read new entries on stream "%s" (NOPERM).', $stream), $this->topology],
        ];
        foreach ($cases as [$command, $message, $topology]) {
            $source = $this->source([self::A], client: $this->clientWithout([$command]), topology: $topology);
            $source->declare();
            try {
                $source->receive(10, 0);
                $this->fail("{$command} refused");
            } catch (RedisStreamsException $e) {
                $this->assertSame($message, $e->getMessage());
            }
        }
    }

    public function testARefusedClaimOfADeliveryGivenBack(): void
    {
        $client = $this->clientWithout(['xclaim']);
        $source = $this->source([self::A], client: $client);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'a1')]);
        [$delivery] = $source->receive(10, 0);
        $source->reject($delivery, true, 'later');
        $this->expectExceptionObject(new RedisStreamsException(sprintf('Redis refused to claim the deliveries given back on stream "%s.%s" (NOPERM).', $this->prefix, self::A)));
        $source->receive(10, 0);
    }

    public function testRefusedAnswersAndReadings(): void
    {
        $this->transport()->send([]);
        $cases = [
            'xack' => static fn(RedisStreamsSource $s, ReceivedMessage $d) => $s->ack($d),
            'eval' => static fn(RedisStreamsSource $s, ReceivedMessage $d) => $s->reject($d, false, 'dead'),
            'xrange' => static fn(RedisStreamsSource $s, ReceivedMessage $d) => $s->deadLetters(),
        ];
        foreach ($cases as $command => $answer) {
            $source = $this->source([self::A], group: "g{$command}", client: $this->clientWithout([$command]));
            $source->declare();
            $message = $this->message(self::A, $command);
            $this->transport()->send([$message]);
            [$delivery] = $source->receive(10, 0);
            try {
                $answer($source, $delivery);
                $this->fail("{$command} refused");
            } catch (RedisStreamsException $e) {
                $stream = $command === 'xrange' ? $this->prefix . '.dead' : $this->prefix . '.' . self::A;
                $operation = match ($command) {
                    'xack' => 'acknowledge ' . $delivery->describe(),
                    'eval' => 'dead-letter ' . $delivery->describe(),
                    'xrange' => 'read the dead letters',
                };
                $this->assertSame(sprintf('Redis refused to %s on stream "%s" (NOPERM).', $operation, $stream), $e->getMessage());
            }
            $this->assertSame(1, $source->inFlightCount(), "{$command}: still in flight");
        }
    }

    public function testTheGroupIsDeclaredOnceBySource(): void
    {
        $source = $this->source([self::A]);
        $this->assertSame([], $source->receive(10, 0));
        $this->redis->executeRaw(['XGROUP', 'DESTROY', $this->prefix . '.' . self::A, 'agents']);
        $this->expectExceptionObject(new RedisStreamsException(sprintf('Redis refused to read the pending deliveries on stream "%s.%s" (NOGROUP).', $this->prefix, self::A)));
        $source->receive(10, 0);
    }

    public function testTheReasonOfADeadLetterIsCutAtTwoThousandCharacters(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm')]);
        [$delivery] = $source->receive(10, 0);
        $source->reject($delivery, false, str_repeat('é', 2500));
        $this->assertSame(str_repeat('é', 2000), $source->deadLetters()[0]['reason']);
    }

    public function testAForeignDeadLetterOfTheGroupIsReadWithDefaults(): void
    {
        $source = $this->source([self::A]);
        $id = $this->redis->executeRaw(['XADD', $this->prefix . '.dead', '*', 'group', 'agents']);
        $this->assertSame([['brokerId' => '#', 'messageId' => null, 'reason' => '', 'deliveryCount' => 0, 'deadLetterId' => $id]], $source->deadLetters());
    }

    public function testOnlyWhatCanBeHandedOutIsClaimedAgain(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm1'), $this->message(self::A, 'm2'), $this->message(self::A, 'm3')]);
        foreach ($source->receive(10, 0) as $delivery) {
            $source->reject($delivery, true, 'later');
        }
        $this->assertSame(['m1'], $this->labels($source->receive(1, 0)));
        $this->assertSame([2, 1, 1], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 1), 'm2 and m3 not claimed yet');
        $this->assertSame(['m2', 'm3'], $this->labels($source->receive(2, 0)));
        $this->assertSame([2, 2, 2], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 1));
    }

    public function testAtMostMaxDeliveriesGivenBackAreClaimedPerStream(): void
    {
        $source = $this->source([self::A]);
        $source->declare();
        $this->transport()->send([$this->message(self::A, 'm1'), $this->message(self::A, 'm2'), $this->message(self::A, 'm3')]);
        [$first, $second, $third] = $source->receive(3, 0);
        $source->reject($first, true, 'later');
        $source->reject($second, true, 'later');
        $this->assertSame(['m1'], $this->labels($source->receive(1, 0)), 'm3 still in flight');
        $this->assertSame([2, 1, 1], array_column($this->pending($this->prefix . '.' . self::A, 'agents'), 1), 'm2 not claimed yet');
        $source->ack($third);
    }

    public function testTheDeliveriesGivenBackAreListedPastWhatIsInFlightInTheirStream(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $transport = $this->transport();
        foreach ([[self::B, 'b1'], [self::B, 'b2'], [self::B, 'b3'], [self::A, 'a1']] as [$name, $label]) {
            $transport->send([$this->message($name, $label)]);
        }
        $deliveries = $source->receive(4, 0);
        $this->assertSame(['b1', 'b2', 'b3', 'a1'], $this->labels($deliveries));
        $source->reject($deliveries[3], true, 'later');
        $source->reject($deliveries[2], true, 'later');
        $this->assertSame(['b3', 'a1'], $this->labels($source->receive(2, 0)), 'b1 and b2 in flight before b3 in its stream');
    }

    public function testTheRecoveryWalksThePendingEntriesOfTheGroup(): void
    {
        $dead = $this->source([self::A], member: 'w1');
        $dead->declare();
        $messages = [];
        for ($i = 1; $i <= 30; $i++) {
            $messages[] = $this->message(self::A, "m{$i}");
        }
        $this->transport()->send($messages);
        $deliveries = $dead->receive(30, 0);
        usleep(80000);
        $ids = array_map(static fn(ReceivedMessage $d): string => explode('#', $d->brokerId, 2)[1], array_slice($deliveries, 0, 25));
        $this->redis->executeRaw(['XCLAIM', $this->prefix . '.' . self::A, 'agents', 'w3', 0, ...$ids, 'JUSTID']);

        // XAUTOCLAIM looks at most ten times COUNT entries per call: with receive(1), ten entries a call.
        $next = $this->source([self::A], member: 'w2', topology: $this->topology(recoverIdleMs: 50));
        $this->assertSame([], $next->receive(1, 0), 'm1..m10 are not idle');
        $this->assertSame([], $next->receive(1, 0), 'm11..m20 are not idle');
        $this->assertSame(['m26'], $this->labels($next->receive(1, 0)), 'the walk goes on where it stopped');
    }

    public function testEachStreamIsReadJustFarEnough(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $transport = $this->transport();
        foreach ([[self::A, 'a1'], [self::B, 'b1'], [self::B, 'b2'], [self::B, 'b3'], [self::A, 'a2'], [self::A, 'a3'], [self::A, 'a4'], [self::B, 'b4'], [self::B, 'b5']] as [$name, $label]) {
            $transport->send([$this->message($name, $label)]);
        }
        $read = fn(): array => [count($this->pending($this->prefix . '.' . self::A, 'agents')), count($this->pending($this->prefix . '.' . self::B, 'agents'))];

        $this->assertSame(['a1'], $this->labels($this->answer($source, $source->receive(1, 0))));
        $this->assertSame([0, 1], $read(), 'one entry of each stream read; b1 kept');
        $this->assertSame(['b1'], $this->labels($this->answer($source, $source->receive(1, 0))));
        $this->assertSame([1, 0], $read(), 'A read to know that a2 comes after b1; B not read again, b1 was known');
        $this->assertSame(['b2', 'b3'], $this->labels($this->answer($source, $source->receive(2, 0))));
        $this->assertSame([3, 0], $read(), 'two more of each read: a3 and a4 kept with a2');
        $this->assertSame(['a2', 'a3'], $this->labels($this->answer($source, $source->receive(2, 0))));
        $this->assertSame([1, 2], $read(), 'A had three known, so only B was read: b4 and b5');
        $this->assertSame(['a4', 'b4'], $this->labels($this->answer($source, $source->receive(2, 0))));
        $this->assertSame(['b5'], $this->labels($this->answer($source, $source->receive(2, 0))));
    }

    public function testAStreamIsReadOnlyForWhatIsMissing(): void
    {
        $source = $this->source([self::A, self::B]);
        $source->declare();
        $transport = $this->transport();
        $send = static function (array $labels) use ($transport): void {
            foreach ($labels as $label) {
                $transport->send([new Message($label[0] === 'a' ? self::A : self::B, ['n' => $label])]);
            }
        };
        $read = fn(): array => [count($this->pending($this->prefix . '.' . self::A, 'agents')), count($this->pending($this->prefix . '.' . self::B, 'agents'))];
        $send(['a1', 'b1', 'a2', 'b2']);
        $this->assertSame(['a1', 'b1'], $this->labels($this->answer($source, $source->receive(2, 0))));
        $this->assertSame([1, 1], $read(), 'a2 and b2 kept');
        $send(['a3', 'a4', 'a5', 'a6', 'b3', 'b4', 'b5', 'b6']);
        $this->assertSame(['a2', 'b2', 'a3'], $this->labels($this->answer($source, $source->receive(3, 0))));
        $this->assertSame([1, 2], $read(), 'one known of each: two more of each read (3 - 1)');
        $this->assertSame(['a4', 'a5', 'a6', 'b3', 'b4', 'b5', 'b6'], $this->labels($source->receive(10, 0)));
    }

    public function testTheMembersAndTheGroupAreChecked(): void
    {
        $source = $this->source([self::A], member: 'w1');
        $this->assertSame('agents', $source->getGroup());
        $this->assertSame('w1', $source->getConsumerName());
        $this->assertSame($this->topology, $source->getTopology());
        $this->assertSame(RedisStreamsTopology::DEFAULT_PREFIX, (new RedisStreamsSource($this->redis, 'agents', [self::A]))->getTopology()->prefix);
        $default = (new RedisStreamsSource($this->redis, 'agents', [self::A]))->getConsumerName();
        $this->assertMatchesRegularExpression(RedisStreamsSource::CONSUMER_NAME_PATTERN, $default);
        $this->assertStringEndsWith('-' . getmypid(), $default);
        $this->assertStringStartsWith(trim((string) preg_replace('/[^A-Za-z0-9._-]+/', '-', (string) gethostname()), '._-'), $default);
        $this->assertSame('W.1:a@b-c_d', $this->source([self::A], member: 'W.1:a@b-c_d')->getConsumerName());
        $this->assertSame(str_repeat('w', 120), $this->source([self::A], member: str_repeat('w', 120))->getConsumerName());
        foreach (['', '-w', 'w 1', str_repeat('w', 121), "w\n"] as $member) {
            try {
                $this->source([self::A], member: $member);
                $this->fail("member {$member}");
            } catch (\InvalidArgumentException $e) {
                $this->assertSame('The consumer name of a source must match ' . RedisStreamsSource::CONSUMER_NAME_PATTERN . '.', $e->getMessage());
            }
        }
    }

    public function testASourceNeedsAValidGroupAndAtLeastOneValidName(): void
    {
        foreach ([[[], 'A source reads the streams of at least one message name.'], [['Not.A.Name'], 'A stream is named after a message name: versioned and lowercase, module.fact.vN, at most 120 characters.']] as [$names, $message]) {
            try {
                new RedisStreamsSource($this->redis, 'agents', $names);
                $this->fail($message);
            } catch (\InvalidArgumentException $e) {
                $this->assertSame($message, $e->getMessage());
            }
        }
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The consumer name must match');
        new RedisStreamsSource($this->redis, 'Agents', [self::A]);
    }

    public function testReceiveTakesAtLeastOneAndNoNegativeWait(): void
    {
        $source = $this->source([self::A], client: new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]));
        foreach ([[0, 0, 'receive() takes at least 1 delivery, 0 given.'], [1, -1, 'The wait cannot be negative, -1 given.']] as [$max, $wait, $message]) {
            try {
                $source->receive($max, $wait);
                $this->fail($message);
            } catch (\InvalidArgumentException $e) {
                $this->assertSame($message, $e->getMessage());
            }
        }
    }

    /**
     * @param list<string> $names
     */
    private function source(array $names, string $group = 'agents', string $member = 'w1', ?RedisStreamsTopology $topology = null, ?ClientInterface $client = null): RedisStreamsSource
    {
        return new RedisStreamsSource($client ?? $this->redis, $group, $names, $member, $topology ?? $this->topology);
    }

    private function transport(): RedisStreamsTransport
    {
        return new RedisStreamsTransport($this->redis, $this->topology);
    }

    private function message(string $name, string $label): Message
    {
        return new Message($name, ['n' => $label]);
    }

    private function client(): Client
    {
        return \Antevemus\ASpecificationRedisStreams\Tests\Support\RedisServer::client();
    }

    /** Closes the connection of a client from the server side. */
    private function kill(Client $client): void
    {
        $id = $client->executeRaw(['CLIENT', 'ID']);
        $this->redis->executeRaw(['CLIENT', 'KILL', 'ID', (string) $id]);
    }

    /**
     * @param list<ReceivedMessage> $deliveries
     * @return list<string>
     */
    private function labels(array $deliveries): array
    {
        return array_map(static fn(ReceivedMessage $d): string => (string) $d->message()->payload['n'], $deliveries);
    }

    /**
     * Acknowledges every delivery.
     *
     * @param list<ReceivedMessage> $deliveries
     * @return list<ReceivedMessage>
     */
    private function answer(RedisStreamsSource $source, array $deliveries): array
    {
        foreach ($deliveries as $delivery) {
            $source->ack($delivery);
        }

        return $deliveries;
    }

    private function assertNotInFlight(\Closure $answer, ReceivedMessage $delivery): void
    {
        try {
            $answer();
            $this->fail('not in flight');
        } catch (MessagingException $e) {
            $this->assertNotInstanceOf(RedisStreamsException::class, $e);
            $this->assertSame(sprintf('Delivery %s is not in flight on this source: it was already answered, or it came from another one.', $delivery->brokerId), $e->getMessage());
        }
    }
}
