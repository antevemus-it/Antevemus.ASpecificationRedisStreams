<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Contract;

use Antevemus\ASpecification\Messaging\ConsumeEntry;
use Antevemus\ASpecification\Messaging\ConsumeOutcome;
use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\Tests\Support\BrokerFixture;
use Antevemus\ASpecificationRedisStreams\Tests\Support\Harness;
use DateTimeImmutable;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The contract battery of the messaging hook, written once and run against two brokers: the in-memory
 * reference of the core (InMemoryContractTest) and Redis Streams on a real server
 * (RedisStreamsContractTest). Every scenario goes the whole way of the core: MessagePublisher into the
 * in-memory outbox, OutboxRelay through the transport of the broker, MessageConsumer over its source
 * with the in-memory inbox, and asserts the same transcript on both (`<label> <outcome> <delivery
 * count>` per delivery, the effects of the handlers, the dead letters).
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Contract
 */
abstract class MessagingContractTestCase extends TestCase
{
    protected const LEAD = 'crm.lead_created.v1';

    protected const CHAT = 'livechat.message_received.v1';

    protected const CREATED = 'underwriting.quote_created.v1';

    protected const SENT = 'underwriting.quote_sent.v1';

    protected const DELETED = 'crm.lead_deleted.v1';

    protected BrokerFixture $broker;

    protected Harness $harness;

    abstract protected function broker(): BrokerFixture;

    protected function setUp(): void
    {
        $this->broker = $this->broker();
        $this->harness = new Harness($this->broker);
    }

    protected function tearDown(): void
    {
        if (isset($this->broker)) {
            $this->broker->close();
        }
    }

    public function testPublishRelayConsumeRoundTrip(): void
    {
        $source = $this->broker->source('agents', [self::LEAD, self::CHAT]);
        $received = [];
        $keep = function (Message $message) use (&$received): void {
            $received[] = $message;
        };
        $this->harness->handle(self::LEAD, $keep)->handle(self::CHAT, $keep);
        $lead = new Message(
            self::LEAD,
            ['n' => 'lead', 'name' => 'Ana', 'score' => 9.0, 'tags' => ['a', 'b'], 'none' => null, 'ok' => true],
            orderingKey: 'lead:1',
            tenant: 'acme',
            headers: ['traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'],
            occurredAt: new DateTimeImmutable('2026-11-15 10:00:00.123456 UTC')
        );
        $chat = new Message(self::CHAT, ['n' => 'chat', 'text' => 'Olá, 世界 / "quoted"']);
        $this->assertSame(2, $this->harness->publish($lead, $chat));

        $relay = $this->harness->relay();
        $first = $relay->relayPending(100);
        $this->assertSame(2, $first->deliveredCount());
        $this->assertSame(0, $first->failedCount());
        $this->assertSame(0, $relay->relayPending(100)->deliveredCount(), 'nothing is sent twice');

        $consumer = $this->harness->consumer($source);
        $this->assertSame(['lead handled 1', 'chat handled 1'], $this->harness->transcript($consumer->consume(10)));
        $this->assertSame([$lead->toArray(), $chat->toArray()], array_map(static fn(Message $m): array => $m->toArray(), $received), 'the messages arrive as published, to the microsecond');
        $this->assertSame([], $consumer->consume(10)->entries(), 'acknowledged deliveries do not come again');
        $this->assertSame(['lead', 'chat'], $this->harness->effects->all());
    }

    public function testPerKeyOrderAcrossNames(): void
    {
        $source = $this->broker->source('agents', [self::CREATED, self::SENT]);
        $this->harness->handle(self::CREATED)->handle(self::SENT);
        for ($i = 1; $i <= 4; $i++) {
            $this->harness->publish(
                $this->harness->message("a{$i}", $i % 2 === 1 ? self::CREATED : self::SENT, 'quote:1'),
                $this->harness->message("b{$i}", $i % 2 === 1 ? self::SENT : self::CREATED, 'quote:2'),
            );
        }
        $relayed = $this->harness->relay(batchSize: 3)->relayPending(100);
        $this->assertSame(8, $relayed->deliveredCount());
        $this->assertSame(4, $relayed->batchCount(), 'one message per key per batch');

        $consumer = $this->harness->consumer($source);
        $transcript = [];
        do {
            $round = $this->harness->transcript($consumer->consume(3));
            $transcript = [...$transcript, ...$round];
        } while ($round !== []);
        $this->assertSame(['a1 handled 1', 'b1 handled 1', 'a2 handled 1', 'b2 handled 1', 'a3 handled 1', 'b3 handled 1', 'a4 handled 1', 'b4 handled 1'], $transcript);
    }

    public function testARequeuedDeliveryComesBackBeforeTheLaterOnesOfItsKey(): void
    {
        $source = $this->broker->source('agents', [self::CREATED, self::CHAT]);
        $this->failOnce('k1');
        $this->harness->handle(self::CHAT);
        $this->harness->publish(
            $this->harness->message('k1', self::CREATED, 'quote:7'),
            $this->harness->message('l1', self::CHAT, 'chat:1'),
            $this->harness->message('k2', self::CREATED, 'quote:7'),
            $this->harness->message('k3', self::CREATED, 'quote:7'),
        );
        $this->assertSame(4, $this->harness->relay()->relayPending(100)->deliveredCount());

        $consumer = $this->harness->consumer($source);
        $this->assertSame(['k1 retried 1', 'l1 handled 1', 'k2 held_back 1', 'k3 held_back 1'], $this->harness->transcript($consumer->consume(10)));
        $this->assertSame(['k1 handled 2', 'k2 handled 2', 'k3 handled 2'], $this->harness->transcript($consumer->consume(10)));
        $this->assertSame([], $consumer->consume(10)->entries());
        $this->assertSame(['l1', 'k1', 'k2', 'k3'], $this->harness->effects->all());
    }

    public function testARequeuedDeliveryComesBackFirstOneDeliveryAtATime(): void
    {
        $source = $this->broker->source('agents', [self::CREATED, self::CHAT]);
        $this->failOnce('k1');
        $this->harness->handle(self::CHAT);
        $this->harness->publish(
            $this->harness->message('k1', self::CREATED, 'quote:7'),
            $this->harness->message('l1', self::CHAT, 'chat:1'),
            $this->harness->message('k2', self::CREATED, 'quote:7'),
            $this->harness->message('k3', self::CREATED, 'quote:7'),
        );
        $this->harness->relay()->relayPending(100);

        $consumer = $this->harness->consumer($source);
        $transcript = [];
        for ($i = 0; $i < 6; $i++) {
            $transcript[] = $this->harness->transcript($consumer->consume(1));
        }
        $this->assertSame([['k1 retried 1'], ['k1 handled 2'], ['l1 handled 1'], ['k2 handled 1'], ['k3 handled 1'], []], $transcript);
    }

    public function testDeadLetterAfterMaxDeliveries(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD, static function (Message $message): void {
            if ($message->payload['n'] === 'p1') {
                throw new RuntimeException('the downstream service is down');
            }
        });
        $this->harness->publish($this->harness->message('p1', self::LEAD, 'lead:5'), $this->harness->message('p2', self::LEAD, 'lead:5'));
        $this->harness->relay()->relayPending(100);

        $consumer = $this->harness->consumer($source, maxDeliveries: 3);
        $this->assertSame(['p1 retried 1', 'p2 held_back 1'], $this->harness->transcript($consumer->consume(10)));
        $this->assertSame(['p1 retried 2', 'p2 held_back 2'], $this->harness->transcript($consumer->consume(10)));
        $last = $consumer->consume(10);
        $this->assertSame(['p1 dead_lettered 3', 'p2 handled 3'], $this->harness->transcript($last));
        $this->assertSame([], $consumer->consume(10)->entries());

        $dead = $this->broker->deadLetters($source);
        $this->assertCount(1, $dead);
        $this->assertSame('p1', $this->harness->label($dead[0]['messageId']));
        $this->assertSame(3, $dead[0]['deliveryCount']);
        $this->assertSame($last->entries()[0]->reason, $dead[0]['reason']);
        $this->assertStringContainsString('(RuntimeException) on delivery 3 of at most 3', $dead[0]['reason']);
        $this->assertSame(['p2'], $this->harness->effects->all());
    }

    public function testAnInvalidBodyComesBackAsAnInvalidDelivery(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $this->broker->storeRaw(self::LEAD, 'not json');
        $this->broker->storeRaw(self::LEAD, '{"v":2,"id":"x"}');
        $this->harness->publish($this->harness->message('ok', self::LEAD, 'lead:1'));
        $this->harness->relay()->relayPending(100);

        $report = $this->harness->consumer($source)->consume(10);
        $this->assertSame(['invalid dead_lettered 1', 'invalid dead_lettered 1', 'ok handled 1'], $this->harness->transcript($report));
        foreach (array_slice($report->entries(), 0, 2) as $entry) {
            $this->assertStringStartsWith('The delivery is not a message of format 1: ', (string) $entry->reason);
        }
        $dead = $this->broker->deadLetters($source);
        $this->assertSame([null, null], array_column($dead, 'messageId'));
        $this->assertSame([1, 1], array_column($dead, 'deliveryCount'));
        $this->assertSame(array_map(static fn(ConsumeEntry $e): ?string => $e->reason, array_slice($report->entries(), 0, 2)), array_column($dead, 'reason'));
    }

    public function testAMessageWithoutHandlerIsAcknowledgedWithAWarning(): void
    {
        $source = $this->broker->source('agents', [self::LEAD, self::DELETED]);
        $this->harness->handle(self::LEAD);
        $this->harness->publish($this->harness->message('d1', self::DELETED, 'lead:1'), $this->harness->message('c1', self::LEAD, 'lead:1'));
        $this->harness->relay()->relayPending(100);

        $consumer = $this->harness->consumer($source);
        $report = $consumer->consume(10);
        $this->assertSame(['d1 unknown_name 1', 'c1 handled 1'], $this->harness->transcript($report));
        $this->assertCount(1, $report->warnings());
        $this->assertStringContainsString('has no handler for message "crm.lead_deleted.v1"', $report->warnings()[0]);
        $this->assertSame([], $consumer->consume(10)->entries());
    }

    public function testATenantRestrictedConsumerSkipsTheOtherTenants(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $this->harness->publish(
            $this->harness->message('a', self::LEAD, tenant: 'acme'),
            $this->harness->message('g', self::LEAD, tenant: 'globex'),
            $this->harness->message('z', self::LEAD),
        );
        $this->harness->relay()->relayPending(100);

        $consumer = $this->harness->consumer($source, tenant: 'acme');
        $this->assertSame(['a handled 1', 'g other_tenant 1', 'z other_tenant 1'], $this->harness->transcript($consumer->consume(10)));
        $this->assertSame([], $consumer->consume(10)->entries());
        $this->assertSame(['a'], $this->harness->effects->all());
    }

    public function testTheLargestPayloadTravelsWhole(): void
    {
        $source = $this->broker->source('agents', [self::CHAT]);
        $received = null;
        $this->harness->handle(self::CHAT, static function (Message $message) use (&$received): void {
            $received = $message;
        });
        $text = 'Olá, 世界 ';
        $base = strlen((string) json_encode(['n' => 'big', 'text' => $text], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        $message = new Message(self::CHAT, ['n' => 'big', 'text' => $text . str_repeat('a', Message::MAX_PAYLOAD_BYTES - $base)], orderingKey: 'chat:9');
        $this->assertSame(Message::MAX_PAYLOAD_BYTES, $message->payloadSize());
        $this->harness->publish($message);
        $this->assertSame(1, $this->harness->relay()->relayPending(100)->deliveredCount());

        $this->assertSame(['big handled 1'], $this->harness->transcript($this->harness->consumer($source)->consume(10)));
        $this->assertInstanceOf(Message::class, $received);
        $this->assertSame($message->payload, $received->payload);
        $this->assertSame($message->toJson(), $received->toJson());
    }

    public function testABatchOfMixedKeys(): void
    {
        $source = $this->broker->source('agents', [self::LEAD, self::CHAT]);
        $this->harness->handle(self::LEAD)->handle(self::CHAT);
        $keys = ['a', 'b', 'c', null];
        for ($i = 1; $i <= 12; $i++) {
            $key = $keys[($i - 1) % 4];
            $this->harness->publish($this->harness->message("m{$i}", $i % 3 === 0 ? self::CHAT : self::LEAD, $key === null ? null : "key:{$key}"));
        }
        $relayed = $this->harness->relay(batchSize: 5)->relayPending(100);
        $this->assertSame(12, $relayed->deliveredCount());
        $this->assertSame(3, $relayed->batchCount());

        $report = $this->harness->consumer($source)->consume(100);
        $this->assertSame(
            ['m1 handled 1', 'm2 handled 1', 'm3 handled 1', 'm4 handled 1', 'm8 handled 1', 'm5 handled 1', 'm6 handled 1', 'm7 handled 1', 'm12 handled 1', 'm9 handled 1', 'm10 handled 1', 'm11 handled 1'],
            $this->harness->transcript($report)
        );
    }

    public function testDuplicateDeliveriesAreAbsorbedByTheInbox(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $message = $this->harness->message('m', self::LEAD, 'lead:1');
        $this->harness->publish($message);
        $this->harness->relay()->relayPending(100);
        $this->assertTrue($this->broker->transport()->send([$message])->isConfirmed($message->messageId), 'the broker stores it again');

        $this->assertSame(['m handled 1', 'm duplicate 1'], $this->harness->transcript($this->harness->consumer($source)->consume(10)));
        $this->assertSame(['m'], $this->harness->effects->all());
    }

    public function testACrashBetweenTheConfirmationAndTheMarkSendsAgainAndTheEffectRunsOnce(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $message = $this->harness->message('m', self::LEAD, 'lead:1');
        $this->harness->publish($message);
        $this->harness->crashingOutbox->crashOnNextMarkDelivered();
        try {
            $this->harness->relay(crashing: true)->relayPending(100);
            $this->fail('the relay died');
        } catch (RuntimeException $e) {
            $this->assertStringContainsString('process died', $e->getMessage());
        }
        $this->assertFalse($this->harness->outbox->find($message->messageId)?->isDelivered(), 'not marked');
        $this->assertSame(1, $this->harness->relay(crashing: true)->relayPending(100)->deliveredCount(), 'sent again');
        $this->assertTrue($this->harness->outbox->find($message->messageId)?->isDelivered());

        $this->assertSame(['m handled 1', 'm duplicate 1'], $this->harness->transcript($this->harness->consumer($source)->consume(10)));
        $this->assertSame(['m'], $this->harness->effects->all(), 'the effect ran once');
    }

    public function testDeliveriesOfADeadMemberAreRecoveredByAnother(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $this->harness->publish(
            $this->harness->message('r1', self::LEAD, 'lead:3'),
            $this->harness->message('r2', self::LEAD, 'lead:3'),
            $this->harness->message('r3', self::LEAD, 'lead:4'),
        );
        $this->harness->relay()->relayPending(100);
        $this->assertCount(3, $source->receive(10, 0), 'the member took them and died before answering');

        $next = $this->broker->abandon($source, 'agents', [self::LEAD]);
        $consumer = $this->harness->consumer($next);
        $this->assertSame(['r1 handled 2', 'r3 handled 2', 'r2 handled 2'], $this->harness->transcript($consumer->consume(10)), 'in the order stored: r1 and r3 in the first batch, r2 in the second');
        $this->assertSame([], $consumer->consume(10)->entries());
    }

    public function testAProcessStartedAgainGetsWhatItLeftInFlight(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $this->harness->publish($this->harness->message('s1', self::LEAD, 'lead:3'), $this->harness->message('s2', self::LEAD, 'lead:3'));
        $this->harness->relay()->relayPending(100);
        $this->assertCount(1, $source->receive(1, 0));

        $restarted = $this->broker->restart($source, 'agents', [self::LEAD]);
        $this->assertSame(['s1 handled 2', 's2 handled 1'], $this->harness->transcript($this->harness->consumer($restarted)->consume(10)));
    }

    public function testALostConnectionFailsTheWholeBatchAndMarksNothing(): void
    {
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD);
        $messages = [$this->harness->message('c1', self::LEAD), $this->harness->message('c2', self::LEAD, 'lead:1'), $this->harness->message('c3', self::LEAD, 'lead:2')];
        $this->harness->publish(...$messages);

        $failed = $this->harness->relay($this->broker->brokenTransport())->relayPending(100);
        $this->assertSame(0, $failed->deliveredCount());
        $this->assertSame(3, $failed->failedCount());
        foreach ($messages as $message) {
            $entry = $this->harness->outbox->find($message->messageId);
            $this->assertFalse($entry?->isDelivered());
            $this->assertSame(1, $entry?->attempts);
            $this->assertNotSame('', $failed->getFailures()[$message->messageId] ?? '', 'every message fails with a reason');
        }
        $this->assertSame([], $this->harness->consumer($source)->consume(10)->entries(), 'nothing reached the broker');

        $this->assertSame(3, $this->harness->relay()->relayPending(100)->deliveredCount(), 'sent when the broker is back');
        $this->assertSame(['c1 handled 1', 'c2 handled 1', 'c3 handled 1'], $this->harness->transcript($this->harness->consumer($source)->consume(10)));
    }

    public function testNoReasonReportOrExceptionCarriesThePayload(): void
    {
        $secret = 'S3CR3T-' . bin2hex(random_bytes(6));
        $source = $this->broker->source('agents', [self::LEAD]);
        $this->harness->handle(self::LEAD, static function (Message $message) use ($secret): void {
            throw new RuntimeException("cannot handle {$secret}");
        });
        $message = new Message(self::LEAD, ['n' => 'x', 'card' => $secret, $secret => [$secret]], orderingKey: 'lead:1', tenant: 'acme', headers: ['x-note' => 'plain']);
        $this->harness->publish($message);
        $texts = [];

        $failed = $this->harness->relay($this->broker->brokenTransport())->relayPending(100);
        $texts[] = (string) json_encode($failed->toArray());
        $this->harness->relay()->relayPending(100);
        $report = $this->harness->consumer($source, maxDeliveries: 1)->consume(10);
        $this->assertSame(['x dead_lettered 1'], $this->harness->transcript($report));
        $texts[] = (string) json_encode($report->toArray());
        $texts[] = (string) json_encode($this->broker->deadLetters($source));

        $this->broker->storeRaw(self::LEAD, '{"v":1,"payload":"' . $secret . '"');
        $invalid = $this->harness->consumer($source)->consume(10);
        $this->assertSame(['invalid dead_lettered 1'], $this->harness->transcript($invalid));
        $texts[] = (string) json_encode($invalid->toArray());

        $this->broker->transport()->send([$message]);
        $deliveries = $source->receive(10, 0);
        $this->assertCount(1, $deliveries);
        $source->ack($deliveries[0]);
        try {
            $source->ack($deliveries[0]);
            $this->fail('a delivery is answered once');
        } catch (MessagingException $e) {
            $texts[] = $e->getMessage();
        }
        foreach ($texts as $text) {
            $this->assertStringNotContainsString($secret, $text);
        }
        $this->assertSame([ConsumeOutcome::DEAD_LETTERED], array_map(static fn(ConsumeEntry $e): ConsumeOutcome => $e->outcome, $report->entries()));
    }

    /** The handler of CREATED fails the first delivery of the message labelled $label. */
    private function failOnce(string $label): void
    {
        $failed = false;
        $this->harness->handle(self::CREATED, static function (Message $message) use ($label, &$failed): void {
            if ($message->payload['n'] === $label && !$failed) {
                $failed = true;
                throw new RuntimeException('a transient failure');
            }
        });
    }
}
