<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Property;

use Antevemus\ASpecification\Messaging\ConsumeEntry;
use Antevemus\ASpecification\Messaging\ConsumeOutcome;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\Tests\Support\BrokerFixture;
use Antevemus\ASpecificationRedisStreams\Tests\Support\DyingSource;
use Antevemus\ASpecificationRedisStreams\Tests\Support\Harness;
use Antevemus\ASpecificationRedisStreams\Tests\Support\InMemoryBroker;
use Antevemus\ASpecificationRedisStreams\Tests\Support\RedisStreamsBroker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Random\Engine\Mt19937;
use Random\Randomizer;
use RuntimeException;

/**
 * Random interleavings of publish, relay and consume, with fixed seeds, against Redis Streams on a real
 * server and against the reference of the core: the relay dies between the confirmation and the mark,
 * the consumer dies between the commit and the acknowledgement (and starts again with the same member),
 * the broker stores a message twice, a handler fails once or always. After a drain:
 * - no confirmed message is lost: every published message was handled or dead-lettered;
 * - no effect runs twice: every message was handled at most once;
 * - the order of each ordering key holds: the messages of a key were handled in the order they were
 *   published (a key whose message went to the dead letter queue is left out, RN-04);
 * - the dead letters are reported: the dead letters of the broker are exactly those of the reports.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Property
 */
final class MessagingPropertyTest extends TestCase
{
    public const SEEDS = [20261115, 6379, 42];

    public const STEPS = 150;

    private const NAMES = ['crm.lead_created.v1', 'crm.lead_updated.v1'];

    private const KEYS = ['lead:1', 'lead:2', 'lead:3', null];

    private const MAX_DELIVERIES = 3;

    private ?BrokerFixture $broker = null;

    /**
     * @return array<string, array{string, int}>
     */
    public static function cases(): array
    {
        $cases = [];
        foreach (['in-memory', 'redis'] as $broker) {
            foreach (self::SEEDS as $seed) {
                $cases["{$broker} seed {$seed}"] = [$broker, $seed];
            }
        }

        return $cases;
    }

    protected function tearDown(): void
    {
        $this->broker?->close();
    }

    #[DataProvider('cases')]
    public function testRandomInterleavingsKeepTheGuarantees(string $broker, int $seed): void
    {
        $this->broker = $broker === 'redis' ? new RedisStreamsBroker() : new InMemoryBroker();
        $random = new Randomizer(new Mt19937($seed));
        $harness = new Harness($this->broker);
        /** @var array<string, int> $failures message id => failures so far (outside the transaction) */
        $failures = [];
        foreach (self::NAMES as $name) {
            $harness->handle($name, static function (Message $message) use (&$failures): void {
                $mode = $message->payload['mode'];
                $failed = $failures[$message->messageId] ?? 0;
                if ($mode === 'poison' || ($mode === 'flaky' && $failed === 0)) {
                    $failures[$message->messageId] = $failed + 1;
                    throw new RuntimeException('the handler failed');
                }
            });
        }
        $source = $this->broker->source('agents', self::NAMES);
        /** @var list<Message> $published */
        $published = [];
        /** @var list<ConsumeEntry> $entries */
        $entries = [];
        $stats = ['crashes' => 0, 'deaths' => 0, 'duplicates' => 0];
        /** @var list<?string> $deadLettered every dead letter of the consumer, the runs that died included */
        $deadLettered = [];
        $consume = function (int $max) use (&$source, &$entries, &$stats, &$deadLettered, $harness, $random): int {
            $wrapper = new DyingSource($source, $random->getInt(1, 10) === 1 ? $random->getInt(1, 2) : 0);
            try {
                $report = $harness->consumer($wrapper, self::MAX_DELIVERIES)->consume($max);
            } catch (RuntimeException $e) {
                $this->assertSame('The process died before the acknowledgement.', $e->getMessage());
                $stats['deaths']++;
                $deadLettered = [...$deadLettered, ...$wrapper->deadLettered];
                $source = $this->broker->restart($source, 'agents', self::NAMES);

                return 1;
            }
            $this->assertSame(
                array_map(static fn(ConsumeEntry $e): ?string => $e->messageId, $report->of(ConsumeOutcome::DEAD_LETTERED)),
                $wrapper->deadLettered,
                'a run reports every dead letter it made'
            );
            $deadLettered = [...$deadLettered, ...$wrapper->deadLettered];
            $entries = [...$entries, ...$report->entries()];

            return $report->receivedCount();
        };

        for ($step = 0; $step < self::STEPS; $step++) {
            $action = $random->getInt(1, 20);
            if ($action <= 7) {
                $batch = [];
                for ($i = $random->getInt(1, 3); $i > 0; $i--) {
                    $roll = $random->getInt(1, 20);
                    $batch[] = new Message(
                        self::NAMES[$random->getInt(0, 1)],
                        ['n' => 'm' . (count($published) + count($batch)), 'mode' => $roll === 1 ? 'poison' : ($roll <= 4 ? 'flaky' : 'ok')],
                        orderingKey: self::KEYS[$random->getInt(0, 3)]
                    );
                }
                $harness->publish(...$batch);
                $published = [...$published, ...$batch];
            } elseif ($action <= 12) {
                $crash = $random->getInt(1, 6) === 1;
                if ($crash) {
                    $harness->crashingOutbox->crashOnNextMarkDelivered();
                }
                try {
                    $harness->relay(batchSize: $random->getInt(1, 3), crashing: true)->relayPending($random->getInt(1, 5));
                } catch (RuntimeException) {
                    $stats['crashes']++;
                }
                $harness->crashingOutbox->disarm(); // a crash that did not happen (nothing confirmed) is not kept
            } elseif ($action <= 19) {
                $consume($random->getInt(1, 4));
            } elseif ($published !== []) {
                $copy = $published[$random->getInt(0, count($published) - 1)];
                $entry = $harness->outbox->find($copy->messageId);
                if ($entry !== null && $entry->isDelivered()) {
                    $this->assertTrue($this->broker->transport()->send([$copy])->isConfirmed($copy->messageId));
                    $stats['duplicates']++;
                }
            }
        }

        // Drain: relay everything, then consume until the broker has nothing left.
        for ($i = 0; $i < 100 && $harness->outbox->fetchPending(1) !== []; $i++) {
            $harness->relay()->relayPending(100);
        }
        $this->assertSame([], $harness->outbox->fetchPending(1), 'the outbox is drained');
        for ($empty = 0, $i = 0; $empty < 3 && $i < 300; $i++) {
            $empty = $consume(10) === 0 ? $empty + 1 : 0;
        }
        $this->assertSame(3, $empty, 'the broker is drained');

        // The effects are the truth of what was handled: a run that died lost its report, not its commits.
        $labels = [];
        foreach ($published as $message) {
            $labels[(string) $message->payload['n']] = $message->messageId;
        }
        $handled = array_map(static fn(string $label): string => $labels[$label], $harness->effects->all());
        $dead = array_map('strval', $deadLettered);
        $deadIds = array_flip($dead);
        foreach ($published as $message) {
            $times = count(array_keys($handled, $message->messageId, true));
            $this->assertLessThanOrEqual(1, $times, "{$message->payload['n']}: no effect twice");
            $this->assertTrue($times === 1 || isset($deadIds[$message->messageId]), "{$message->payload['n']}: handled or dead-lettered, never lost");
            if ($message->payload['mode'] === 'poison') {
                $this->assertSame(0, $times, "{$message->payload['n']}: a poison message never commits");
            }
        }
        $this->assertSame(count(array_filter($entries, static fn(ConsumeEntry $e): bool => $e->outcome === ConsumeOutcome::HANDLED)) <= count($handled), true, 'every handled delivery reported has its effect');

        $index = array_flip(array_map(static fn(Message $m): string => $m->messageId, $published));
        $keyOf = [];
        foreach ($published as $message) {
            $keyOf[$message->messageId] = $message->orderingKey;
        }
        $deadKeys = [];
        foreach ($dead as $id) {
            $deadKeys[(string) $keyOf[$id]] = true;
        }
        $lastOfKey = [];
        foreach ($handled as $id) {
            $key = $keyOf[$id];
            if ($key === null || isset($deadKeys[$key])) {
                continue;
            }
            $this->assertGreaterThan($lastOfKey[$key] ?? -1, $index[$id], "key {$key}: handled in the order published");
            $lastOfKey[$key] = $index[$id];
        }

        $reported = $dead;
        $stored = array_map(static fn(array $d): string => (string) $d['messageId'], $this->broker->deadLetters($source));
        sort($reported);
        sort($stored);
        $this->assertSame($reported, $stored, 'the dead letters of the broker are those of the reports');

        $this->assertGreaterThan(60, count($published), 'the generator publishes');
        $this->assertNotSame([], $dead, 'the generator reaches dead letters');
        $this->assertGreaterThan(0, $stats['crashes'], 'the generator reaches crashes of the relay');
        $this->assertGreaterThan(0, $stats['deaths'], 'the generator reaches deaths of the consumer');
        $this->assertGreaterThan(0, $stats['duplicates'], 'the generator reaches copies stored by the broker');
        $this->assertNotSame([], array_filter($entries, static fn(ConsumeEntry $e): bool => $e->outcome === ConsumeOutcome::DUPLICATE), 'the inbox absorbs duplicates');
        $this->assertGreaterThan(0, count(array_filter($entries, static fn(ConsumeEntry $e): bool => $e->outcome === ConsumeOutcome::HELD_BACK)), 'the generator reaches held-back deliveries');
    }
}
