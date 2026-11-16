<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\ConsumeEntry;
use Antevemus\ASpecification\Messaging\ConsumeReport;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecification\StateMachine\Persistence\InMemory\InMemoryTransactionManager;

/**
 * The path of a message through the core, the same for every broker: MessagePublisher and the
 * in-memory outbox, OutboxRelay, MessageConsumer with the in-memory inbox, and the effects of the
 * handlers committed with the inbox row (the in-memory transaction manager of the core). A message is
 * named by the value of its payload key `n`, so that a transcript reads the same on every broker.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class Harness
{
    public readonly InMemoryMessageOutbox $outbox;

    public readonly CrashingOutbox $crashingOutbox;

    public readonly MessagePublisher $publisher;

    public readonly InMemoryProcessedMessageStore $inbox;

    public readonly EffectLog $effects;

    public readonly InMemoryTransactionManager $transactions;

    public readonly MessageHandlerRegistry $handlers;

    /** @var array<string, string> message id => label */
    private array $labels = [];

    public function __construct(public readonly BrokerFixture $broker)
    {
        $this->outbox = new InMemoryMessageOutbox();
        $this->crashingOutbox = new CrashingOutbox($this->outbox);
        $this->publisher = new MessagePublisher($this->outbox);
        $this->inbox = new InMemoryProcessedMessageStore();
        $this->effects = new EffectLog();
        $this->transactions = new InMemoryTransactionManager([$this->inbox, $this->effects]);
        $this->handlers = new MessageHandlerRegistry();
    }

    /** Registers a handler that records the label of each message as its effect (then runs $also). */
    public function handle(string $name, ?\Closure $also = null): self
    {
        $this->handlers->register($name, function (Message $message) use ($also): void {
            $this->effects->record((string) ($message->payload['n'] ?? '?'));
            if ($also !== null) {
                $also($message);
            }
        });

        return $this;
    }

    /** Publishes messages, each labelled by its payload key `n`. */
    public function publish(Message ...$messages): int
    {
        foreach ($messages as $message) {
            $this->labels[$message->messageId] = (string) ($message->payload['n'] ?? '?');
        }

        return $this->transactions->transactional(fn(): int => $this->publisher->publish(...$messages));
    }

    /** A labelled message. */
    public function message(string $label, string $name, ?string $key = null, ?string $tenant = null, array $payload = []): Message
    {
        return new Message($name, ['n' => $label] + $payload, orderingKey: $key, tenant: $tenant);
    }

    public function relay(?IMessageTransport $transport = null, int $batchSize = 100, ?int $maxAttempts = null, bool $crashing = false): OutboxRelay
    {
        return new OutboxRelay($crashing ? $this->crashingOutbox : $this->outbox, $transport ?? $this->broker->transport(), $maxAttempts, $batchSize);
    }

    public function consumer(IMessageSource $source, int $maxDeliveries = 5, ?string $tenant = null, string $name = 'agents'): MessageConsumer
    {
        return new MessageConsumer($name, $source, $this->handlers, $this->inbox, $this->transactions, $maxDeliveries, $tenant);
    }

    /**
     * The transcript of a report: `<label> <outcome> <delivery count>` per delivery.
     *
     * @return list<string>
     */
    public function transcript(ConsumeReport $report): array
    {
        return array_map(fn(ConsumeEntry $entry): string => sprintf(
            '%s %s %d',
            $entry->messageId === null ? 'invalid' : ($this->labels[$entry->messageId] ?? 'unpublished'),
            $entry->outcome->value,
            $entry->deliveryCount
        ), $report->entries());
    }

    /** The label of a message id. */
    public function label(?string $messageId): string
    {
        return $messageId === null ? 'invalid' : ($this->labels[$messageId] ?? 'unpublished');
    }
}
