<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\Outbox\IMessageOutbox;
use Antevemus\ASpecification\Messaging\Outbox\MessageOutboxEntry;
use DateTimeImmutable;
use RuntimeException;

/**
 * An outbox whose process dies once between the confirmation of the broker and the mark: the next
 * markDelivered() throws, and nothing is marked.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class CrashingOutbox implements IMessageOutbox
{
    private bool $crash = false;

    public function __construct(private readonly IMessageOutbox $outbox)
    {
    }

    public function crashOnNextMarkDelivered(): self
    {
        $this->crash = true;

        return $this;
    }

    public function disarm(): self
    {
        $this->crash = false;

        return $this;
    }

    public function enqueue(array $entries): int
    {
        return $this->outbox->enqueue($entries);
    }

    public function fetchPending(int $limit = 100, ?int $maxAttempts = null): array
    {
        return $this->outbox->fetchPending($limit, $maxAttempts);
    }

    public function markDelivered(MessageOutboxEntry $entry, DateTimeImmutable $at): void
    {
        if ($this->crash) {
            $this->crash = false;
            throw new RuntimeException('The process died after the broker confirmed and before the outbox was marked.');
        }
        $this->outbox->markDelivered($entry, $at);
    }

    public function markFailed(MessageOutboxEntry $entry, string $reason, DateTimeImmutable $at): void
    {
        $this->outbox->markFailed($entry, $reason, $at);
    }

    public function find(string $messageId): ?MessageOutboxEntry
    {
        return $this->outbox->find($messageId);
    }

    public function findByOrderingKey(string $orderingKey): array
    {
        return $this->outbox->findByOrderingKey($orderingKey);
    }

    public function count(): int
    {
        return $this->outbox->count();
    }
}
