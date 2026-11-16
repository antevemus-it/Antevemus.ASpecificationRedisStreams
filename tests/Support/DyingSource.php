<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use RuntimeException;

/**
 * A source whose process dies at the n-th acknowledgement (0 = never): after the commit of the inbox row
 * and the effect, before the broker hears of it. What it received and did not answer stays in flight.
 * It records the message id of every delivery the consumer dead-lettered, a run that died included.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class DyingSource implements IMessageSource
{
    private int $acks = 0;

    /** @var list<?string> */
    public array $deadLettered = [];

    public function __construct(public readonly IMessageSource $source, private readonly int $dieAtAck)
    {
    }

    public function receive(int $max, int $waitMs): array
    {
        return $this->source->receive($max, $waitMs);
    }

    public function ack(ReceivedMessage $message): void
    {
        if (++$this->acks === $this->dieAtAck) {
            throw new RuntimeException('The process died before the acknowledgement.');
        }
        $this->source->ack($message);
    }

    public function reject(ReceivedMessage $message, bool $requeue, string $reason): void
    {
        $this->source->reject($message, $requeue, $reason);
        if (!$requeue) {
            $this->deadLettered[] = $message->isValid() ? $message->message()->messageId : null;
        }
    }
}
