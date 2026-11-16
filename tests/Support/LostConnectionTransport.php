<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\TransportSendResult;

/**
 * A transport whose connection is gone: every send() throws, as the in-memory broker of the core does
 * with failNextBatch().
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class LostConnectionTransport implements IMessageTransport
{
    public function send(array $batch): TransportSendResult
    {
        throw new MessagingException('The connection to the broker was lost.');
    }
}
