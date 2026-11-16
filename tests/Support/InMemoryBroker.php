<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\InMemory\InMemoryMessageSource;
use Antevemus\ASpecification\Messaging\InMemory\InMemoryMessageTransport;
use PHPUnit\Framework\Assert;

/**
 * The reference broker: InMemoryMessageTransport and InMemoryMessageSource of the core.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class InMemoryBroker implements BrokerFixture
{
    public readonly InMemoryMessageTransport $transport;

    public function __construct()
    {
        $this->transport = new InMemoryMessageTransport();
    }

    public function transport(): IMessageTransport
    {
        return $this->transport;
    }

    public function brokenTransport(): IMessageTransport
    {
        return new LostConnectionTransport();
    }

    public function source(string $group, array $names, string $member = 'm1'): IMessageSource
    {
        return new InMemoryMessageSource($this->transport);
    }

    public function storeRaw(string $name, string $body): void
    {
        $this->transport->storeRaw($body);
    }

    public function deadLetters(IMessageSource $source): array
    {
        Assert::assertInstanceOf(InMemoryMessageSource::class, $source);

        return array_map(
            static fn(array $dead): array => ['messageId' => $dead['messageId'], 'reason' => $dead['reason'], 'deliveryCount' => $dead['deliveryCount']],
            $source->deadLetters()
        );
    }

    public function abandon(IMessageSource $source, string $group, array $names): IMessageSource
    {
        Assert::assertInstanceOf(InMemoryMessageSource::class, $source);
        $source->recoverInFlight();

        return $source;
    }

    public function restart(IMessageSource $source, string $group, array $names): IMessageSource
    {
        return $this->abandon($source, $group, $names);
    }

    public function close(): void
    {
    }
}
