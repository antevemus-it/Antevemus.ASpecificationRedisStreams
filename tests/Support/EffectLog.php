<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Support;

use Antevemus\ASpecification\StateMachine\Persistence\InMemory\IInMemoryTransactional;

/**
 * The effects of the handlers, rolled back with the inbox by the in-memory transaction manager of the
 * core: what a handler writes through the connection of the inbox.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Support
 */
final class EffectLog implements IInMemoryTransactional
{
    /** @var list<string> */
    private array $effects = [];

    public function record(string $effect): void
    {
        $this->effects[] = $effect;
    }

    /** @return list<string> */
    public function all(): array
    {
        return $this->effects;
    }

    public function exportState(): array
    {
        return $this->effects;
    }

    public function importState(array $state): void
    {
        $this->effects = array_values($state);
    }
}
