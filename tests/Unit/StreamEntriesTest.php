<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Unit;

use Antevemus\ASpecificationRedisStreams\Internal\StreamEntries;
use PHPUnit\Framework\TestCase;

/**
 * StreamEntries (internal): the fields of an entry, the order of the candidates, the broker id and the
 * default member name.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Unit
 */
final class StreamEntriesTest extends TestCase
{
    public function testTheFieldsOfAnEntry(): void
    {
        $this->assertSame(['seq' => '1', 'body' => '{}'], StreamEntries::fields(['seq', '1', 'body', '{}']));
        $this->assertSame(['a' => '1'], StreamEntries::fields(['a', '1', 'b']), 'a field without value is left out');
        $this->assertSame(['a' => '2'], StreamEntries::fields(['a', '1', 'a', '2']), 'the last value of a field');
        $this->assertSame(['a' => '1'], StreamEntries::fields([3 => 'a', 7 => '1']), 'whatever the keys of the list');
        $this->assertSame([], StreamEntries::fields(null));
        $this->assertSame([], StreamEntries::fields('a'));
        $this->assertSame([], StreamEntries::fields([]));
    }

    public function testTheGlobalOrderComesFirst(): void
    {
        $this->assertSame(-1, StreamEntries::compare(self::entry('b', '9-0', '1'), self::entry('a', '5-0', '2')));
        $this->assertSame(1, StreamEntries::compare(self::entry('a', '5-0', '10'), self::entry('b', '9-0', '9')), 'compared as numbers');
        $this->assertSame(-1, StreamEntries::compare(self::entry('b', '9-0', null), self::entry('a', '5-0', '1')), 'an entry without seq first');
        $this->assertSame(0, StreamEntries::compare(self::entry('a', '5-0', '0'), self::entry('a', '5-0', null)), 'a seq of 0 is the same as none');
        $this->assertSame(-1, StreamEntries::compare(self::entry('a', '5-0', 'x'), self::entry('a', '4-0', '1')), 'an invalid seq is none');
        $this->assertSame(-1, StreamEntries::compare(self::entry('a', '5-0', '-1'), self::entry('a', '4-0', '1')));
        $this->assertSame(-1, StreamEntries::compare(self::deleted('a', '5-0'), self::entry('a', '4-0', '1')), 'a deleted entry first');
    }

    public function testThenTheStreamThenTheEntryIdAsNumbers(): void
    {
        $this->assertSame(-1, StreamEntries::compare(self::entry('a', '9-0', null), self::entry('b', '1-0', null)));
        $this->assertSame(-1, StreamEntries::compare(self::entry('a', '7-3', null), self::entry('a', '7-12', null)));
        $this->assertSame(1, StreamEntries::compare(self::entry('a', '10-0', null), self::entry('a', '9-5', null)));
        $this->assertSame(-1, StreamEntries::compare(self::entry('a', '9-5', '3'), self::entry('a', '10-0', '3')));
        $this->assertSame(0, StreamEntries::compare(self::entry('a', '9-5', '3'), self::entry('a', '9-5', '3')));
        $this->assertSame([7, 12], StreamEntries::idParts('7-12'));
        $this->assertSame([1731650000000, 0], StreamEntries::idParts('1731650000000-0'));
    }

    public function testTheSequenceOfAnEntry(): void
    {
        $this->assertSame(42, StreamEntries::sequence(['seq' => '42']));
        $this->assertSame(0, StreamEntries::sequence(['seq' => '4a']));
        $this->assertSame(0, StreamEntries::sequence(['body' => '{}']));
        $this->assertSame(0, StreamEntries::sequence(null));
    }

    public function testTheBrokerId(): void
    {
        $this->assertSame('aspec.crm.lead_created.v1#5-0', StreamEntries::brokerId('aspec.crm.lead_created.v1', '5-0'));
    }

    public function testTheDefaultMemberName(): void
    {
        $this->assertSame('worker-01-123', StreamEntries::consumerName('worker-01', 123));
        $this->assertSame('web-1.example_com-7', StreamEntries::consumerName('._web 1.example_com-', 7));
        $this->assertSame('a-b-9', StreamEntries::consumerName('a  b', 9));
        $this->assertSame('host-9', StreamEntries::consumerName('', 9));
        $this->assertSame('host-9', StreamEntries::consumerName('...', 9));
        $long = StreamEntries::consumerName(str_repeat('h', 200), 12345);
        $this->assertSame(str_repeat('h', 120), $long);
        $this->assertSame(str_repeat('h', 114) . '-12345', StreamEntries::consumerName(str_repeat('h', 114), 12345));
    }

    /**
     * @return array{stream: string, id: string, fields: array<string, string>|null}
     */
    private static function entry(string $stream, string $id, ?string $seq): array
    {
        return ['stream' => $stream, 'id' => $id, 'fields' => $seq === null ? ['body' => '{}'] : ['seq' => $seq, 'body' => '{}']];
    }

    /**
     * @return array{stream: string, id: string, fields: null}
     */
    private static function deleted(string $stream, string $id): array
    {
        return ['stream' => $stream, 'id' => $id, 'fields' => null];
    }
}
