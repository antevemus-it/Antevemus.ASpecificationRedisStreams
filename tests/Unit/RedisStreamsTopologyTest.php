<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Unit;

use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * RedisStreamsTopology: the defaults, the keys and every boundary of the values.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Unit
 */
final class RedisStreamsTopologyTest extends TestCase
{
    public function testDefaults(): void
    {
        $topology = new RedisStreamsTopology();
        $this->assertSame('aspec', $topology->prefix);
        $this->assertNull($topology->maxLen);
        $this->assertSame(60000, $topology->recoverIdleMs);
        $this->assertSame(5000, $topology->maxBlockMs);
        $this->assertSame('aspec.crm.lead_created.v1', $topology->streamOf('crm.lead_created.v1'));
        $this->assertSame('aspec.dead', $topology->deadLetterStream());
        $this->assertSame('aspec.seq', $topology->sequenceKey());
    }

    public function testTheKeysFollowThePrefix(): void
    {
        $topology = new RedisStreamsTopology(prefix: '{shop}', maxLen: 1, recoverIdleMs: 1, maxBlockMs: 1);
        $this->assertSame('{shop}.billing.invoice_overdue.v12', $topology->streamOf('billing.invoice_overdue.v12'));
        $this->assertSame('{shop}.dead', $topology->deadLetterStream());
        $this->assertSame('{shop}.seq', $topology->sequenceKey());
        $this->assertSame(1, $topology->maxLen);
        $this->assertSame(1, $topology->recoverIdleMs);
        $this->assertSame(1, $topology->maxBlockMs);
        $this->assertNull((new RedisStreamsTopology(recoverIdleMs: null))->recoverIdleMs);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function validPrefixes(): array
    {
        return [
            'one character' => ['a'],
            'sixty-four characters' => [str_repeat('p', 64)],
            'every allowed character' => ['Az09_.:{}-x'],
            'a hash tag' => ['{aspec}'],
            'a dot inside' => ['app.messages'],
        ];
    }

    #[DataProvider('validPrefixes')]
    public function testValidPrefixes(string $prefix): void
    {
        $this->assertSame($prefix, (new RedisStreamsTopology(prefix: $prefix))->prefix);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function invalidPrefixes(): array
    {
        return [
            'empty' => [''],
            'sixty-five characters' => [str_repeat('p', 65)],
            'a trailing dot' => ['aspec.'],
            'only a dot' => ['.'],
            'a space' => ['as pec'],
            'a glob character' => ['as*pec'],
            'a newline at the end' => ["aspec\n"],
            'a slash' => ['as/pec'],
        ];
    }

    #[DataProvider('invalidPrefixes')]
    public function testInvalidPrefixes(string $prefix): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The prefix must have 1 to 64 characters of [A-Za-z0-9_.:{}-] and not end with a dot.');
        new RedisStreamsTopology(prefix: $prefix);
    }

    public function testMaxLenBelowOneIsRefused(): void
    {
        $this->expectExceptionObject(new InvalidArgumentException('maxLen must be null or at least 1, 0 given.'));
        new RedisStreamsTopology(maxLen: 0);
    }

    public function testRecoverIdleBelowOneIsRefused(): void
    {
        $this->expectExceptionObject(new InvalidArgumentException('recoverIdleMs must be null or at least 1, 0 given.'));
        new RedisStreamsTopology(recoverIdleMs: 0);
    }

    public function testMaxBlockBelowOneIsRefused(): void
    {
        $this->expectExceptionObject(new InvalidArgumentException('maxBlockMs must be at least 1, 0 given.'));
        new RedisStreamsTopology(maxBlockMs: 0);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function invalidNames(): array
    {
        return [
            'no version' => ['crm.lead_created'],
            'capital letters' => ['CRM.lead_created.v1'],
            'one segment' => ['crm.v1'],
            'longer than 120' => [str_repeat('a', 118) . '.b.v1'],
            'a key of the package' => ['dead'],
        ];
    }

    #[DataProvider('invalidNames')]
    public function testAStreamIsNamedAfterAValidMessageName(string $name): void
    {
        $this->expectExceptionObject(new InvalidArgumentException('A stream is named after a message name: versioned and lowercase, module.fact.vN, at most 120 characters.'));
        (new RedisStreamsTopology())->streamOf($name);
    }
}
