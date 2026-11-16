<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Integration;

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTopology;
use Antevemus\ASpecificationRedisStreams\RedisStreamsTransport;
use Antevemus\ASpecificationRedisStreams\Tests\Support\RedisTestCase;
use PHPUnit\Framework\Attributes\Group;
use Predis\Client;

/**
 * RedisStreamsTransport against the server: the layout of an entry, the global order, the cap of the
 * streams, a refused message among confirmed ones, a lost connection, and the secrecy of the reasons.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Integration
 */
#[Group('redis')]
final class RedisStreamsTransportTest extends RedisTestCase
{
    public function testAnEntryHoldsTheWireFormAndTheFieldsOfTheBrokerSide(): void
    {
        $transport = new RedisStreamsTransport($this->redis, $this->topology);
        $this->assertSame($this->topology, $transport->getTopology());
        $full = new Message('crm.lead_created.v1', ['lead_id' => 7], orderingKey: 'lead:7', tenant: 'acme', headers: ['traceparent' => 'abc']);
        $bare = new Message('livechat.message_received.v1', ['chat_id' => 1]);

        $result = $transport->send([$full, $bare]);

        $this->assertSame([$full->messageId, $bare->messageId], $result->confirmedIds());
        $this->assertSame([], $result->failures());
        $leads = $this->entries($this->prefix . '.crm.lead_created.v1');
        $chats = $this->entries($this->prefix . '.livechat.message_received.v1');
        $this->assertSame([(string) $result->brokerIdOf($full->messageId)], array_keys($leads), 'the broker id is the entry id');
        $this->assertSame([(string) $result->brokerIdOf($bare->messageId)], array_keys($chats));
        $this->assertSame(
            ['seq' => '1', 'message_id' => $full->messageId, 'name' => 'crm.lead_created.v1', 'ordering_key' => 'lead:7', 'tenant' => 'acme', 'body' => $full->toJson()],
            reset($leads)
        );
        $this->assertSame(['seq' => '2', 'message_id' => $bare->messageId, 'name' => 'livechat.message_received.v1', 'body' => $bare->toJson()], reset($chats));
        $this->assertSame('2', $this->redis->executeRaw(['GET', $this->prefix . '.seq']));
        $this->assertSame($full->toArray(), Message::fromJson(reset($leads)['body'])->toArray(), 'the body reads back as the message');
    }

    public function testTheGlobalOrderGrowsAcrossBatchesAndStreams(): void
    {
        $transport = new RedisStreamsTransport($this->redis, $this->topology);
        $transport->send([new Message('a.b.v1'), new Message('a.c.v1')]);
        $transport->send([new Message('a.c.v1')]);
        $this->assertSame(['1'], array_column($this->entries($this->prefix . '.a.b.v1'), 'seq'));
        $this->assertSame(['2', '3'], array_column($this->entries($this->prefix . '.a.c.v1'), 'seq'));
    }

    public function testAnEmptyBatchSendsNothing(): void
    {
        $transport = new RedisStreamsTransport(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]), $this->topology);
        $result = $transport->send([]);
        $this->assertSame([], $result->confirmedIds());
        $this->assertSame([], $result->failures());
    }

    public function testTheBatchIsReadInOrderWhateverItsKeys(): void
    {
        $transport = new RedisStreamsTransport($this->redis, $this->topology);
        $first = new Message('a.b.v1', ['i' => 1]);
        $second = new Message('a.b.v1', ['i' => 2]);
        $result = $transport->send([5 => $first, 9 => $second]);
        $this->assertSame([$first->messageId, $second->messageId], $result->confirmedIds());
        $this->assertSame([$first->messageId, $second->messageId], array_column($this->entries($this->prefix . '.a.b.v1'), 'message_id'));
    }

    public function testTheCapTrimsTheStreamsApproximately(): void
    {
        $transport = new RedisStreamsTransport($this->redis, $this->topology(maxLen: 10));
        $batch = [];
        for ($i = 0; $i < 600; $i++) {
            $batch[] = new Message('a.b.v1', ['i' => $i]);
        }
        $this->assertCount(600, $transport->send($batch)->confirmedIds());
        $length = (int) $this->redis->executeRaw(['XLEN', $this->prefix . '.a.b.v1']);
        $this->assertGreaterThanOrEqual(10, $length);
        $this->assertLessThan(300, $length, 'trimmed (by whole nodes, so above the cap)');

        $uncapped = new RedisStreamsTransport($this->redis, $this->topology);
        $uncapped->send(array_map(static fn(int $i): Message => new Message('a.c.v1', ['i' => $i]), range(1, 600)));
        $this->assertSame(600, (int) $this->redis->executeRaw(['XLEN', $this->prefix . '.a.c.v1']), 'without a cap nothing is trimmed');
    }

    public function testARefusedMessageFailsAloneWithTheErrorCodeAndNoPayload(): void
    {
        $secret = 'S3CR3T-' . bin2hex(random_bytes(6));
        $this->redis->executeRaw(['SET', $this->prefix . '.a.wrong.v1', 'a string']);
        $transport = new RedisStreamsTransport($this->redis, $this->topology);
        $before = new Message('a.b.v1', ['card' => $secret]);
        $refused = new Message('a.wrong.v1', ['card' => $secret], orderingKey: 'k:1');
        $after = new Message('a.b.v1', ['card' => $secret]);

        $result = $transport->send([$before, $refused, $after]);

        $this->assertSame([$before->messageId, $after->messageId], $result->confirmedIds());
        $this->assertSame(
            [$refused->messageId => sprintf('Redis refused message "a.wrong.v1" (id %s, ordering key "k:1") on stream "%s.a.wrong.v1" (WRONGTYPE).', $refused->messageId, $this->prefix)],
            $result->failures()
        );
        $this->assertStringNotContainsString($secret, (string) $result->reasonOf($refused->messageId));
    }

    public function testACommandTheServerDoesNotAllowFailsEveryMessageWithItsCode(): void
    {
        $transport = new RedisStreamsTransport($this->clientWithout(['eval']), $this->topology);
        $message = new Message('a.b.v1');
        $this->assertSame(
            [$message->messageId => sprintf('Redis refused message "a.b.v1" (id %s) on stream "%s.a.b.v1" (NOPERM).', $message->messageId, $this->prefix)],
            $transport->send([$message])->failures()
        );
    }

    public function testALostConnectionFailsTheWholeBatchWithOneReason(): void
    {
        $transport = new RedisStreamsTransport(new Client(['host' => '127.0.0.1', 'port' => 1, 'timeout' => 1.0]), $this->topology);
        $batch = [new Message('a.b.v1', ['secret' => 'S3CR3T']), new Message('a.c.v1')];
        $result = $transport->send($batch);
        $this->assertSame([], $result->confirmedIds());
        $reason = 'Redis Streams did not confirm the batch of 2 message(s): ConnectionException: Connection refused [tcp://127.0.0.1:1].';
        $this->assertSame([$batch[0]->messageId => $reason, $batch[1]->messageId => $reason], $result->failures());
    }

    public function testTheDefaultTopologyIsThePrefixAspec(): void
    {
        $this->assertSame(RedisStreamsTopology::DEFAULT_PREFIX, (new RedisStreamsTransport($this->redis))->getTopology()->prefix);
    }
}
