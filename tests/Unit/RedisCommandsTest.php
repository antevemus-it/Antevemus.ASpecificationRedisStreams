<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRedisStreams\Tests\Unit;

use Antevemus\ASpecificationRedisStreams\Internal\RedisCommands;
use Antevemus\ASpecificationRedisStreams\RedisStreamsException;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;
use Predis\Command\CommandInterface;
use Predis\Connection\ConnectionException;
use Predis\Connection\NodeConnectionInterface;
use Predis\Connection\Replication\ReplicationInterface;
use Predis\Protocol\ProtocolException;
use Predis\Response\Error;
use Predis\Response\Status;
use RuntimeException;

/**
 * RedisCommands (internal) and RedisStreamsException: one round trip per pipeline, the master of a
 * replicated connection, and the only texts of a failure that may reach a reason.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRedisStreams\Tests
 * @subpackage Unit
 */
final class RedisCommandsTest extends TestCase
{
    public function testEveryCommandIsWrittenBeforeAnyReplyIsRead(): void
    {
        $log = [];
        $connection = $this->createMock(NodeConnectionInterface::class);
        $connection->method('writeRequest')->willReturnCallback(static function (CommandInterface $command) use (&$log): void {
            $log[] = 'write ' . $command->getId() . ' ' . implode(' ', $command->getArguments());
        });
        $connection->method('readResponse')->willReturnCallback(static function (CommandInterface $command) use (&$log): mixed {
            $log[] = 'read ' . $command->getId();

            return $command->getId() === 'GET' ? new Error('WRONGTYPE Operation against a key holding the wrong kind of value') : 'reply of ' . $command->getId();
        });
        $client = $this->createStub(ClientInterface::class);
        $client->method('getConnection')->willReturn($connection);

        $replies = RedisCommands::pipeline($client, [['SET', 'k', 'v'], ['GET', 'k'], ['PING']]);

        $this->assertSame(['write SET k v', 'write GET k', 'write PING ', 'read SET', 'read GET', 'read PING'], $log);
        $this->assertSame('reply of SET', $replies[0]);
        $this->assertInstanceOf(Error::class, $replies[1]);
        $this->assertSame('reply of PING', $replies[2]);
    }

    public function testAReplicatedConnectionIsSwitchedToTheMaster(): void
    {
        $connection = $this->createMock(ReplicationInterface::class);
        $connection->expects($this->once())->method('switchToMaster');
        $connection->method('readResponse')->willReturn(new Status('OK'));
        $client = $this->createStub(ClientInterface::class);
        $client->method('getConnection')->willReturn($connection);

        $replies = RedisCommands::pipeline($client, [['PING']]);
        $this->assertCount(1, $replies);
        $this->assertInstanceOf(Status::class, $replies[0]);
    }

    public function testAFailedConnectionIsDescribedByItsClassAndItsMessage(): void
    {
        $connection = $this->createStub(NodeConnectionInterface::class);
        $failure = new ConnectionException($connection, 'Connection refused [tcp://127.0.0.1:1]');
        $this->assertSame('ConnectionException: Connection refused [tcp://127.0.0.1:1]', RedisCommands::describe($failure));
    }

    public function testAnyOtherFailureIsDescribedByItsClassOnly(): void
    {
        $connection = $this->createStub(NodeConnectionInterface::class);
        $this->assertSame('ProtocolException', RedisCommands::describe(new ProtocolException($connection, "Unknown response prefix: 'secret'.")));
        $this->assertSame('RuntimeException', RedisCommands::describe(new RuntimeException('secret')));
    }

    public function testTheErrorCodeIsTheFirstWordOfTheErrorReplyOnly(): void
    {
        $this->assertSame('WRONGTYPE', RedisCommands::errorCode(new Error('WRONGTYPE Operation against a key holding the wrong kind of value')));
        $this->assertSame('ERR', RedisCommands::errorCode(new Error("ERR unknown command 'XADD', with args beginning with: 'secret'")));
        $this->assertSame('NOPERM', RedisCommands::errorCode(new Error('NOPERM')));
        $this->assertSame('A' . str_repeat('B', 31), RedisCommands::errorCode(new Error('A' . str_repeat('B', 31) . ' text')));
        $this->assertSame('error reply', RedisCommands::errorCode(new Error('A' . str_repeat('B', 32) . ' text')));
        $this->assertSame('error reply', RedisCommands::errorCode(new Error('secret value')));
        $this->assertSame('error reply', RedisCommands::errorCode(new Error('9ERR text')));
        $this->assertSame('error reply', RedisCommands::errorCode(new Error("ERR\n text")));
        $this->assertSame('unexpected reply of type string', RedisCommands::errorCode('secret'));
        $this->assertSame('unexpected reply of type null', RedisCommands::errorCode(null));
        $this->assertSame('unexpected reply of type Predis\Response\Status', RedisCommands::errorCode(new Status('OK')));
    }

    public function testTheExceptionsOfTheSourceNameTheOperationAndTheCause(): void
    {
        $connection = $this->createStub(NodeConnectionInterface::class);
        $cause = new ConnectionException($connection, 'Connection refused [tcp://127.0.0.1:1]');
        $unreachable = RedisStreamsException::unreachable('read new entries', $cause);
        $this->assertSame('Redis Streams could not read new entries: ConnectionException: Connection refused [tcp://127.0.0.1:1].', $unreachable->getMessage());
        $this->assertSame($cause, $unreachable->getPrevious());

        $refused = RedisStreamsException::refused('read new entries', 'aspec.crm.lead_created.v1', new Error('NOGROUP No such key or consumer group'));
        $this->assertSame('Redis refused to read new entries on stream "aspec.crm.lead_created.v1" (NOGROUP).', $refused->getMessage());
        $this->assertNull($refused->getPrevious());
    }
}
