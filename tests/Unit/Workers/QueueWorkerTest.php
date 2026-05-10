<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Tests\Unit\Workers;

use DanilKashin\FileQueue\Queue\QueueInterface;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\FileQueue\Workers\QueueWorker;
use PHPUnit\Framework\TestCase;

final class QueueWorkerTest extends TestCase
{
    private function makeWorker(
        array $queues,
        callable $processMessage,
        ?int $perQueueLimit = null,
    ): QueueWorker {
        return new class($queues, $processMessage, $perQueueLimit) extends QueueWorker {
            public function __construct(
                private readonly array $queues,
                private readonly mixed $processFn,
                private readonly ?int $perQueueLimit,
            ) {
                parent::__construct();
            }

            protected function getQueues(): array { return $this->queues; }
            protected function processMessage(QueueMessage $message): bool { return ($this->processFn)($message); }
            protected function getMessagesPerQueueLimit(): int
            {
                return $this->perQueueLimit ?? parent::getMessagesPerQueueLimit();
            }
            public function callTick(): void { $this->tick(); }
        };
    }

    public function testTickProcessesExactlyOneMessage(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->expects($this->once())->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 1]));

        $callCount = 0;

        $this->makeWorker([$queue], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(1, $callCount);
    }

    public function testWorkerStaysOnQueueUntilLimitThenAdvances(): void
    {
        $busy = $this->createMock(QueueInterface::class);
        $busy->method('isEmpty')->willReturn(false);
        $busy->method('dequeue')->willReturn(new QueueMessage(['q' => 'busy']));

        $other = $this->createMock(QueueInterface::class);
        $other->method('isEmpty')->willReturn(false);
        $other->method('dequeue')->willReturn(new QueueMessage(['q' => 'other']));

        $seen = [];
        $worker = $this->makeWorker(
            [$busy, $other],
            function (QueueMessage $m) use (&$seen): bool {
                $seen[] = $m->payload['q'];
                return true;
            },
            perQueueLimit: 3,
        );

        for ($i = 0; $i < 7; $i++) {
            $worker->callTick();
        }

        // 3 from busy (limit hit), 3 from other (limit hit), generator ends,
        // a new pass starts: 1 more from busy.
        $this->assertSame(['busy', 'busy', 'busy', 'other', 'other', 'other', 'busy'], $seen);
    }

    public function testFailedMessageDoesNotCountTowardLimit(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->method('dequeue')->willReturn(new QueueMessage(['v' => 1]));

        $callCount = 0;
        $worker = $this->makeWorker(
            [$queue],
            // First 5 fail, then everything succeeds.
            function () use (&$callCount): bool {
                return ++$callCount > 5;
            },
            perQueueLimit: 3,
        );

        // 5 failed + 3 successful = 8 dequeues before the limit is reached.
        for ($i = 0; $i < 8; $i++) {
            $worker->callTick();
        }

        $this->assertSame(8, $callCount);
    }

    public function testEmptyQueueIsSkippedToNextQueue(): void
    {
        $first = $this->createMock(QueueInterface::class);
        $first->method('isEmpty')->willReturn(true);
        $first->expects($this->never())->method('dequeue');

        $second = $this->createMock(QueueInterface::class);
        $second->method('isEmpty')->willReturn(false);
        $second->expects($this->once())->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 2]));

        $callCount = 0;

        $this->makeWorker([$first, $second], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(1, $callCount);
    }

    public function testNullDequeueAdvancesToNextQueue(): void
    {
        $first = $this->createMock(QueueInterface::class);
        $first->method('isEmpty')->willReturn(false);
        $first->method('dequeue')->willReturn(null);

        $second = $this->createMock(QueueInterface::class);
        $second->method('isEmpty')->willReturn(false);
        $second->expects($this->once())->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 2]));

        $callCount = 0;

        $this->makeWorker([$first, $second], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(1, $callCount);
    }

    public function testTickIsNoOpWhenAllQueuesEmpty(): void
    {
        $first = $this->createMock(QueueInterface::class);
        $first->method('isEmpty')->willReturn(true);
        $first->expects($this->never())->method('dequeue');

        $second = $this->createMock(QueueInterface::class);
        $second->method('isEmpty')->willReturn(true);
        $second->expects($this->never())->method('dequeue');

        $callCount = 0;

        $this->makeWorker([$first, $second], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(0, $callCount);
    }

    public function testNoQueuesIsNoOp(): void
    {
        $callCount = 0;

        $this->makeWorker([], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(0, $callCount);
    }
}
