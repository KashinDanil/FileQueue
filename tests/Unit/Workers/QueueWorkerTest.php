<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Tests\Unit\Workers;

use DanilKashin\FileQueue\Queue\QueueInterface;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\FileQueue\Workers\QueueWorker;
use PHPUnit\Framework\TestCase;

final class QueueWorkerTest extends TestCase
{
    /**
     * Returns a concrete QueueWorker with configurable processMessage() and shouldStop().
     * Exposes callTick() to drive tick() directly from tests.
     *
     * @param array $queues
     * @param callable $processMessage
     * @param callable|null $shouldStop defaults to always returning false
     *
     * @return QueueWorker
     */
    private function makeWorker(
        array $queues,
        callable $processMessage,
        ?callable $shouldStop = null,
    ): QueueWorker {
        $shouldStop ??= static fn(): bool => false;

        return new class($queues, $processMessage, $shouldStop) extends QueueWorker {
            public function __construct(
                private readonly array $queues,
                private readonly mixed $processFn,
                private readonly mixed $shouldStopFn,
            ) {
                parent::__construct();
            }

            protected function getQueues(): array { return $this->queues; }
            protected function processMessage(QueueMessage $message): bool { return ($this->processFn)($message); }
            protected function shouldStop(): bool { return ($this->shouldStopFn)(); }
            public function callTick(): void { $this->tick(); }
        };
    }

    public function testProcessMessageIsCalledForEachMessage(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturnOnConsecutiveCalls(false, false, false, true);
        $queue->method('dequeue')->willReturn(new QueueMessage(['v' => 1]));

        $callCount = 0;

        $this->makeWorker([$queue], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(3, $callCount);
    }

    public function testBatchLimitStopsAfterTenSuccessfulMessages(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->expects($this->exactly(10))->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 1]));

        $this->makeWorker([$queue], static fn(): bool => true)->callTick();
    }

    public function testFailedMessageIsStillDequeued(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturnOnConsecutiveCalls(false, true);
        $queue->expects($this->once())->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 1]));

        $this->makeWorker([$queue], static fn(): bool => false)->callTick();
    }

    public function testFailedMessageDoesNotCountTowardBatchLimit(): void
    {
        // First 5 messages fail, next 10 succeed → 15 total dequeues before batch limit kicks in
        $callCount = 0;

        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->expects($this->exactly(15))->method('dequeue')
            ->willReturn(new QueueMessage(['v' => 1]));

        $this->makeWorker([$queue], function () use (&$callCount): bool {
            return ++$callCount > 5;
        })->callTick();
    }

    public function testAllQueuesAreProcessedInOneTick(): void
    {
        $makeQueue = function (int $messageCount): QueueInterface {
            $isEmpty = array_merge(array_fill(0, $messageCount, false), [true]);
            $queue   = $this->createMock(QueueInterface::class);
            $queue->method('isEmpty')->willReturnOnConsecutiveCalls(...$isEmpty);
            $queue->method('dequeue')->willReturn(new QueueMessage(['v' => 1]));

            return $queue;
        };

        $callCount = 0;

        $this->makeWorker([$makeQueue(2), $makeQueue(3)], function () use (&$callCount): bool {
            $callCount++;
            return true;
        })->callTick();

        $this->assertSame(5, $callCount);
    }

    public function testStopSignalBreaksOutOfProcessingLoop(): void
    {
        $processCount = 0;

        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->method('dequeue')->willReturn(new QueueMessage(['v' => 1]));

        $this->makeWorker(
            [$queue],
            function () use (&$processCount): bool {
                $processCount++;
                return true;
            },
            function () use (&$processCount): bool {
                return $processCount >= 3;
            },
        )->callTick();

        $this->assertSame(3, $processCount);
    }

    public function testDequeueReturningNullBreaksInnerLoop(): void
    {
        $queue = $this->createMock(QueueInterface::class);
        $queue->method('isEmpty')->willReturn(false);
        $queue->method('dequeue')->willReturn(null);

        $wasCalled = false;

        $this->makeWorker([$queue], function () use (&$wasCalled): bool {
            $wasCalled = true;
            return true;
        })->callTick();

        $this->assertFalse($wasCalled);
    }
}