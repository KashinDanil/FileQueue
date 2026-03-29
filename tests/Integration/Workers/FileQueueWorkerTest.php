<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Tests\Integration\Workers;

use DanilKashin\FileQueue\Queue\FileQueue;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\FileQueue\Workers\FileQueueWorker;
use PHPUnit\Framework\TestCase;

final class FileQueueWorkerTest extends TestCase
{
    private string $dir;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/fq_worker_test_' . uniqid(more_entropy: true);
        mkdir($this->dir, 0777, true);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir . '/*') ?: [] as $file) {
            @unlink($file);
        }

        @rmdir($this->dir);
    }

    private function makeWorker(): FileQueueWorker
    {
        $dir = $this->dir;

        return new class($dir) extends FileQueueWorker {
            protected function processMessage(QueueMessage $message): bool { return true; }
            public function callGetQueues(): array { return $this->getQueues(); }
        };
    }

    public function testReturnsEmptyArrayWhenNoQueuesExist(): void
    {
        $queues = $this->makeWorker()->callGetQueues();

        $this->assertSame([], $queues);
    }

    public function testCreatesOneQueuePerDataFile(): void
    {
        touch($this->dir . '/foo.queue.data');
        touch($this->dir . '/bar.queue.data');

        $queues = $this->makeWorker()->callGetQueues();

        $this->assertCount(2, $queues);
    }

    public function testIgnoresNonDataFiles(): void
    {
        touch($this->dir . '/foo.queue.data');
        touch($this->dir . '/foo.queue.lock');
        touch($this->dir . '/foo.queue.pointer');
        touch($this->dir . '/unrelated.txt');

        $queues = $this->makeWorker()->callGetQueues();

        $this->assertCount(1, $queues);
    }

    public function testReturnedQueuesAreFileQueueInstances(): void
    {
        touch($this->dir . '/foo.queue.data');

        $queues = $this->makeWorker()->callGetQueues();

        $this->assertContainsOnlyInstancesOf(FileQueue::class, $queues);
    }
}