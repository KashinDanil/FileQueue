<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Tests\Integration\Queue;

use DanilKashin\FileLock\Exceptions\FileLockException;
use DanilKashin\FileQueue\Exceptions\QueueException;
use DanilKashin\FileQueue\Queue\FileQueue;
use DanilKashin\FileQueue\Queue\QueueMessage;
use PHPUnit\Framework\TestCase;

final class FileQueueTest extends TestCase
{
    private string $dir;

    private FileQueue $queue;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/fq_queue_test_' . uniqid(more_entropy: true);
        mkdir($this->dir, 0777, true);
        $this->queue = new FileQueue('test', $this->dir);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir . '/*') ?: [] as $file) {
            @unlink($file);
        }

        @rmdir($this->dir);
    }

    public function testIsEmptyOnFreshQueue(): void
    {
        $this->assertTrue($this->queue->isEmpty());
    }

    public function testSizeIsZeroOnFreshQueue(): void
    {
        $this->assertSame(0, $this->queue->size());
    }

    public function testDequeueReturnsNullOnFreshQueue(): void
    {
        $this->assertNull($this->queue->dequeue());
    }

    public function testFilesAbsentBeforeFirstEnqueue(): void
    {
        $this->assertFileDoesNotExist($this->dir . '/test.queue.data');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.pointer');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.lock');
    }

    public function testFilesCreatedAfterFirstEnqueue(): void
    {
        $this->queue->enqueue(new QueueMessage(['v' => 1]));

        $this->assertFileExists($this->dir . '/test.queue.data');
        $this->assertFileExists($this->dir . '/test.queue.pointer');
        $this->assertFileExists($this->dir . '/test.queue.lock');
    }

    public function testFilesRemovedAfterFullDrain(): void
    {
        $this->queue->enqueue(new QueueMessage(['v' => 1]));
        $this->queue->dequeue(); // consumes last message → triggers deleteAllFiles() immediately

        $this->assertFileDoesNotExist($this->dir . '/test.queue.data');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.pointer');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.lock');
    }

    public function testFifoOrder(): void
    {
        $this->queue->enqueue(new QueueMessage(['text' => 'first']));
        $this->queue->enqueue(new QueueMessage(['text' => 'второй']));
        $this->queue->enqueue(new QueueMessage(['text' => 'third with emoji 🤖']));

        $this->assertFalse($this->queue->isEmpty());
        $this->assertSame(3, $this->queue->size());
        $this->assertSame(['text' => 'first'], $this->queue->dequeue()->payload);
        $this->assertSame(['text' => 'второй'], $this->queue->dequeue()->payload);
        $this->assertSame(['text' => 'third with emoji 🤖'], $this->queue->dequeue()->payload);
        $this->assertTrue($this->queue->isEmpty());
        $this->assertNull($this->queue->dequeue());
    }

    public function testDequeueReturnsQueueMessageInstance(): void
    {
        $this->queue->enqueue(new QueueMessage(['x' => 1]));

        $this->assertInstanceOf(QueueMessage::class, $this->queue->dequeue());
    }

    public function testEmptyArrayPayloadRoundTrips(): void
    {
        $this->queue->enqueue(new QueueMessage([]));

        $this->assertSame([], $this->queue->dequeue()->payload);
    }

    public function testNestedArrayPayloadRoundTrips(): void
    {
        $payload = ['a' => ['b' => ['c' => 42]], 'list' => [1, 2, 3]];
        $this->queue->enqueue(new QueueMessage($payload));

        $this->assertSame($payload, $this->queue->dequeue()->payload);
    }

    public function testUtf8StringPreserved(): void
    {
        $this->queue->enqueue(new QueueMessage(['utf8' => 'Привет мир 🏐']));

        $this->assertSame('Привет мир 🏐', $this->queue->dequeue()->payload['utf8']);
    }

    public function testEmbeddedNewlinesPreserved(): void
    {
        $this->queue->enqueue(new QueueMessage(['body' => "line1\nline2\r\nline3"]));

        $this->assertSame("line1\nline2\r\nline3", $this->queue->dequeue()->payload['body']);
    }

    public function testBooleanFalsePreserved(): void
    {
        $this->queue->enqueue(new QueueMessage(['flag' => false]));

        $this->assertFalse($this->queue->dequeue()->payload['flag']);
    }

    public function testNullValuePreserved(): void
    {
        $this->queue->enqueue(new QueueMessage(['nothing' => null]));

        $this->assertNull($this->queue->dequeue()->payload['nothing']);
    }

    public function testSizeTracksUnreadMessagesOnly(): void
    {
        $this->queue->enqueue(new QueueMessage(['n' => 1]));
        $this->queue->enqueue(new QueueMessage(['n' => 2]));
        $this->queue->enqueue(new QueueMessage(['n' => 3]));

        $this->assertSame(3, $this->queue->size());
        $this->queue->dequeue();
        $this->assertSame(2, $this->queue->size());
        $this->queue->dequeue();
        $this->assertSame(1, $this->queue->size());
        $this->queue->dequeue();
        $this->assertSame(0, $this->queue->size());
    }

    public function testCompactPreservesUnreadMessages(): void
    {
        $this->queue->enqueue(new QueueMessage(['i' => 1]));
        $this->queue->enqueue(new QueueMessage(['i' => 2]));
        $this->queue->enqueue(new QueueMessage(['i' => 3]));
        $this->queue->dequeue();

        $this->queue->compact();

        $this->assertSame(2, $this->queue->size());
        $this->assertSame(['i' => 2], $this->queue->dequeue()->payload);
        $this->assertSame(['i' => 3], $this->queue->dequeue()->payload);
        $this->assertTrue($this->queue->isEmpty());
    }

    public function testCompactRemovesFilesWhenQueueIsEmpty(): void
    {
        $this->queue->enqueue(new QueueMessage(['x' => 1]));
        $this->queue->dequeue();
        $this->queue->compact();

        $this->assertFileDoesNotExist($this->dir . '/test.queue.data');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.pointer');
        $this->assertFileDoesNotExist($this->dir . '/test.queue.lock');
    }

    public function testInterleavedEnqueueDequeue(): void
    {
        $this->queue->enqueue(new QueueMessage(['v' => 'a']));
        $this->queue->enqueue(new QueueMessage(['v' => 'b']));

        $this->assertSame(['v' => 'a'], $this->queue->dequeue()->payload);

        $this->queue->enqueue(new QueueMessage(['v' => 'c']));

        $this->assertSame(['v' => 'b'], $this->queue->dequeue()->payload);
        $this->assertSame(['v' => 'c'], $this->queue->dequeue()->payload);
        $this->assertTrue($this->queue->isEmpty());
    }

    public function testLockFileCreatedOnFirstUse(): void
    {
        $this->queue->enqueue(new QueueMessage(['x' => 1]));

        $this->assertFileExists($this->dir . '/test.queue.lock');
    }

    public function testLockReleasedAfterOperation(): void
    {
        $this->queue->enqueue(new QueueMessage(['x' => 1]));

        $fh = fopen($this->dir . '/test.queue.lock', 'cb');
        $acquired = flock($fh, LOCK_EX | LOCK_NB);
        if ($acquired) {
            flock($fh, LOCK_UN);
        }
        fclose($fh);

        $this->assertTrue($acquired);
    }

    public function testConcurrentExclusiveLockIsRejected(): void
    {
        $this->queue->enqueue(new QueueMessage(['x' => 1]));
        $lockFile = $this->dir . '/test.queue.lock';

        $holder = fopen($lockFile, 'cb');
        flock($holder, LOCK_EX);
        $other = fopen($lockFile, 'cb');
        $blocked = !flock($other, LOCK_EX | LOCK_NB);
        fclose($other);
        flock($holder, LOCK_UN);
        fclose($holder);

        $this->assertTrue($blocked);
    }

    public function testEnqueueNonSerialisableValueThrowsQueueException(): void
    {
        $this->expectException(QueueException::class);

        $this->queue->enqueue(new QueueMessage(['fh' => fopen('php://memory', 'rb')]));
    }

    public function testEnqueueThrowsWhenDirectoryDoesNotExist(): void
    {
        $queue = new FileQueue('missing', sys_get_temp_dir() . '/fq_nonexistent_' . uniqid(more_entropy: true));

        $this->expectException(FileLockException::class);

        $queue->enqueue(new QueueMessage(['x' => 1]));
    }

    public function testReenqueueAfterFullDrain(): void
    {
        $this->queue->enqueue(new QueueMessage(['n' => 1]));
        $this->queue->dequeue(); // consumes last message — files still present
        $this->queue->dequeue(); // returns null → files deleted

        $this->assertTrue($this->queue->isEmpty());
        $this->assertSame(0, $this->queue->size());
        $this->assertNull($this->queue->dequeue());

        $this->queue->enqueue(new QueueMessage(['n' => 2]));

        $this->assertFalse($this->queue->isEmpty());
        $this->assertSame(1, $this->queue->size());
        $this->assertSame(['n' => 2], $this->queue->dequeue()->payload);
        $this->assertTrue($this->queue->isEmpty());
    }

    public function testMultipleEnqueuesDoNotCorruptData(): void
    {
        for ($i = 1; $i <= 5; ++$i) {
            $this->queue->enqueue(new QueueMessage(['i' => $i]));
        }

        $this->assertSame(5, $this->queue->size());

        for ($i = 1; $i <= 5; ++$i) {
            $this->assertSame(['i' => $i], $this->queue->dequeue()->payload);
        }

        $this->assertTrue($this->queue->isEmpty());
    }

    public function testSameMessagesAreNotMerged(): void
    {
        for ($i = 1; $i <= 5; ++$i) {
            $this->queue->enqueue(new QueueMessage(['i' => '123']));
        }

        $this->assertSame(5, $this->queue->size());

        for ($i = 1; $i <= 5; ++$i) {
            $this->assertSame(['i' => '123'], $this->queue->dequeue()->payload);
        }

        $this->assertTrue($this->queue->isEmpty());
    }
}