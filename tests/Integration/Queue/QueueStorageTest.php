<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Tests\Integration\Queue;

use DanilKashin\FileQueue\Exceptions\QueueException;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\FileQueue\Queue\QueueStorage;
use PHPUnit\Framework\TestCase;

final class QueueStorageTest extends TestCase
{
    private string $dataFile;
    private string $pointerFile;
    private QueueStorage $storage;

    protected function setUp(): void
    {
        $base              = sys_get_temp_dir() . '/fq_storage_test_' . uniqid(more_entropy: true);
        $this->dataFile    = $base . '.data';
        $this->pointerFile = $base . '.pointer';
        $this->storage     = new QueueStorage($this->dataFile, $this->pointerFile);
    }

    protected function tearDown(): void
    {
        @unlink($this->dataFile);
        @unlink($this->pointerFile);
        @unlink($this->dataFile . '.tmp');
        @unlink($this->pointerFile . '.tmp');
    }

    public function testExistsReturnsFalseWhenFilesAbsent(): void
    {
        $this->assertFalse($this->storage->exists());
    }

    public function testExistsReturnsTrueAfterInit(): void
    {
        $this->storage->init();

        $this->assertTrue($this->storage->exists());
    }

    public function testExistsCachesResultAfterInit(): void
    {
        $this->storage->init();
        unlink($this->dataFile); // delete from disk without going through storage

        // cache still says true — no filesystem re-check
        $this->assertTrue($this->storage->exists());
    }

    public function testExistsReturnsFalseAfterDeleteFiles(): void
    {
        $this->storage->init();
        $this->storage->deleteFiles();

        $this->assertFalse($this->storage->exists());
    }

    public function testExistsReturnsFalseWhenOnlyDataFileExists(): void
    {
        touch($this->dataFile);

        $this->assertFalse($this->storage->exists());
    }

    public function testExistsReturnsFalseWhenOnlyPointerFileExists(): void
    {
        file_put_contents($this->pointerFile, '0');

        $this->assertFalse($this->storage->exists());
    }

    public function testInitCreatesDataAndPointerFiles(): void
    {
        $this->storage->init();

        $this->assertFileExists($this->dataFile);
        $this->assertFileExists($this->pointerFile);
    }

    public function testInitWritesZeroToPointerFile(): void
    {
        $this->storage->init();

        $this->assertSame('0', file_get_contents($this->pointerFile));
    }

    public function testInitIsIdempotent(): void
    {
        $this->storage->init();
        file_put_contents($this->dataFile, 'sentinel');
        $this->storage->init(); // second call must not overwrite

        $this->assertSame('sentinel', file_get_contents($this->dataFile));
    }

    public function testDeleteFilesRemovesBothFiles(): void
    {
        $this->storage->init();
        $this->storage->deleteFiles();

        $this->assertFileDoesNotExist($this->dataFile);
        $this->assertFileDoesNotExist($this->pointerFile);
    }

    public function testDeleteFilesResetsCacheForSubsequentInit(): void
    {
        $this->storage->init();
        $this->storage->deleteFiles();
        $this->storage->init(); // should re-create, not skip

        $this->assertFileExists($this->dataFile);
        $this->assertFileExists($this->pointerFile);
    }

    public function testShiftReturnsNullOnEmptyStorage(): void
    {
        $this->storage->init();

        $this->assertNull($this->storage->shift());
    }

    public function testShiftReturnsMessageAndAdvancesPointer(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));

        $message = $this->storage->shift();

        $this->assertInstanceOf(QueueMessage::class, $message);
        $this->assertSame(['v' => 1], $message->payload);
    }

    public function testShiftDequeuesInFifoOrder(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 'a']));
        $this->storage->append(new QueueMessage(['v' => 'b']));
        $this->storage->append(new QueueMessage(['v' => 'c']));

        $this->assertSame(['v' => 'a'], $this->storage->shift()->payload);
        $this->assertSame(['v' => 'b'], $this->storage->shift()->payload);
        $this->assertSame(['v' => 'c'], $this->storage->shift()->payload);
        $this->assertNull($this->storage->shift());
    }

    public function testShiftDoesNotConsumeNextMessage(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));

        $this->storage->shift();

        $this->assertSame(['v' => 2], $this->storage->shift()->payload);
    }

    public function testHasNextReturnsFalseOnEmptyStorage(): void
    {
        $this->storage->init();

        $this->assertFalse($this->storage->hasNext());
    }

    public function testHasNextReturnsTrueWhenMessageExists(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));

        $this->assertTrue($this->storage->hasNext());
    }

    public function testHasNextDoesNotConsumeMessage(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));

        $this->storage->hasNext();

        $this->assertSame(['v' => 1], $this->storage->shift()->payload);
    }

    public function testCountRemainingIsZeroOnEmptyStorage(): void
    {
        $this->storage->init();

        $this->assertSame(0, $this->storage->countRemaining());
    }

    public function testCountRemainingReflectsAppendedMessages(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));
        $this->storage->append(new QueueMessage(['v' => 3]));

        $this->assertSame(3, $this->storage->countRemaining());
    }

    public function testCountRemainingDecreasesAfterShift(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));

        $this->storage->shift();

        $this->assertSame(1, $this->storage->countRemaining());
    }

    public function testCountRemainingDoesNotConsumeMessages(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));

        $this->storage->countRemaining();

        $this->assertSame(['v' => 1], $this->storage->shift()->payload);
    }

    public function testCompactReturnsFalseWhenUnreadRecordsRemain(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));
        $this->storage->shift();

        $this->assertFalse($this->storage->compact());
    }

    public function testCompactReturnsTrueWhenFullyDrained(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->shift();

        $this->assertTrue($this->storage->compact());
    }

    public function testCompactPreservesUnreadMessages(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));
        $this->storage->append(new QueueMessage(['v' => 3]));
        $this->storage->shift();

        $this->storage->compact();

        $this->assertSame(2, $this->storage->countRemaining());
        $this->assertSame(['v' => 2], $this->storage->shift()->payload);
        $this->assertSame(['v' => 3], $this->storage->shift()->payload);
    }

    public function testCompactResetsPointerToZero(): void
    {
        $this->storage->init();
        $this->storage->append(new QueueMessage(['v' => 1]));
        $this->storage->append(new QueueMessage(['v' => 2]));
        $this->storage->shift();

        $this->storage->compact();

        $this->assertSame('0', file_get_contents($this->pointerFile));
    }

    public function testAppendThrowsQueueExceptionForNonSerialisableValue(): void
    {
        $this->storage->init();

        $this->expectException(QueueException::class);

        $this->storage->append(new QueueMessage(['fh' => fopen('php://memory', 'rb')]));
    }
}