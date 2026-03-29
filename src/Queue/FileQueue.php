<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Queue;

use DanilKashin\FileLock\FileLock;

/**
 * File-based FIFO queue using an append-only storage strategy.
 *
 * Orchestrates FileLock and QueueStorage.
 */
class FileQueue implements QueueInterface
{
    private FileLock $lock;

    private QueueStorage $storage;

    public function __construct(
        string $queueName,
        string $baseDir,
    ) {
        $queueFileName = $baseDir . '/' . $queueName . '.queue';
        $this->lock = new FileLock(
            lockFile: $queueFileName . '.lock'
        );
        $this->storage = new QueueStorage(
            dataFile: $queueFileName . '.data',
            pointerFile: $queueFileName . '.pointer',
        );
    }

    public function enqueue(QueueMessage $message): void
    {
        $this->lock->acquire();
        try {
            $this->storage->init();
            $this->storage->append($message);
        } finally {
            $this->lock->release();
        }
    }

    public function dequeue(): ?QueueMessage
    {
        if (!$this->storage->exists()) {
            return null;
        }

        $this->lock->acquire();
        try {
            $record = $this->storage->shift();
            if (null === $record) {
                $this->deleteAllFiles();
            }

            return $record;
        } finally {
            $this->lock->release();
        }
    }

    public function isEmpty(): bool
    {
        if (!$this->storage->exists()) {
            return true;
        }

        $this->lock->acquire();
        try {
            return !$this->storage->hasNext();
        } finally {
            $this->lock->release();
        }
    }

    public function size(): int
    {
        if (!$this->storage->exists()) {
            return 0;
        }

        $this->lock->acquire();
        try {
            return $this->storage->countRemaining();
        } finally {
            $this->lock->release();
        }
    }

    public function compact(): void
    {
        if (!$this->storage->exists()) {
            return;
        }

        $this->lock->acquire();
        try {
            if ($this->storage->compact()) {
                $this->deleteAllFiles();
            }
        } finally {
            $this->lock->release();
        }
    }

    private function deleteAllFiles(): void
    {
        $this->storage->deleteFiles();
        $this->lock->deleteFile();
    }
}
