<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Workers;

use DanilKashin\FileQueue\Queue\QueueInterface;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\Worker\Worker;
use Generator;

abstract class QueueWorker extends Worker
{
    private const int DEFAULT_MESSAGES_PER_QUEUE = 10;

    private ?Generator $work = null;

    /**
     * @return QueueInterface[]
     */
    abstract protected function getQueues(): array;

    abstract protected function processMessage(QueueMessage $message): bool;

    protected function getMessagesPerQueueLimit(): int
    {
        return self::DEFAULT_MESSAGES_PER_QUEUE;
    }

    protected function tick(): void
    {
        if (!$this->resume()) {
            $this->start();
        }
    }

    private function resume(): bool
    {
        if ($this->work === null) {
            return false;
        }

        $this->work->next();

        if ($this->work->valid()) {
            return true;
        }

        $this->work = null;

        return false;
    }

    private function start(): void
    {
        $this->work = $this->iterate();

        if (!$this->work->valid()) {
            $this->work = null;
        }
    }

    private function iterate(): Generator
    {
        foreach ($this->getQueues() as $queue) {
            $processed = 0;
            $perQueueLimit = $this->getMessagesPerQueueLimit();

            while ($processed < $perQueueLimit && !$queue->isEmpty()) {
                $message = $queue->dequeue();

                if ($message === null) {
                    break;
                }

                if ($this->processMessage($message)) {
                    $processed++;
                    echo '+';
                } else {
                    echo '-';
                }

                yield;
            }
        }
    }
}
