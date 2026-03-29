<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Workers;

use DanilKashin\FileQueue\Queue\QueueInterface;
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\Worker\Worker;

abstract class QueueWorker extends Worker
{
    private const int MESSAGES_PER_QUEUE = 10;

    /**
     * @return QueueInterface[]
     */
    abstract protected function getQueues(): array;

    abstract protected function processMessage(QueueMessage $message): bool;

    protected function tick(): void
    {
        foreach ($this->getQueues() as $queue) {
            $processed = 0;

            while ($processed < self::MESSAGES_PER_QUEUE && !$queue->isEmpty()) {
                $message = $queue->dequeue();

                if (null === $message) {
                    break;
                }

                if ($this->processMessage($message)) {
                    $processed++;
                    echo '+';
                } else {
                    echo '-';
                }

                if ($this->shouldStop()) {
                    break 2;
                }
            }

            if ($this->shouldStop()) {
                break;
            }
        }
    }
}