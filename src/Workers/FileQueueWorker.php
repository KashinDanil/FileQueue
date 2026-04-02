<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Workers;

use DanilKashin\FileQueue\Queue\FileQueue;
use DanilKashin\FileQueue\Queue\QueueInterface;

abstract class FileQueueWorker extends QueueWorker
{
    public function __construct(
        private readonly string $queuesDir,
        ?int $maxTicks = null,
    ) {
        parent::__construct($maxTicks);
    }

    /**
     * @return QueueInterface[]
     */
    protected function getQueues(): array
    {
        $files = glob($this->queuesDir . '/*.queue.data') ?: [];
        $queues = [];

        foreach ($files as $file) {
            $queues[] = new FileQueue(basename($file, '.queue.data'), $this->queuesDir);
        }

        return $queues;
    }
}