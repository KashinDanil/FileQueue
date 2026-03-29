<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Queue;

interface QueueInterface
{
    public function enqueue(QueueMessage $message): void;

    public function dequeue(): ?QueueMessage;

    public function isEmpty(): bool;

    public function size(): int;

    public function compact(): void;
}