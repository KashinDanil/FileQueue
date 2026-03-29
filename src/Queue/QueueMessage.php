<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Queue;

use DanilKashin\FileQueue\Exceptions\CorruptedQueueException;
use JsonException;
use JsonSerializable;

readonly class QueueMessage implements JsonSerializable
{
    public function __construct(
        public array $payload,
    ) {
    }

    public function jsonSerialize(): array
    {
        return $this->payload;
    }

    public static function jsonDeserialize(string $json): static
    {
        try {
            $payload = json_decode($json, associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new CorruptedQueueException("Invalid JSON payload: {$e->getMessage()}", previous: $e);
        }

        return new static(payload: $payload);
    }
}