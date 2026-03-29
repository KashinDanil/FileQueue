<?php

declare(strict_types=1);

namespace DanilKashin\FileQueue\Queue;

use DanilKashin\FileQueue\Exceptions\CorruptedQueueException;
use DanilKashin\FileQueue\Exceptions\QueueException;
use JsonException;

/**
 * Low-level file I/O for a single queue.
 *
 * File layout:
 *   {key}.queue.data – binary log of framed records
 *   {key}.queue.pointer – current read offset (plain integer string)
 *
 * Record format:
 *   [4 bytes unsigned big-endian length][N bytes raw payload]
 *
 * All public methods assume the caller holds an exclusive lock.
 */
final class QueueStorage
{
    private const int HEADER_BYTES = 4;

    private string $dataFile;

    private string $pointerFile;

    private ?bool $exists = null;

    public function __construct(string $dataFile, string $pointerFile)
    {
        $this->dataFile = $dataFile;
        $this->pointerFile = $pointerFile;
    }

    public function exists(): bool
    {
        return $this->exists ??= file_exists($this->dataFile) && file_exists($this->pointerFile);
    }

    public function init(): void
    {
        if ($this->exists()) {
            return;
        }

        $this->initFile($this->dataFile);
        $this->initFile($this->pointerFile, '0');
        $this->exists = true;
    }

    public function append(QueueMessage $message): void
    {
        $fh = $this->openFile($this->dataFile, 'ab');
        try {
            $this->appendMessage($fh, $message);
        } finally {
            fclose($fh);
        }
    }

    public function shift(): ?QueueMessage
    {
        $offset = $this->readPointer();
        $fh = $this->openFile($this->dataFile, 'rb');
        try {
            $record = $this->readRecordAt($fh, $offset);
        } finally {
            fclose($fh);
        }

        $this->writePointer($offset);

        return $record;
    }

    public function hasNext(): bool
    {
        $offset = $this->readPointer();
        $fh = $this->openFile($this->dataFile, 'rb');
        try {
            return $this->skipRecord($fh, $offset);
        } finally {
            fclose($fh);
        }
    }

    public function countRemaining(): int
    {
        $count = 0;
        $offset = $this->readPointer();
        $fh = $this->openFile($this->dataFile, 'rb');
        try {
            while ($this->skipRecord($fh, $offset)) {
                ++$count;
            }
        } finally {
            fclose($fh);
        }

        return $count;
    }

    /**
     * Rewrites the data file to contain only unread records and resets the pointer to 0.
     * Returns true when the queue is fully drained (caller should delete all files).
     */
    public function compact(): bool
    {
        $offset = $this->readPointer();
        $dataFh = $this->openFile($this->dataFile, 'rb');

        try {
            if (0 !== fseek($dataFh, $offset)) {
                throw new QueueException("Cannot seek to offset $offset in data file.");
            }

            $tmpData = $this->dataFile . '.tmp';
            $tmpFh = $this->openFile($tmpData, 'wb');

            try {
                $copied = stream_copy_to_stream($dataFh, $tmpFh);
            } finally {
                fclose($tmpFh);
            }

            if (0 === $copied) {
                @unlink($tmpData);

                return true;
            }

            if (!rename($tmpData, $this->dataFile)) {
                throw new QueueException('Cannot replace data file during compaction.');
            }
        } finally {
            fclose($dataFh);
        }

        $this->writePointer(0);

        return false;
    }

    public function deleteFiles(): void
    {
        @unlink($this->dataFile);
        @unlink($this->pointerFile);
        $this->exists = false;
    }

    private function appendMessage(mixed $fh, QueueMessage $message): void
    {
        try {
            $bytes = json_encode($message, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new QueueException("Cannot encode message payload: {$e->getMessage()}", previous: $e);
        }

        $record = pack('N', strlen($bytes)) . $bytes;
        $written = fwrite($fh, $record);
        if (false === $written || $written !== strlen($record)) {
            throw new QueueException('Failed to write record to queue data file.');
        }
    }

    private function readPointer(): int
    {
        $raw = file_get_contents($this->pointerFile);
        if (false === $raw) {
            throw new QueueException("Cannot read pointer file: $this->pointerFile");
        }

        $offset = (int)trim($raw);
        if ($offset < 0) {
            throw new CorruptedQueueException("Invalid pointer offset ($offset) in $this->pointerFile");
        }

        return $offset;
    }

    private function writePointer(int $offset): void
    {
        $tmp = $this->pointerFile . '.tmp';
        if (false === file_put_contents($tmp, (string)$offset)) {
            throw new QueueException("Cannot write temp pointer file: $tmp");
        }

        if (!rename($tmp, $this->pointerFile)) {
            throw new QueueException("Cannot replace pointer file: $this->pointerFile");
        }
    }

    private function initFile(string $path, string $initialContent = ''): void
    {
        if (file_exists($path)) {
            return;
        }

        $fh = @fopen($path, 'xb'); // 'x' = create-new, fails atomically if already exists
        if (false === $fh) {
            if (!file_exists($path)) {
                throw new QueueException("Cannot create queue file: $path");
            }

            return;
        }

        if ('' !== $initialContent) {
            fwrite($fh, $initialContent);
        }
        fclose($fh);
    }

    /** @return resource */
    private function openFile(string $path, string $mode): mixed
    {
        $fh = @fopen($path, $mode);
        if (false === $fh) {
            throw new QueueException("Cannot open file ($mode): $path");
        }

        return $fh;
    }

    /**
     * Read exactly $bytes bytes from the current position.
     * Returns null on clean EOF; throws on a short read (truncated record).
     *
     * @param resource $fh
     */
    private function readExactly(mixed $fh, int $bytes): ?string
    {
        $buf = fread($fh, $bytes);
        if (false === $buf) {
            throw new QueueException('Unexpected read error on queue data file.');
        }

        if ('' === $buf) {
            return null;
        }

        if (strlen($buf) !== $bytes) {
            throw new CorruptedQueueException(
                "Truncated record: expected $bytes bytes, got " . strlen($buf) . '.'
            );
        }

        return $buf;
    }

    /**
     * Seeks to $offset and reads the header.
     * Returns the payload length, or null on clean EOF.
     */
    private function readRecordHeader(mixed $fh, int $offset): ?int
    {
        if (0 !== fseek($fh, $offset)) {
            throw new QueueException("Cannot seek to offset $offset in data file.");
        }

        $header = $this->readExactly($fh, self::HEADER_BYTES);
        if (null === $header) {
            return null;
        }

        $unpacked = unpack('Nlen', $header);
        if (false === $unpacked || !isset($unpacked['len'])) {
            throw new CorruptedQueueException('Cannot unpack length header.');
        }

        $len = (int)$unpacked['len'];
        if ($len <= 0) {
            throw new CorruptedQueueException("Invalid record length ($len) at offset $offset.");
        }

        return $len;
    }

    /**
     * Advances $offset past the next record without reading the payload.
     * Returns false on clean EOF.
     *
     * @param resource $fh
     */
    private function skipRecord(mixed $fh, int &$offset): bool
    {
        $len = $this->readRecordHeader($fh, $offset);
        if (null === $len) {
            return false;
        }

        $offset += self::HEADER_BYTES + $len;

        return true;
    }

    /**
     * Reads the full record at $offset and advances $offset to the next record.
     * Returns null on clean EOF.
     *
     * @param resource $fh
     */
    private function readRecordAt(mixed $fh, int &$offset): ?QueueMessage
    {
        $len = $this->readRecordHeader($fh, $offset);
        if (null === $len) {
            return null;
        }

        $payload = $this->readExactly($fh, $len);
        if (null === $payload) {
            throw new CorruptedQueueException(
                "Truncated payload at offset $offset: header claims $len bytes but data is absent."
            );
        }

        try {
            $message = QueueMessage::jsonDeserialize($payload);
        } catch (CorruptedQueueException $e) {
            throw new CorruptedQueueException(
                "Invalid JSON payload at offset $offset: {$e->getMessage()}",
                previous: $e,
            );
        }

        $offset += self::HEADER_BYTES + $len;

        return $message;
    }
}