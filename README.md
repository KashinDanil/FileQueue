# FileQueue

A simple, file-based FIFO queue for PHP. Messages are persisted to disk using an append-only log with exclusive file locking, so the queue survives process restarts and is safe to use across multiple processes.

## Requirements

- PHP 8.1+
- Extension: `json`

## Installation

```bash
composer require danil-kashin/file-queue
```

## Usage

### 1. Create a worker

Extend `FileQueueWorker` and implement `processMessage()`. Return `true` on success, `false` on failure. Failed messages are dequeued and not retried automatically.

The worker scans a directory for queue files and processes each discovered queue on every tick, up to **10 messages per queue**. This ensures equal resource distribution across queues and avoids the noisy-neighbor problem. It handles `SIGTERM`/`SIGINT` for graceful shutdown.

```php
use DanilKashin\FileQueue\Queue\QueueMessage;
use DanilKashin\FileQueue\Workers\FileQueueWorker;

class OrderWorker extends FileQueueWorker
{
    protected function processMessage(QueueMessage $message): bool
    {
        return handleOrder($message->payload);
    }
}
```

For each processed message the worker prints `+` (success) or `-` (failure) to stdout.

### 2. Run the worker

The worker is a long-running process — run it in the background so your application keeps responding. Progress is written to stdout (`+` / `-` per message and `.` per tick); errors go to stderr.

```bash
vendor/bin/run_worker "App\Workers\OrderWorker" --queuesDir=/var/queues > worker.log 2>&1 &
```

### 3. Enqueue messages

Messages can be enqueued from anywhere in your application as long as they share the same `baseDir`. No running worker is required at enqueue time; messages will be picked up on the next tick.

```php
use DanilKashin\FileQueue\Queue\FileQueue;
use DanilKashin\FileQueue\Queue\QueueMessage;

$queue = new FileQueue(queueName: 'orders', baseDir: '/var/queues');
$queue->enqueue(new QueueMessage(['order_id' => 42, 'status' => 'pending']));
```

## Architecture

```mermaid
flowchart LR
    App["Your App"] -->|"enqueue(QueueMessage)"| FileQueue
    FileQueue -->|"dequeue()"| FileQueueWorker
    FileQueueWorker -->|"processMessage(QueueMessage)"| YourWorker["Your Worker"]
```

If you need to supply queues from a custom source (e.g. a database-backed list of queue names), extend `QueueWorker` directly and implement both `getQueues()` and `processMessage()`.

## Queue API

```php
$queue = new FileQueue(queueName: 'orders', baseDir: '/var/queues');

// Write
$queue->enqueue(new QueueMessage(['order_id' => 42]));

// Read
$message = $queue->dequeue(); // QueueMessage|null
if ($message !== null) {
    $payload = $message->payload; // ['order_id' => 42]
}

// Inspect
$queue->isEmpty(); // bool
$queue->size();    // int — counts remaining (unconsumed) messages

// Cleanup
$queue->compact();
```

### Compaction

Over time the data file grows as messages are appended and consumed. `compact()` rewrites the file to contain only unread messages, reclaiming disk space. 

No need to call it regularly if your `FileQueueWorker` runs continuously and the queues are emptied regularly. When the last message is dequeued, all associated files are removed immediately, so no manual cleanup is needed.

## How it works

Each queue is backed by three files:

| File | Purpose |
|------|---------|
| `{name}.queue.data` | Append-only binary log of framed records |
| `{name}.queue.pointer` | Read offset — tracks the next unread message |
| `{name}.queue.lock` | Exclusive lock file — guards all mutations |

Records are size-prefixed with a 4-byte big-endian length header followed by the JSON payload. Reads advance the pointer without touching the data file. `compact()` rewrites the data file to drop consumed records. All mutations are guarded by an exclusive lock.

## Exceptions

| Class | When thrown |
|-------|------------|
| `QueueException` | I/O failure (unreadable file, write error, …) |
| `CorruptedQueueException` | Truncated or structurally invalid record |

`CorruptedQueueException` extends `QueueException`, so catching `QueueException` covers both.
