Redis implementation of the AsyncQueue interface, which in turn is just an async
**push** and **shift**.  Provides reliable delivery with automatic recovery in
case of failure.

Basic Usage
===========
```js
const key = "awesome-queue";
const RedisQueue = require("@zingle/redis-queue");
const queue = RedisQueue({key});

(async () => {
    let value;

    await queue.push("foo");
    await queue.push("bar");

    do {
        value = await queue.shift();
        console.log(value);
    } while (value !== undefined);
})().catch(console.error);
```

Reliable Delivery
=================
Putting data into the queue is an atomic operation and is thus intrinsically
reliable.  Pulling data out of the queue can be an atomic operation, but to
ensure reliable processing, a callback function can be used which must complete
successfully before the data is removed permanently from the queue.

When a callback is passed to the **shift** method, a transaction is written
which can be used to recover the data in case of failure during processing.  The
queue treats un-exceptional results as delivery and exceptional results as
temporary failures.

After making several failed attempts to deliver data, the failure becomes
permanent.  Data which cannot be delivered will be written to a Redis LIST
specified by the `dead_key` option, if configured.

When a queue starts up it will execute recovery before data deliver starts.  Any
queued data which was left in an unfinished transaction will be atomically put
back into the queue for processing.

Quick Overview
--------------
 * **push** is atomic and reliable
 * **shift** without handler is atomic and reliable
 * **shift** with handler ends up in one of three states
   * handled successfully without exception
   * in unfinished transaction after system failure
   * in dead drop after permanent failure
 * recovery will re-queue any unfinished transactions

Duplicate Delivery
------------------
It's possible for duplicate delivery to occur when there are multiple processors
operating on the same queue.  This can happen if processing takes a long time in
one worker.  Another worker will assume the previous worker has permanently hung
and will never complete, and so it will "recover" that data by putting it back
into the queue.


TODO:
 * notes on timeouts
 * implement/test timeouts
