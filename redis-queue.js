const Redis = require("async-redis");
const {randomBytes} = require("crypto");

const ONE_HOUR = 60*60;
const ONE_MINUTE = 60;

/**
 * Create asynchronous queue backed by Redis LISTs.
 * @param {object} options
 * @param {string} options.key
 * @param {string} [options.dead_key]
 * @param {string} [options.recover_key]
 * @param {RedisClient|string|URL} [options.redis]
 * @param {function} [options.retry]
 * @param {number} [options.timeout]
 * @param {number} [options.recover_timeout]
 * @returns {RedisQueue}
 */
function redisQueue({
    key,
    delim=":",
    dead_key=undefined,
    recover_key=`${key}${delim}tx`,
    redis=Redis.createClient(),
    retry=fn=>fn(),
    timeout=ONE_HOUR,
    recover_timeout=ONE_MINUTE,
}) {
    if (!key) throw new Error("Redis queue requires key");
    if (redis instanceof URL) redis = String(redis);
    if (typeof redis === "string") redis = Redis.createClient(redis);

    let recovered = false;

    return {push, shift};

    /**
     * Add value to end of queue.
     * @name {RedisQueue#push}
     * @param {*} value
     */
    async function push(value) {
        value = JSON.stringify(value);
        await recover();
        await redis.lpush(key, value);
    }

    /**
     * Remove value from beginning of queue.
     * @name {RedisQueue#shift}
     * @param {function} [handleValue]
     * @returns {object}
     */
    async function shift(handleValue) {
        await recover();

        // can use quick atomic pop if there's no handler to run
        if (!handleValue) {
            const value = await redis.rpop(key);
            return JSON.parse(value);
        }

        // need to execute in transaction; start with a transaction key
        const transaction_key = `${recover_key}${delim}${idgen()}`;
        const lock_key = `${transaction_key}${delim}lock`;

        // record transaction for recovery
        await redis.sadd(recover_key, transaction_key);
        await redis.set(lock_key, "locked");
        await redis.expire(lock_key, timeout);

        // move next value into transaction (created as 1-elem LIST)
        // TODO: RPOPLPUSH can be replaced with LMOVE after Redis 6.2
        await redis.rpoplpush(key, transaction_key);

        // read the only value in the transaction
        const value = await redis.lindex(transaction_key, 0);

        try {
            // parse value first so retry doesn't get called for bad JSON
            const parsed = JSON.parse(value);

            // execute handler if transaction contains value
            if (typeof value === "string") {
                // call handler; retry with fresh copy of value each call
                await retry(() => handleValue(JSON.parse(value)));
            }

            // clean up transaction
            await redis.del(lock_key);
            await redis.del(transaction_key);
            await redis.srem(recover_key, transaction_key);

            // in addition to calling handler, return parsed result
            return parsed;
        } catch (err) {
            // permanent failure; send to dead drop if configured
            if (dead_key) {
                const {message} = err;
                const failure = JSON.stringify({err: {message}, value});
                await redis.lpush(dead_key, failure);
            }

            // re-throw so caller can take further action
            throw err;
        }
    }

    /**
     * Re-queue values which previously failed to process.
     */
    async function recover() {
        if (recovered && recovered + recover_timeout*1000 >= Date.now()) {
            return;
        }

        for await (const transaction_key of sscan(recover_key)) {
            // lock key indicates if transaction is still active
            const lock_key = `${transaction_key}${delim}lock`;
            const locked = await redis.get(lock_key);

            // if unlocked, cleanup transaction and move data back into queue
            if (!locked) {
                await redis.rpoplpush(transaction_key, key);
                await redis.srem(recover_key, transaction_key);
            }
        }

        recovered = Date.now();
    }

    /**
     * Scan Redis SET.
     * @param {string} key
     */
    async function* sscan(key) {
        let cursor = 0, elements;

        do {
            [cursor, elements] = await redis.sscan(key, cursor);
            yield* elements;
        } while (cursor > 0);
    }
}

module.exports = redisQueue;

/**
 * Generate random 64-bit hex-encoded value.
 * @returns {string}
 */
function idgen() {
    return randomBytes(8).toString("hex");
}
