const expect = require("expect.js");
const sinon = require("sinon");
const AsyncRedis = require("async-redis-mock");
const Queue = require("..");

describe("redis-queue module", () => {
    it("should export redisQueue factory function", () => {
        expect(Queue).to.be.a("function");
        expect(Queue.length).to.be(1);
    });
});

describe("redisQueue(object)", () => {
    const key = "foo";
    let queue, redis;

    beforeEach(() => {
        redis = AsyncRedis.createClient();
        queue = Queue({key, redis});
    });

    it("should return RedisQueue which implements AsyncQueue", () => {
        expect(queue).to.be.an("object");
        expect(queue.push).to.be.a("function");
        expect(queue.shift).to.be.a("function");

        const pushResult = queue.push({foo:13});
        const shiftResult = queue.shift();

        // handle any errors that might arise; only interested in result type
        pushResult.catch(() => {});
        shiftResult.catch(() => {});

        expect(pushResult.then).to.be.a("function");
        expect(shiftResult.then).to.be.a("function");
    });

    it("should throw on missing key", () => {
        expect(() => Queue()).to.throwError();
    });
});

describe("RedisQueue#push(object)", () => {
    const key = "foo";
    let queue, redis;

    beforeEach(() => {
        redis = AsyncRedis.createClient();
        queue = Queue({key, redis});
        redis.sscan = (key, cursor) => [0, []];
    });

    it("should add value to start of LIST found at the queue key", async () => {
        const value = {foo:13};
        await queue.push(value);
        const popped = await redis.lpop(key);
        expect(popped).to.be(JSON.stringify(value));
    });
});

describe("RedisQueue#shift([function])", () => {
    const key = "foo";
    const dead_key = "graveyard";
    const value = {foo:13};
    let queue, redis;

    beforeEach(() => {
        redis = AsyncRedis.createClient();
        queue = Queue({key, dead_key, redis});
        redis.sscan = (key, cursor) => [0, []];
    });

    describe("w/o handler function", () => {
        it("should pop value from end of LIST", async () => {
            await queue.push(value);    // FIFO value at end(R) of LIST
            await queue.push("other");  // another value in front(L) of it
            const beforeLength = await redis.llen(key);

            const popped = await queue.shift();
            const afterLength = await redis.llen(key);
            const other = await redis.rpop(key);

            expect(beforeLength).to.be(2);
            expect(afterLength).to.be(1);
            expect(JSON.stringify(popped)).to.be(JSON.stringify(value));
            expect(JSON.parse(other)).to.be("other");
        });
    });

    describe("w/ handler function", () => {
        it("should pop from end of LIST and pass value to handler", async () => {
            const handler = sinon.spy();
            await queue.push(value);    // FIFO value at end(R) of LIST
            await queue.push("other");  // another value in front(L) of it
            const beforeLength = await redis.llen(key);
            await queue.shift(handler);
            const afterLength = await redis.llen(key);
            const other = await redis.rpop(key);

            expect(beforeLength).to.be(2);
            expect(afterLength).to.be(1);
            expect(handler.calledOnce).to.be(true);
            expect(JSON.stringify(handler.args[0][0])).to.be(JSON.stringify(value));
            expect(JSON.parse(other)).to.be("other");
        });

        it("should raise handler exceptions to caller", async () => {
            let threw;

            try {
                await queue.push(value);
                await queue.shift(snafu);
            } catch (err) { threw = err; }

            expect(threw).to.be.an(Error);
        });

        it("should write failures to dead_key", async () => {
            try {
                await queue.push(value);
                await queue.shift(snafu);
            } catch (err) {}

            const dead_count = await redis.llen(dead_key);
            const grave = await redis.lpop(dead_key);
            const info = JSON.parse(grave);

            expect(info).to.be.an("object");
            expect(info.err).to.be.an("object");
            expect(info.err.message).to.be.a("string");
            expect(info.err.message.includes("snafu")).to.be(true);
            expect(info.value).to.be(JSON.stringify(value));
        });

        it("should recover data on next run after failure", async () => {
            const {stringify} = JSON;
            const value = {foo: "bar"};
            let handler = sinon.spy();

            // initialize queue with single value
            await queue.push(value);

            // start worker A, but hang to let worker B recover
            const workerA = Queue({key, redis});
            const hungResult = workerA.shift(hang);

            // ensure hungResult is handled
            hungResult.catch(() => {});

            // resume after tick to flush out any pending callbacks
            await new Promise(done => setTimeout(done, 0));

            // queue should now be empty because data is stored in transaction
            const emptyLength = await redis.llen(key);

            // have to stub some stuff Redis mock doesn't handle
            const tx = "uniquekey";
            await redis.lpush(tx, stringify(value));
            redis.sscan = (key, cursor) => [0, [tx]];

            // start worker B; should get value originally delivered to A
            const workerB = Queue({key, redis});
            await workerB.shift(handler);

            expect(emptyLength).to.be(0);
            expect(handler.calledOnce).to.be(true);
            expect(stringify(handler.args[0][0])).to.be(stringify(value));
        });
    });
});

async function hang(timeout=2500) {
    await new Promise(release => setTimeout(release, timeout));
}

async function snafu(value) {
    throw Object.assign(new Error("snafu"), {value});
}
