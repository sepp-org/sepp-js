/**
 * End-to-end tests against a real sepp server started via testcontainers.
 * When Docker is unavailable the tests skip gracefully.
 */
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Payload, RetryDirective, SeppClient, TraceContext, Worker } from "../src/index";
import { HandlerError } from "../src/worker";

const QUEUE = "sepp-js-it";
const VALID_TP = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

let container: StartedTestContainer | undefined;
let endpoint: string | undefined;
let client: SeppClient;

beforeAll(async () => {
  try {
    container = await new GenericContainer("ghcr.io/sepp-org/sepp:master")
      .withExposedPorts(50051)
      .withEnvironment({ SEPP_ALLOW_QUEUE_CREATION: "true" })
      .withWaitStrategy(Wait.forLogMessage("queue server listening"))
      .withStartupTimeout(60000)
      .start();
  } catch (err) {
    console.warn(`integration: could not start sepp container (${err}); skipping tests`);
    return;
  }

  const port = container.getMappedPort(50051);
  endpoint = `localhost:${port}`;

  client = await SeppClient.connect(endpoint);
}, 120_000);

afterAll(async () => {
  client?.close();
  await container?.stop();
});

async function reserve(extra: Record<string, unknown> = {}) {
  return client.reserve({
    queues: [QUEUE],
    leaseDuration: 30_000,
    waitTimeout: 5_000,
    maxJobs: 100,
    ...extra,
  });
}

describe("integration", () => {
  it("reports server info", async () => {
    if (!endpoint) return;
    const info = await client.getServerInfo();
    expect(info.version.length).toBeGreaterThan(0);
    expect(info.maxEnqueueBatch).toBeGreaterThan(0);
  });

  it("enqueues, reserves, and acks a job", async () => {
    if (!endpoint) return;
    const jobType = `it-${Date.now()}`;
    const ack = await client.enqueue({
      queue: QUEUE,
      jobType,
      payload: Payload.json({ hello: "world" }),
      custom: { n: 7 },
    });
    expect(ack.jobId).toBeTruthy();

    const jobs = await reserve();
    expect(jobs).not.toBeNull();
    const job = jobs!.find((j) => j.ctx.jobType === jobType);
    expect(job).toBeDefined();
    expect(job!.payload?.json()).toEqual({ hello: "world" });
    expect(job!.ctx.custom.n).toBe(7);
    await client.ack(job!.ctx);
  });

  it("enqueue with idempotency deduplicates", async () => {
    if (!endpoint) return;
    const jobType = `it-idem-${Date.now()}`;
    const idemKey = `key-${Date.now()}`;

    const ack1 = await client.enqueue({
      queue: QUEUE,
      jobType,
      idempotencyKey: idemKey,
    });
    expect(ack1.jobId).toBeTruthy();
    expect(ack1.deduplicated).toBe(false);

    const ack2 = await client.enqueue({
      queue: QUEUE,
      jobType,
      idempotencyKey: idemKey,
    });
    expect(ack2.jobId).toBe(ack1.jobId);
    expect(ack2.deduplicated).toBe(true);
  });

  it("nack retry and reserve again with incremented attempt", async () => {
    if (!endpoint) return;
    const jobType = `it-nack-retry-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("retry") });

    const jobs1 = await reserve();
    expect(jobs1).not.toBeNull();
    const job1 = jobs1!.find((j) => j.ctx.jobType === jobType);
    expect(job1).toBeDefined();
    expect(job1!.ctx.attempt).toBe(1);
    await client.nack(job1!.ctx);

    const jobs2 = await reserve();
    expect(jobs2).not.toBeNull();
    const job2 = jobs2!.find((j) => j.ctx.jobType === jobType);
    expect(job2).toBeDefined();
    expect(job2!.ctx.attempt).toBe(2);
    await client.ack(job2!.ctx);
  });

  it("nack dead letter removes the job", async () => {
    if (!endpoint) return;
    const jobType = `it-dl-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("dl") });

    const jobs = await reserve();
    expect(jobs).not.toBeNull();
    const job = jobs!.find((j) => j.ctx.jobType === jobType);
    expect(job).toBeDefined();
    await client.nack(job!.ctx, { retry: RetryDirective.deadLetter() });

    const empty = await reserve({ waitTimeout: 2_000, leaseDuration: 1_000 });
    const found = empty?.find((j) => j.ctx.jobType === jobType);
    expect(found).toBeUndefined();
  });

  it("extend lease returns a new expiry", async () => {
    if (!endpoint) return;
    const jobType = `it-extend-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("extend") });

    const jobs = await reserve({ leaseDuration: 2_000 });
    expect(jobs).not.toBeNull();
    const job = jobs!.find((j) => j.ctx.jobType === jobType);
    expect(job).toBeDefined();

    const oldExpiry = job!.ctx.leaseExpiresAt.getTime();
    const newExpiry = await client.extend(job!.ctx, 10_000);
    expect(newExpiry.getTime()).toBeGreaterThan(oldExpiry);
    await client.ack(job!.ctx);
  });

  it("trace context propagates through enqueue and reserve", async () => {
    if (!endpoint) return;
    const jobType = `it-trace-${Date.now()}`;
    const tc = TraceContext.create(VALID_TP, "v=1");

    await client.enqueue({
      queue: QUEUE,
      jobType,
      payload: Payload.text("traced"),
      traceContext: tc,
    });

    const jobs = await reserve();
    expect(jobs).not.toBeNull();
    const job = jobs!.find((j) => j.ctx.jobType === jobType);
    expect(job).toBeDefined();
    expect(job!.ctx.traceContext?.traceparent).toBe(VALID_TP);
    expect(job!.ctx.traceContext?.tracestate).toBe("v=1");
    await client.ack(job!.ctx);
  });

  it("worker processes job through handler", async () => {
    if (!endpoint) return;
    const jobType = `it-worker-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("ping") });

    let completedJobId: string | undefined;
    let resolveDone: (() => void) | undefined;
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve;
    });

    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 2_000,
    }).handle(jobType, async (payload, ctx) => {
      expect(payload?.text()).toBe("ping");
      completedJobId = ctx.id;
      resolveDone?.();
    });

    const handle = worker.shutdownHandle();
    const running = worker.run();

    await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 15_000)),
    ]);
    expect(completedJobId).toBeTruthy();

    await new Promise((r) => setTimeout(r, 300));
    handle.shutdown();
    await running;
  }, 20_000);

  it("worker handler error nacks the job", async () => {
    if (!endpoint) return;
    const jobType = `it-worker-err-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("fail") });

    let handled = false;
    const done = new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (handled) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 10_000);
    });

    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 2_000,
    }).handle(jobType, async () => {
      handled = true;
      throw HandlerError.retry("test failure");
    });

    const handle = worker.shutdownHandle();
    const running = worker.run();

    await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 15_000)),
    ]);
    expect(handled).toBe(true);

    await new Promise((r) => setTimeout(r, 500));
    handle.shutdown();
    await running;
  }, 20_000);

  it("worker catch-all handles unregistered job type", async () => {
    if (!endpoint) return;
    const jobType = `it-catchall-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("catchall") });

    let catchCalled = false;
    const done = new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (catchCalled) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 10_000);
    });

    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 2_000,
    }).catchAll(async (_payload, ctx) => {
      expect(ctx.jobType).toBe(jobType);
      catchCalled = true;
    });

    const handle = worker.shutdownHandle();
    const running = worker.run();

    await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 15_000)),
    ]);
    expect(catchCalled).toBe(true);

    await new Promise((r) => setTimeout(r, 300));
    handle.shutdown();
    await running;
  }, 20_000);

  it("worker duplicate handler throws", async () => {
    if (!endpoint) return;
    const worker = new Worker(client, { queues: [QUEUE], leaseDuration: 30_000 });
    worker.handle("t", async () => {});
    expect(() => worker.handle("t", async () => {})).toThrow();
  });

  it("worker auto-extend keeps lease alive for long handler", async () => {
    if (!endpoint) return;
    const jobType = `it-autoextend-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("long") });

    let completed = false;
    const done = new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (completed) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 15_000);
    });

    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 2_000,
      waitTimeout: 2_000,
      autoExtend: true,
      autoExtendInterval: 500,
    }).handle(jobType, async () => {
      await new Promise((r) => setTimeout(r, 4_000));
      completed = true;
    });

    const handle = worker.shutdownHandle();
    const running = worker.run();

    await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 20_000)),
    ]);
    expect(completed).toBe(true);

    handle.shutdown();
    await running;
  }, 25_000);

  it("worker processing with maxInFlight bounds concurrency", async () => {
    if (!endpoint) return;
    const jobType = `it-concurrent-${Date.now()}`;
    for (let i = 0; i < 5; i++) {
      await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text(`job${i}`) });
    }

    let completed = 0;
    const done = new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (completed >= 5) {
          clearInterval(check);
          resolve();
        }
      }, 100);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 20_000);
    });

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 2_000,
      maxInFlight: 3,
    }).handle(jobType, async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await new Promise((r) => setTimeout(r, 500));
      currentConcurrent--;
      completed++;
    });

    const handle = worker.shutdownHandle();
    const running = worker.run();

    await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 25_000)),
    ]);
    expect(completed).toBe(5);
    expect(maxConcurrent).toBeLessThanOrEqual(3);

    handle.shutdown();
    await running;
  }, 30_000);

  it("reserve returns null on empty queue", async () => {
    if (!endpoint) return;
    const empty = await client.reserve({
      queues: [`empty-${Date.now()}`],
      leaseDuration: 1_000,
      waitTimeout: 2_000,
    });
    expect(empty).toBeNull();
  });
});
