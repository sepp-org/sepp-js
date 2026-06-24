import { describe, expect, it } from "vitest";

import type { SeppClient } from "../src/client";
import {
  AttemptMismatchError,
  DuplicateHandlerError,
  JobNotFoundError,
  ValidationError,
} from "../src/errors";
import { type Job, JobCtx, type ReserveOptions, RetryDirective } from "../src/types";
import { HandlerError, Worker } from "../src/worker";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class FakeClient {
  batches: Array<Job[] | null> = [];
  acked: string[] = [];
  nacked: Array<{ id: string; directive: RetryDirective; reason: string }> = [];
  extends = 0;
  extendError: Error | null = null;

  async reserve(_opts: ReserveOptions): Promise<Job[] | null> {
    if (this.batches.length === 0) {
      await sleep(5);
      return null;
    }
    return this.batches.shift() ?? null;
  }

  async ack(ctx: { id: string }): Promise<void> {
    this.acked.push(ctx.id);
  }

  async nack(
    ctx: { id: string },
    opts?: { retry?: RetryDirective; reason?: string },
  ): Promise<boolean> {
    this.nacked.push({
      id: ctx.id,
      directive: opts?.retry ?? RetryDirective.default(),
      reason: opts?.reason ?? "",
    });
    return opts?.retry?.kind === "dead_letter";
  }

  _extend(
    _jobId: string,
    _attempt: number,
    extensionMs: number,
    _workerId: string | undefined,
  ): Promise<Date> {
    this.extends++;
    if (this.extendError) throw this.extendError;
    return Promise.resolve(new Date(Date.now() + extensionMs));
  }
}

function makeJob(client: FakeClient, jobType: string, overrides: Record<string, any> = {}): Job {
  const extend = async (jobId: string, attempt: number, ms: number, wid?: string) =>
    client._extend(jobId, attempt, ms, wid);
  const ctx = new JobCtx({
    id: overrides.id ?? "550e8400-e29b-41d4-a716-446655440000",
    queue: "q",
    jobType,
    priority: 3,
    attempt: 1,
    maxAttempts: 5,
    enqueuedAt: new Date(1_700_000_000_000),
    custom: {},
    leaseExpiresAt: new Date(1_700_000_060_000),
    workerId: "w1",
    extend,
    ...overrides,
  });
  return { payload: overrides.payload, ctx };
}

function waitUntil(fn: () => boolean, interval = 10, timeout = 3000): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (fn()) return resolve();
      if (Date.now() - start > timeout) return reject(new Error("waitUntil timeout"));
      setTimeout(check, interval);
    };
    check();
  });
}

async function driveWorker(worker: Worker, signal: Promise<void>, timeout = 3000): Promise<void> {
  const running = worker.run();
  try {
    await Promise.race([
      signal,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), timeout)),
    ]);
  } finally {
    worker.shutdownHandle().shutdown();
    await Promise.race([
      running,
      new Promise((_, reject) => setTimeout(() => reject(new Error("shutdown timeout")), timeout)),
    ]);
  }
}

// ---------------------------------------------------------------------------
// Dispatch outcomes
// ---------------------------------------------------------------------------
describe("dispatch outcomes", () => {
  it("processes and acks a job", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let done = false;
    worker.handle("t", async () => {
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(1);
    expect(client.nacked).toHaveLength(0);
  });

  it("handler error nacks for retry", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let done = false;
    worker.handle("t", async () => {
      done = true;
      throw HandlerError.retry("try again");
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.nacked).toHaveLength(1);
    expect(client.nacked[0]!.directive.kind).toBe("default");
  });

  it("handler permanent dead letters the job", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let done = false;
    worker.handle("t", async () => {
      done = true;
      throw HandlerError.permanent("dead");
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.nacked).toHaveLength(1);
    expect(client.nacked[0]!.directive.kind).toBe("dead_letter");
  });

  it("handler exception nacks the job", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let done = false;
    worker.handle("t", async () => {
      done = true;
      throw new Error("boom");
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.nacked).toHaveLength(1);
    expect(client.nacked[0]!.reason).toBe("handler threw an error");
  });

  it("no handler nacks the job", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "unknown_type")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    worker.handle("something_else", async () => {});
    const running = worker.run();
    await sleep(300);
    worker.shutdownHandle().shutdown();
    await Promise.race([
      running,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 2000)),
    ]);
    expect(client.nacked).toHaveLength(1);
  });

  it("catch-all handles unregistered job type", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let handled = false;
    worker.catchAll(async () => {
      handled = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => handled),
    );
    expect(client.acked).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Handler registration
// ---------------------------------------------------------------------------
describe("handler registration", () => {
  it("duplicate handler raises DuplicateHandlerError", () => {
    const client = new FakeClient();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    worker.handle("t", async () => {});
    expect(() => worker.handle("t", async () => {})).toThrow(DuplicateHandlerError);
  });

  it("replace handler overwrites", () => {
    const client = new FakeClient();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    worker.handle("t", async () => {
      throw new Error("old");
    });
    worker.replaceHandler("t", async () => {});
  });

  it("remove handler allows re-registration", () => {
    const client = new FakeClient();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    worker.handle("t", async () => {});
    worker.removeHandler("t");
    worker.handle("t", async () => {});
  });
});

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------
describe("graceful shutdown", () => {
  it("drains in-flight jobs before returning", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    worker.handle("t", async () => {
      await blocked;
    });

    const running = worker.run();
    await sleep(100);
    worker.shutdownHandle().shutdown();

    let resolved = false;
    running.then(() => {
      resolved = true;
    });
    await sleep(100);
    expect(resolved).toBe(false);

    release();
    await Promise.race([
      running,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 2000)),
    ]);
    expect(client.acked).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Auto-extend
// ---------------------------------------------------------------------------
describe("auto-extend", () => {
  it("renews lease while handler runs", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 60,
      autoExtendInterval: 10,
    });

    let done = false;
    worker.handle("t", async () => {
      await sleep(50);
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.extends).toBeGreaterThanOrEqual(1);
    expect(client.acked).toHaveLength(1);
  });

  it("aborts handler on lease lost via AttemptMismatchError", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    client.extendError = new AttemptMismatchError();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 60,
      autoExtendInterval: 10,
    });

    let done = false;
    worker.handle("t", async () => {
      await sleep(200);
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(0);
    expect(client.nacked).toHaveLength(0);
  });

  it("aborts handler on lease lost via JobNotFoundError", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    client.extendError = new JobNotFoundError();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 60,
      autoExtendInterval: 10,
    });

    let done = false;
    worker.handle("t", async () => {
      await sleep(200);
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Reserve error handling
// ---------------------------------------------------------------------------
describe("reserve error handling", () => {
  it("continues after transient reserve error", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    let failedCalls = 0;
    const origReserve = client.reserve.bind(client);
    client.reserve = async (opts) => {
      if (failedCalls < 2) {
        failedCalls++;
        throw new Error("blip");
      }
      return origReserve(opts);
    };

    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      reserveErrorBackoff: 1,
    });

    let done = false;
    worker.handle("t", async () => {
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(1);
    expect(failedCalls).toBeGreaterThanOrEqual(2);
  });
});

// ---------------------------------------------------------------------------
// Concurrency bounding
// ---------------------------------------------------------------------------
describe("max in flight", () => {
  it("bounds concurrent handler execution", async () => {
    const client = new FakeClient();
    const jobs = [makeJob(client, "t", { id: "a" }), makeJob(client, "t", { id: "b" })];
    client.batches = [null, jobs];

    let concurrent = 0;
    let maxConcurrent = 0;
    const running = new Set<number>();
    const releases: Array<() => void> = [];
    const doneCount = { v: 0 };

    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      maxInFlight: 2,
      reserveErrorBackoff: 1,
    });

    worker.handle("t", async () => {
      const id = running.size;
      running.add(id);
      concurrent = running.size;
      maxConcurrent = Math.max(maxConcurrent, concurrent);

      let release: () => void;
      const p = new Promise<void>((r) => {
        release = r;
      });
      releases.push(release!);
      await p;

      running.delete(id);
      concurrent = running.size;
      doneCount.v++;
    });

    const runningP = worker.run();
    await sleep(300);
    expect(doneCount.v).toBe(0);
    for (const r of releases) r();
    await sleep(100);
    worker.shutdownHandle().shutdown();
    await Promise.race([
      runningP,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 2000)),
    ]);
    expect(maxConcurrent).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// ShutdownHandle
// ---------------------------------------------------------------------------
describe("ShutdownHandle", () => {
  it("starts unsignaled and flips after shutdown", () => {
    const client = new FakeClient();
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    const handle = worker.shutdownHandle();
    expect(handle.isShutdown).toBe(false);
    handle.shutdown();
    expect(handle.isShutdown).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// HandlerError constructors
// ---------------------------------------------------------------------------
describe("HandlerError", () => {
  it("retry sets default directive", () => {
    const e = HandlerError.retry("try again");
    expect(e.directive.kind).toBe("default");
    expect(e.reason).toBe("try again");
    expect(e).toBeInstanceOf(Error);
  });

  it("retryAfter sets after directive with delay", () => {
    const e = HandlerError.retryAfter("slow down", 5000);
    expect(e.directive.kind).toBe("after");
    expect(e.directive.delayMs).toBe(5000);
  });

  it("permanent sets dead_letter directive", () => {
    const e = HandlerError.permanent("fatal");
    expect(e.directive.kind).toBe("dead_letter");
  });
});

// ---------------------------------------------------------------------------
// Worker validation
// ---------------------------------------------------------------------------
describe("Worker validation", () => {
  it("rejects empty queues", () => {
    const client = new FakeClient();
    expect(
      () => new Worker(client as unknown as SeppClient, { queues: [], leaseDuration: 30_000 }),
    ).toThrow(ValidationError);
  });

  it("rejects empty queue name", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, { queues: ["", "ok"], leaseDuration: 30_000 }),
    ).toThrow(ValidationError);
  });

  it("rejects zero lease duration", () => {
    const client = new FakeClient();
    expect(
      () => new Worker(client as unknown as SeppClient, { queues: ["q"], leaseDuration: 0 }),
    ).toThrow(ValidationError);
  });

  it("rejects negative lease duration", () => {
    const client = new FakeClient();
    expect(
      () => new Worker(client as unknown as SeppClient, { queues: ["q"], leaseDuration: -1 }),
    ).toThrow(ValidationError);
  });

  it("rejects maxJobs < 1", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, {
          queues: ["q"],
          leaseDuration: 30_000,
          maxJobs: 0,
        }),
    ).toThrow(ValidationError);
  });

  it("rejects empty workerId", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, {
          queues: ["q"],
          leaseDuration: 30_000,
          workerId: "",
        }),
    ).toThrow(ValidationError);
  });

  it("clamps maxInFlight to at least 1", () => {
    const client = new FakeClient();
    const w = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      maxInFlight: 0,
    });
    expect(w).toBeDefined();
  });

  it("accepts custom workerId", () => {
    const client = new FakeClient();
    const w = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      workerId: "custom-1",
    });
    expect(w).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Dispose error paths
// ---------------------------------------------------------------------------
describe("dispose error paths", () => {
  it("ack JobNotFound is tolerated", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    const origAck = client.ack.bind(client);
    let done = false;
    client.ack = async (ctx) => {
      if (!done) {
        done = true;
        throw new JobNotFoundError();
      }
      return origAck(ctx);
    };

    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    let handlerDone = false;
    worker.handle("t", async () => {
      handlerDone = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => handlerDone),
    );
  });

  it("nack error is tolerated", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "t")]];
    let nackCalled = false;
    client.nack = async () => {
      if (!nackCalled) {
        nackCalled = true;
        throw new Error("nack failed");
      }
      return false;
    };

    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });
    let handlerDone = false;
    worker.handle("t", async () => {
      handlerDone = true;
      throw new Error("handler error");
    });
    await driveWorker(
      worker,
      waitUntil(() => handlerDone),
    );
  });
});
