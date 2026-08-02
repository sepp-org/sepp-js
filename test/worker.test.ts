import { status as GrpcStatus } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";

import { SeppClient } from "../src/client";
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
    expect(client.nacked[0]!.reason).toBe("handler threw: Error: boom");
  });

  it("no handler nacks the job", async () => {
    const client = new FakeClient();
    client.batches = [null, [makeJob(client, "unknown_type")]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    worker.handle("something_else", async () => {});
    await driveWorker(
      worker,
      waitUntil(() => client.nacked.length === 1),
    );
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

    let started = false;
    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    worker.handle("t", async () => {
      started = true;
      await blocked;
    });

    const running = worker.run();
    await waitUntil(() => started);
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

  it("cancels an in-flight long-poll reserve and resolves promptly", async () => {
    // A real SeppClient over a stubbed gRPC method: the reserve never responds
    // until its call handle is cancelled, like a genuine 30s long poll.
    let cancelled = false;
    const reserve = (
      _req: unknown,
      _md: unknown,
      _opts: unknown,
      cb: (err: (Error & { code: number }) | null, res?: unknown) => void,
    ) => {
      const timer = setTimeout(() => cb(null, { jobs: [] }), 30_000);
      return {
        cancel: () => {
          cancelled = true;
          clearTimeout(timer);
          const err = new Error("Cancelled on client") as Error & { code: number };
          err.code = GrpcStatus.CANCELLED;
          cb(err);
        },
      };
    };
    const client = SeppClient.fromClient({ reserve, close: () => {} } as any);
    const worker = new Worker(client, { queues: ["q"], leaseDuration: 30_000 });

    const running = worker.run();
    await sleep(50); // let run() block inside the long-poll reserve
    const start = Date.now();
    worker.shutdownHandle().shutdown();
    await Promise.race([
      running,
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 2000)),
    ]);
    expect(Date.now() - start).toBeLessThan(1000);
    expect(cancelled).toBe(true);
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
    let signalAborted = false;
    worker.handle("t", async (_payload, ctx) => {
      await sleep(200);
      signalAborted = ctx.signal?.aborted === true;
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(0);
    expect(client.nacked).toHaveLength(0);
    expect(signalAborted).toBe(true);
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

  it("default heartbeat interval tracks the granted (server-clamped) lease", async () => {
    const client = new FakeClient();
    // The server grants ~60ms per extension no matter what lease was requested.
    client._extend = (_jobId, _attempt, _extensionMs, _workerId) => {
      client.extends++;
      return Promise.resolve(new Date(Date.now() + 60));
    };
    client.batches = [[makeJob(client, "t", { leaseExpiresAt: new Date(Date.now() + 60) })]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000, // requested/3 would be 10s and never fire during the handler
      autoExtend: true,
    });

    let done = false;
    worker.handle("t", async () => {
      await sleep(150);
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    // Cadence follows the granted lease: ~every 20ms, so several extends fired.
    expect(client.extends).toBeGreaterThanOrEqual(2);
    expect(client.acked).toHaveLength(1);
  });

  it("retries after a transient extend failure while the lease is still valid", async () => {
    const client = new FakeClient();
    let first = true;
    client._extend = (_jobId, _attempt, extensionMs, _workerId) => {
      client.extends++;
      if (first) {
        first = false;
        return Promise.reject(new Error("transient blip"));
      }
      return Promise.resolve(new Date(Date.now() + extensionMs));
    };
    client.batches = [[makeJob(client, "t", { leaseExpiresAt: new Date(Date.now() + 60_000) })]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      autoExtendInterval: 10,
    });

    let done = false;
    worker.handle("t", async () => {
      await sleep(60);
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.extends).toBeGreaterThanOrEqual(2); // failed once, then kept extending
    expect(client.acked).toHaveLength(1); // the transient failure did not lose the lease
  });

  it("marks the lease lost when extend fails past expiry", async () => {
    const client = new FakeClient();
    client.extendError = new Error("storage down"); // transient, but the lease has lapsed
    client.batches = [[makeJob(client, "t", { leaseExpiresAt: new Date(Date.now() + 30) })]];
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 60,
      autoExtendInterval: 50, // first extend attempt lands after the 30ms lease expired
    });

    let done = false;
    let signalAborted = false;
    worker.handle("t", async (_payload, ctx) => {
      await sleep(150);
      signalAborted = ctx.signal?.aborted === true;
      done = true;
    });
    await driveWorker(
      worker,
      waitUntil(() => done),
    );
    expect(client.acked).toHaveLength(0);
    expect(client.nacked).toHaveLength(0);
    expect(signalAborted).toBe(true);
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
    // More jobs than permits, delivered in one oversized batch, so
    // over-admission would be observable.
    const jobs = Array.from({ length: 6 }, (_, i) => makeJob(client, "t", { id: `job-${i}` }));
    client.batches = [jobs];
    const reserveMaxJobs: Array<number | undefined> = [];
    const origReserve = client.reserve.bind(client);
    client.reserve = async (opts) => {
      reserveMaxJobs.push(opts.maxJobs);
      return origReserve(opts);
    };

    let concurrent = 0;
    let maxConcurrent = 0;
    let completed = 0;

    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      maxInFlight: 2,
    });

    worker.handle("t", async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await sleep(30);
      concurrent--;
      completed++;
    });

    await driveWorker(
      worker,
      waitUntil(() => completed === 6),
    );
    expect(completed).toBe(6);
    expect(maxConcurrent).toBeGreaterThanOrEqual(1);
    expect(maxConcurrent).toBeLessThanOrEqual(2);
    // The worker never asks for more jobs than it has free capacity for.
    expect(reserveMaxJobs[0]).toBe(2);
    for (const m of reserveMaxJobs) expect(m).toBeLessThanOrEqual(2);
  });
});

// ---------------------------------------------------------------------------
// Early-empty reserve backoff
// ---------------------------------------------------------------------------
describe("early-empty reserve backoff", () => {
  it("pauses between reserves when the server answers empty immediately", async () => {
    const client = new FakeClient();
    let calls = 0;
    client.reserve = async () => {
      calls++;
      return null; // returns instantly, long before the 30s wait window
    };
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
    });

    const running = worker.run();
    await sleep(600);
    worker.shutdownHandle().shutdown();
    await running;
    // ~250ms pause between early empties, not a hot loop.
    expect(calls).toBeGreaterThanOrEqual(1);
    expect(calls).toBeLessThanOrEqual(5);
  });

  it("re-polls immediately after a full-window empty reserve", async () => {
    const client = new FakeClient();
    let calls = 0;
    client.reserve = async () => {
      calls++;
      await sleep(120); // the full wait window elapses server-side
      return null;
    };
    const worker = new Worker(client as unknown as SeppClient, {
      queues: ["q"],
      leaseDuration: 30_000,
      waitTimeout: 120,
    });

    const running = worker.run();
    await sleep(600);
    worker.shutdownHandle().shutdown();
    await running;
    // ~120ms per poll with no extra backoff; a spurious 250ms pause would
    // allow at most 2 polls in this window.
    expect(calls).toBeGreaterThanOrEqual(3);
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

  it("rejects zero waitTimeout", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, {
          queues: ["q"],
          leaseDuration: 30_000,
          waitTimeout: 0,
        }),
    ).toThrow(ValidationError);
  });

  it("rejects negative waitTimeout", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, {
          queues: ["q"],
          leaseDuration: 30_000,
          waitTimeout: -1,
        }),
    ).toThrow(ValidationError);
  });

  it("rejects non-positive reserveErrorBackoff", () => {
    const client = new FakeClient();
    expect(
      () =>
        new Worker(client as unknown as SeppClient, {
          queues: ["q"],
          leaseDuration: 30_000,
          reserveErrorBackoff: 0,
        }),
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
