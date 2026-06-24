import { describe, expect, it } from "vitest";

import { TraceContextError } from "../src/errors";
import {
  describeJobRejection,
  isJobRejection,
  JobCtx,
  type JobRejection,
  Payload,
  Priority,
  RetryDirective,
  TraceContext,
} from "../src/types";

const VALID_TP = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

describe("Payload", () => {
  it("round-trips text and json", () => {
    expect(Payload.text("hi").text()).toBe("hi");
    expect(Payload.json({ a: 1 }).json()).toEqual({ a: 1 });
    expect(Payload.json({ a: 1 }).encoding).toBe("application/json");
  });

  it("preserves explicit encoding hint", () => {
    const p = Payload.text("hi", "text/plain");
    expect(p.encoding).toBe("text/plain");
  });

  it("bytes factory preserves raw bytes and encoding", () => {
    const data = new Uint8Array([1, 2, 3]);
    const p = Payload.bytes(data, "raw");
    expect(p.data).toEqual(data);
    expect(p.encoding).toBe("raw");
  });
});

describe("Priority", () => {
  it("exposes constants", () => {
    expect(Priority.P7).toBe(7);
    expect(Priority.MIN).toBe(0);
    expect(Priority.MAX).toBe(9);
  });

  it("exposes all named levels", () => {
    expect(Priority.P0).toBe(0);
    expect(Priority.P1).toBe(1);
    expect(Priority.P2).toBe(2);
    expect(Priority.P3).toBe(3);
    expect(Priority.P4).toBe(4);
    expect(Priority.P5).toBe(5);
    expect(Priority.P6).toBe(6);
    expect(Priority.P8).toBe(8);
    expect(Priority.P9).toBe(9);
  });
});

describe("TraceContext", () => {
  it("validates the traceparent", () => {
    expect(() => TraceContext.create("garbage")).toThrow(TraceContextError);
    const tc = TraceContext.create(VALID_TP).withTracestate("v=1");
    expect(tc.traceparent).toBe(VALID_TP);
    expect(tc.tracestate).toBe("v=1");
  });

  it("withTracestate creates a new instance", () => {
    const tc = TraceContext.create(VALID_TP);
    const withState = tc.withTracestate("v=1");
    expect(tc.tracestate).toBeUndefined();
    expect(withState.tracestate).toBe("v=1");
    expect(withState.traceparent).toBe(VALID_TP);
  });

  it("creation without tracestate leaves it undefined", () => {
    const tc = TraceContext.create(VALID_TP);
    expect(tc.tracestate).toBeUndefined();
  });

  it("creation with tracestate sets it", () => {
    const tc = TraceContext.create(VALID_TP, "v=1");
    expect(tc.tracestate).toBe("v=1");
  });
});

describe("RetryDirective", () => {
  it("carries kind and delay", () => {
    expect(RetryDirective.default().kind).toBe("default");
    expect(RetryDirective.deadLetter().kind).toBe("dead_letter");
    const a = RetryDirective.after(5000);
    expect(a.kind).toBe("after");
    expect(a.delayMs).toBe(5000);
  });

  it("after clamps negative delay to 0", () => {
    const a = RetryDirective.after(-100);
    expect(a.delayMs).toBe(0);
  });
});

describe("job rejection helpers", () => {
  it("narrows and describes", () => {
    const rej: JobRejection = { kind: "payload_too_large", limit: 10, actual: 20 };
    expect(isJobRejection(rej)).toBe(true);
    expect(isJobRejection({ jobId: "x", deduplicated: false })).toBe(false);
    expect(describeJobRejection(rej)).toContain("exceeds the queue limit of 10");
  });

  it("describes each rejection variant", () => {
    expect(describeJobRejection({ kind: "unknown_queue", queue: "q" })).toContain("not declared");
    expect(describeJobRejection({ kind: "queue_full", queue: "q", limit: 100 })).toContain(
      "capacity",
    );
    expect(describeJobRejection({ kind: "queue_closing", queue: "q" })).toContain("being deleted");
    expect(describeJobRejection({ kind: "invalid_request", message: "bad" })).toContain(
      "structural validation",
    );
    expect(describeJobRejection({ kind: "unknown" })).toContain("unrecognized");
  });
});

describe("DeadLetterRecord", () => {
  it("toEnqueueRequest preserves payload, priority, custom, and trace context", async () => {
    const { DeadLetterRecord } = await import("../src/types");
    const tc = TraceContext.create(VALID_TP);
    const rec = new DeadLetterRecord({
      queue: "emails",
      jobId: "abc",
      jobType: "send_email",
      payload: Payload.json({ x: 1 }),
      priority: 5,
      custom: { k: 1 },
      traceContext: tc,
      enqueuedAt: new Date(1_700_000_000_000),
      cause: "attempts_exhausted",
      failedAt: new Date(1_700_000_100_000),
      finalAttempt: 3,
      lastReason: "boom",
    });

    const req = rec.toEnqueueRequest();
    expect(req.queue).toBe("emails");
    expect(req.jobType).toBe("send_email");
    expect(req.priority).toBe(5);
    expect(req.payload?.json()).toEqual({ x: 1 });
    expect(req.custom).toEqual({ k: 1 });
    expect(req.traceContext?.traceparent).toBe(VALID_TP);
  });
});

describe("JobCtx", () => {
  it("carries job identity fields", () => {
    const ctx = new JobCtx({
      id: "abc",
      queue: "emails",
      jobType: "send_email",
      priority: 3,
      attempt: 2,
      maxAttempts: 5,
      enqueuedAt: new Date(1_700_000_000_000),
      custom: { k: "v" },
      leaseExpiresAt: new Date(1_700_000_060_000),
      extend: async () => new Date(),
    });
    expect(ctx.id).toBe("abc");
    expect(ctx.queue).toBe("emails");
    expect(ctx.jobType).toBe("send_email");
    expect(ctx.priority).toBe(3);
    expect(ctx.attempt).toBe(2);
    expect(ctx.maxAttempts).toBe(5);
    expect(ctx.custom.k).toBe("v");
  });

  it("updates leaseExpiresAt on extend", async () => {
    const newExpiry = new Date(1_700_000_120_000);
    const ctx = new JobCtx({
      id: "abc",
      queue: "q",
      jobType: "t",
      priority: 3,
      attempt: 1,
      maxAttempts: 5,
      enqueuedAt: new Date(1_700_000_000_000),
      custom: {},
      leaseExpiresAt: new Date(1_700_000_060_000),
      extend: async () => newExpiry,
    });
    const result = await ctx.extend(30_000);
    expect(result).toEqual(newExpiry);
    expect(ctx.leaseExpiresAt).toEqual(newExpiry);
    expect(ctx._knownExpiryMs).toBe(newExpiry.getTime());
  });

  it("signal is undefined for non-worker jobs", () => {
    const ctx = new JobCtx({
      id: "abc",
      queue: "q",
      jobType: "t",
      priority: 3,
      attempt: 1,
      maxAttempts: 5,
      enqueuedAt: new Date(1_700_000_000_000),
      custom: {},
      leaseExpiresAt: new Date(1_700_000_060_000),
      extend: async () => new Date(),
    });
    expect(ctx.signal).toBeUndefined();
  });

  it("signal fires after markLeaseLost", () => {
    const ctx = new JobCtx({
      id: "abc",
      queue: "q",
      jobType: "t",
      priority: 3,
      attempt: 1,
      maxAttempts: 5,
      enqueuedAt: new Date(1_700_000_000_000),
      custom: {},
      leaseExpiresAt: new Date(1_700_000_060_000),
      extend: async () => new Date(),
    });
    ctx._bindAbort(new AbortController());
    let aborted = false;
    ctx.signal?.addEventListener("abort", () => {
      aborted = true;
    });
    ctx._markLeaseLost();
    expect(ctx._leaseLost).toBe(true);
    expect(aborted).toBe(true);
  });
});
