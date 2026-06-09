import { describe, expect, it } from "vitest";

import { TraceContextError } from "../src/errors";
import {
  describeJobRejection,
  isJobRejection,
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
});

describe("Priority", () => {
  it("exposes constants", () => {
    expect(Priority.P7).toBe(7);
    expect(Priority.MIN).toBe(0);
    expect(Priority.MAX).toBe(9);
  });
});

describe("TraceContext", () => {
  it("validates the traceparent", () => {
    expect(() => TraceContext.create("garbage")).toThrow(TraceContextError);
    const tc = TraceContext.create(VALID_TP).withTracestate("v=1");
    expect(tc.traceparent).toBe(VALID_TP);
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
});

describe("job rejection helpers", () => {
  it("narrows and describes", () => {
    const rej: JobRejection = { kind: "payload_too_large", limit: 10, actual: 20 };
    expect(isJobRejection(rej)).toBe(true);
    expect(isJobRejection({ jobId: "x", deduplicated: false })).toBe(false);
    expect(describeJobRejection(rej)).toContain("exceeds the queue limit of 10");
  });
});
