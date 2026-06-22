import { describe, expect, it } from "vitest";

import {
  ackFromPb,
  deadLetterRecordFromPb,
  durationToMs,
  enqueueRequestToPb,
  jobFromPb,
  jobRejectionFromPb,
  msToDuration,
  primitiveFromPb,
  primitiveToPb,
  reserveOptionsToPb,
  retryDirectiveToPb,
  serverInfoFromPb,
  validatePriority,
} from "../src/convert";
import {
  JobConversionError,
  PriorityRangeError,
  ServerInfoError,
  ValidationError,
} from "../src/errors";
import {
  DeadLetterCause,
  type GetServerInfoResponse,
  type Job as PbJob,
} from "../src/gen/sepp/v1/queue";
import { Payload, RetryDirective, TraceContext } from "../src/types";

const VALID_TP = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

const noopExtend = async () => new Date();

function validPbJob(): PbJob {
  return {
    id: "550e8400-e29b-41d4-a716-446655440000",
    jobType: "send_email",
    priority: 3,
    attempt: 1,
    maxAttempts: 5,
    enqueuedAt: new Date(1_700_000_000_000),
    leaseExpiresAt: new Date(1_700_000_060_000),
    custom: {},
    queue: "emails",
  };
}

describe("durations", () => {
  it("round-trips milliseconds", () => {
    expect(msToDuration(5000)).toEqual({ seconds: 5n, nanos: 0 });
    expect(msToDuration(1234)).toEqual({ seconds: 1n, nanos: 234_000_000 });
    expect(durationToMs({ seconds: 2n, nanos: 500_000_000 })).toBe(2500);
    expect(durationToMs(undefined)).toBe(0);
  });
});

describe("priority", () => {
  it("accepts 0-9 and rejects out of range", () => {
    expect(validatePriority(0)).toBe(0);
    expect(validatePriority(9)).toBe(9);
    expect(() => validatePriority(10)).toThrow(PriorityRangeError);
    expect(() => validatePriority(-1)).toThrow(PriorityRangeError);
    expect(() => validatePriority(1.5)).toThrow(PriorityRangeError);
  });
});

describe("primitives", () => {
  it("encodes by JS type", () => {
    expect(primitiveToPb("x")).toEqual({ stringValue: "x" });
    expect(primitiveToPb(true)).toEqual({ boolValue: true });
    expect(primitiveToPb(7)).toEqual({ intValue: 7n });
    expect(primitiveToPb(1.5)).toEqual({ doubleValue: 1.5 });
    expect(primitiveToPb(42n)).toEqual({ intValue: 42n });
  });

  it("decodes oneof", () => {
    expect(primitiveFromPb({ stringValue: "x" }, "k")).toBe("x");
    expect(primitiveFromPb({ boolValue: false }, "k")).toBe(false);
    expect(primitiveFromPb({ doubleValue: 2.5 }, "k")).toBe(2.5);
    expect(primitiveFromPb({ intValue: 7n }, "k")).toBe(7);
    expect(primitiveFromPb({ intValue: 2n ** 60n }, "k")).toBe(2n ** 60n);
  });

  it("throws on an empty custom value", () => {
    expect(() => primitiveFromPb({}, "k")).toThrow(JobConversionError);
  });
});

describe("enqueueRequestToPb", () => {
  it("builds a minimal request", () => {
    const pb = enqueueRequestToPb({ queue: "q", jobType: "t" });
    expect(pb.queue).toBe("q");
    expect(pb.jobType).toBe("t");
    expect(pb.payload).toBeUndefined();
    expect(pb.custom).toEqual({});
    expect(pb.scheduledAt).toBeUndefined();
  });

  it("maps all fields", () => {
    const pb = enqueueRequestToPb({
      queue: "q",
      jobType: "t",
      payload: Payload.bytes(new Uint8Array([1]), "raw"),
      idempotencyKey: "idem",
      priority: 7,
      maxAttempts: 5,
      custom: { k: 1 },
      traceContext: TraceContext.create(VALID_TP),
      scheduledAt: new Date(1234),
    });
    expect(pb.priority).toBe(7);
    expect(pb.idempotencyKey).toBe("idem");
    expect(pb.payload?.encoding).toBe("raw");
    expect(pb.custom.k).toEqual({ intValue: 1n });
    expect(pb.traceContext?.traceparent).toBe(VALID_TP);
    expect(pb.scheduledAt).toEqual(new Date(1234));
  });

  it("drops a pre-epoch scheduledAt", () => {
    const pb = enqueueRequestToPb({ queue: "q", jobType: "t", scheduledAt: new Date(-1000) });
    expect(pb.scheduledAt).toBeUndefined();
  });

  it("rejects empty queue / job type", () => {
    expect(() => enqueueRequestToPb({ queue: "", jobType: "t" })).toThrow(ValidationError);
    expect(() => enqueueRequestToPb({ queue: "q", jobType: "" })).toThrow(ValidationError);
  });
});

describe("ackFromPb", () => {
  it("maps job id and dedup flag", () => {
    expect(ackFromPb({ jobId: "abc", deduplicated: true })).toEqual({
      jobId: "abc",
      deduplicated: true,
    });
  });
});

describe("jobRejectionFromPb", () => {
  it("maps each variant", () => {
    expect(jobRejectionFromPb({ unknownQueue: { queue: "q" } })).toEqual({
      kind: "unknown_queue",
      queue: "q",
    });
    expect(jobRejectionFromPb({ payloadTooLarge: { limit: 10n, actual: 20n } })).toEqual({
      kind: "payload_too_large",
      limit: 10,
      actual: 20,
    });
    expect(jobRejectionFromPb({ invalidRequest: { message: "oops" } })).toEqual({
      kind: "invalid_request",
      message: "oops",
    });
    expect(jobRejectionFromPb({ queueFull: { queue: "emails", limit: 1000n } })).toEqual({
      kind: "queue_full",
      queue: "emails",
      limit: 1000,
    });
    expect(jobRejectionFromPb({ queueClosing: { queue: "emails" } })).toEqual({
      kind: "queue_closing",
      queue: "emails",
    });
    expect(jobRejectionFromPb({})).toEqual({ kind: "unknown" });
  });
});

describe("reserveOptionsToPb", () => {
  it("maps fields and applies the default wait timeout", () => {
    const pb = reserveOptionsToPb({ queues: ["q1", "q2"], leaseDuration: 5000, maxJobs: 7 });
    expect(pb.queues).toEqual(["q1", "q2"]);
    expect(pb.leaseDuration).toEqual({ seconds: 5n, nanos: 0 });
    expect(pb.waitTimeout).toEqual({ seconds: 30n, nanos: 0 });
    expect(pb.maxJobs).toBe(7);
  });

  it("validates queues and lease", () => {
    expect(() => reserveOptionsToPb({ queues: [], leaseDuration: 1000 })).toThrow(ValidationError);
    expect(() => reserveOptionsToPb({ queues: ["ok", ""], leaseDuration: 1000 })).toThrow(
      ValidationError,
    );
    expect(() => reserveOptionsToPb({ queues: ["q"], leaseDuration: 0 })).toThrow(ValidationError);
  });
});

describe("jobFromPb", () => {
  it("maps the happy path", () => {
    const pb = validPbJob();
    pb.payload = { data: Buffer.from([1, 2]), encoding: "json" };
    pb.custom = { k: { stringValue: "v" } };
    const job = jobFromPb(noopExtend, pb, undefined);
    expect(job.ctx.id).toBe("550e8400-e29b-41d4-a716-446655440000");
    expect(job.ctx.priority).toBe(3);
    expect(job.ctx.queue).toBe("emails");
    expect(job.payload?.encoding).toBe("json");
    expect(Array.from(job.payload?.data ?? [])).toEqual([1, 2]);
    expect(job.ctx.custom.k).toBe("v");
    expect(job.ctx.enqueuedAt).toEqual(new Date(1_700_000_000_000));
  });

  it("rejects missing id / job type", () => {
    expect(() => jobFromPb(noopExtend, { ...validPbJob(), id: "" }, undefined)).toThrow(
      JobConversionError,
    );
    expect(() => jobFromPb(noopExtend, { ...validPbJob(), jobType: "" }, undefined)).toThrow(
      JobConversionError,
    );
  });

  it("rejects out-of-range priority", () => {
    expect(() => jobFromPb(noopExtend, { ...validPbJob(), priority: 10 }, undefined)).toThrow(
      JobConversionError,
    );
  });

  it("rejects a pre-epoch timestamp", () => {
    expect(() =>
      jobFromPb(noopExtend, { ...validPbJob(), enqueuedAt: new Date(-1000) }, undefined),
    ).toThrow(JobConversionError);
  });

  it("drops an invalid trace context but keeps the job", () => {
    const job = jobFromPb(
      noopExtend,
      { ...validPbJob(), traceContext: { traceparent: "garbage" } },
      undefined,
    );
    expect(job.ctx.traceContext).toBeUndefined();
  });

  it("preserves a valid trace context", () => {
    const job = jobFromPb(
      noopExtend,
      { ...validPbJob(), traceContext: { traceparent: VALID_TP, tracestate: "v=1" } },
      undefined,
    );
    expect(job.ctx.traceContext?.traceparent).toBe(VALID_TP);
    expect(job.ctx.traceContext?.tracestate).toBe("v=1");
  });
});

describe("serverInfoFromPb", () => {
  function validInfo(): GetServerInfoResponse {
    return {
      serverVersion: "1.2.3",
      supportedProtocolVersions: ["v1"],
      serverTime: new Date(1_700_000_000_000),
      restrictsEncodings: false,
      allowedEncodings: ["json"],
      maxPayloadBytes: 1024n,
      maxCustomEntries: 10,
      maxCustomTotalBytes: 2048n,
      maxCustomKeyBytes: 64,
      maxQueueNameBytes: 512,
      maxJobTypeBytes: 256,
      maxIdempotencyKeyBytes: 128,
      maxScheduleHorizon: { seconds: 86_400n, nanos: 0 },
      maxEnqueueBatch: 100,
      maxReserveBatch: 50,
      maxReserveQueues: 8,
      maxWaitTimeout: { seconds: 30n, nanos: 0 },
      maxLeaseDuration: { seconds: 60n, nanos: 0 },
      strictQueues: true,
      deadLetterRetentionEnabled: false,
    };
  }

  it("maps the happy path", () => {
    const info = serverInfoFromPb(validInfo());
    expect(info.version).toBe("1.2.3");
    expect(info.maxPayloadSize).toBe(1024);
    expect(info.maxScheduleHorizon).toBe(86_400_000);
    expect(info.maxWaitTimeout).toBe(30_000);
    expect(info.strictQueues).toBe(true);
  });

  it("rejects a missing version", () => {
    expect(() => serverInfoFromPb({ ...validInfo(), serverVersion: "" })).toThrow(ServerInfoError);
  });

  it("rejects an invalid server time", () => {
    expect(() => serverInfoFromPb({ ...validInfo(), serverTime: new Date(-1) })).toThrow(
      ServerInfoError,
    );
  });
});

describe("deadLetterRecordFromPb", () => {
  it("maps a record and replays into the original queue", () => {
    const r = deadLetterRecordFromPb({
      job: { ...validPbJob(), payload: { data: Buffer.from([1, 2, 3]), encoding: "json" } },
      cause: DeadLetterCause.DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED,
      failedAt: new Date(1_700_000_100_000),
      finalAttempt: 5,
      lastReason: "boom",
    });
    expect(r.queue).toBe("emails");
    expect(r.cause).toBe("attempts_exhausted");
    expect(r.finalAttempt).toBe(5);
    expect(r.lastReason).toBe("boom");

    const replay = r.toEnqueueRequest();
    expect(replay.queue).toBe("emails");
    expect(replay.jobType).toBe("send_email");
    expect(replay.priority).toBe(3);
    expect(Array.from(replay.payload?.data ?? [])).toEqual([1, 2, 3]);
  });

  it("maps the admin cause", () => {
    const r = deadLetterRecordFromPb({
      job: validPbJob(),
      cause: DeadLetterCause.DEAD_LETTER_CAUSE_ADMIN,
      failedAt: new Date(1_700_000_100_000),
      finalAttempt: 1,
    });
    expect(r.cause).toBe("admin");
  });

  it("maps an unknown cause to unspecified", () => {
    const r = deadLetterRecordFromPb({
      job: validPbJob(),
      cause: 99 as DeadLetterCause,
      failedAt: new Date(1_700_000_100_000),
      finalAttempt: 1,
    });
    expect(r.cause).toBe("unspecified");
  });

  it("rejects a missing job", () => {
    expect(() =>
      deadLetterRecordFromPb({
        cause: DeadLetterCause.DEAD_LETTER_CAUSE_REJECTED,
        failedAt: new Date(1),
        finalAttempt: 1,
      }),
    ).toThrow(JobConversionError);
  });
});

describe("retryDirectiveToPb", () => {
  it("maps each strategy", () => {
    expect(retryDirectiveToPb(RetryDirective.default())).toEqual({ default: {} });
    expect(retryDirectiveToPb(RetryDirective.deadLetter())).toEqual({ deadLetter: {} });
    expect(retryDirectiveToPb(RetryDirective.after(5000))).toEqual({
      delay: { seconds: 5n, nanos: 0 },
    });
  });
});
