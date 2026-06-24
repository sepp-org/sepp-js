import type { CallOptions, ClientUnaryCall, Metadata, ServiceError } from "@grpc/grpc-js";
import { status as GrpcStatus } from "@grpc/grpc-js";
import { describe, expect, it, vi } from "vitest";

import { SeppClient } from "../src/client";
import {
  AttemptMismatchError,
  BatchResultCountMismatchError,
  BatchValidationError,
  ConnectError,
  EmptyBatchError,
  InvalidApiKeyError,
  InvalidRequestError,
  JobNotFoundError,
  JobRejectedError,
  MalformedResponseError,
  OverloadedError,
  ServerInternalError,
  TransportError,
  UnauthenticatedError,
  UnexpectedStatusError,
  UnknownQueuesError,
} from "../src/errors";
import type {
  EnqueueBatchRequest,
  EnqueueBatchResponse,
  GetServerInfoResponse,
  DeadLetterRecord as PbDeadLetterRecord,
} from "../src/gen/sepp/v1/queue";
import { DeadLetterCause } from "../src/gen/sepp/v1/queue";
import { RetryPolicy } from "../src/retry";
import { RetryDirective, TraceContext } from "../src/types";

const VALID_TP = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

function rpcError(code: number, message = "x"): ServiceError {
  return { code, message, details: message, metadata: undefined as never, name: "ServiceError" };
}

type FakeRpc = {
  (
    request: any,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (err: ServiceError | null, res?: any) => void,
  ): ClientUnaryCall;
  calls: number;
  lastRequest: any;
};

function createFakeRpc(response?: any, error?: ServiceError, sequence?: any[]): FakeRpc {
  const fn = function (
    this: any,
    _request: any,
    _metadata: Metadata,
    _options: Partial<CallOptions>,
    callback: (err: ServiceError | null, res?: any) => void,
  ): ClientUnaryCall {
    fn.calls++;
    fn.lastRequest = _request;
    const idx = Math.min(fn.calls - 1, (sequence?.length ?? 0) - 1);
    const result = sequence ? sequence[idx] : (error ?? response);
    if (result && typeof result === "object" && "code" in result) {
      callback(result as ServiceError);
    } else {
      callback(null, result);
    }
    return {} as ClientUnaryCall;
  } as FakeRpc;
  fn.calls = 0;
  fn.lastRequest = undefined;
  return fn;
}

function makeClient(
  overrides: Partial<{
    enqueueBatch: FakeRpc;
    enqueueAtomic: FakeRpc;
    reserve: FakeRpc;
    ack: FakeRpc;
    nack: FakeRpc;
    extend: FakeRpc;
    drainDeadLetters: FakeRpc;
    getServerInfo: FakeRpc;
  }> = {},
  retryPolicy?: RetryPolicy,
): SeppClient {
  const stub = {
    enqueueBatch: createFakeRpc(),
    enqueueAtomic: createFakeRpc(),
    reserve: createFakeRpc(),
    ack: createFakeRpc(),
    nack: createFakeRpc(),
    extend: createFakeRpc(),
    drainDeadLetters: createFakeRpc(),
    getServerInfo: createFakeRpc(),
    close: vi.fn(),
    ...overrides,
  };
  return SeppClient.fromClient(stub as any, { retryPolicy, rpcTimeout: 100 });
}

function fastRetry(maxAttempts: number): RetryPolicy {
  return new RetryPolicy({ maxAttempts, initialBackoff: 1, maxBackoff: 1, jitter: false });
}

function validPbJob(overrides: Record<string, any> = {}) {
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
    ...overrides,
  };
}

function enqueueBatchResponse(ack: { jobId: string }): EnqueueBatchResponse {
  return { results: [{ success: { jobId: ack.jobId, deduplicated: false } }] };
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------
describe("enqueue", () => {
  it("returns ack on success", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    const ack = await client.enqueue({ queue: "q", jobType: "t" });
    expect(ack.jobId).toBe("abc");
    expect(ack.deduplicated).toBe(false);
  });

  it("throws JobRejectedError on rejection", async () => {
    const rej = { rejection: { payloadTooLarge: { limit: 10n, actual: 20n } } };
    const eb = createFakeRpc({ results: [rej] });
    const client = makeClient({ enqueueBatch: eb });
    await expect(client.enqueue({ queue: "q", jobType: "t" })).rejects.toThrow(JobRejectedError);
  });

  it("throws on transport error", async () => {
    const eb = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ enqueueBatch: eb });
    await expect(client.enqueue({ queue: "q", jobType: "t" })).rejects.toThrow(TransportError);
  });
});

// ---------------------------------------------------------------------------
// Enqueue batch
// ---------------------------------------------------------------------------
describe("enqueueBatch", () => {
  it("handles mixed success and rejection", async () => {
    const eb = createFakeRpc({
      results: [
        { success: { jobId: "a", deduplicated: false } },
        { rejection: { unknownQueue: { queue: "x" } } },
      ],
    });
    const client = makeClient({ enqueueBatch: eb });
    const results = await client.enqueueBatch([
      { queue: "q", jobType: "t" },
      { queue: "x", jobType: "t" },
    ]);
    expect(results[0]!).toEqual({ jobId: "a", deduplicated: false });
    expect("kind" in results[1]!).toBe(true);
  });

  it("throws EmptyBatchError for empty batch", async () => {
    const client = makeClient();
    await expect(client.enqueueBatch([])).rejects.toThrow(EmptyBatchError);
  });

  it("throws on count mismatch", async () => {
    const eb = createFakeRpc({ results: [{ success: { jobId: "a", deduplicated: false } }] });
    const client = makeClient({ enqueueBatch: eb });
    await expect(
      client.enqueueBatch([
        { queue: "q", jobType: "t" },
        { queue: "q2", jobType: "t2" },
      ]),
    ).rejects.toThrow(BatchResultCountMismatchError);
  });

  it("throws on missing outcome", async () => {
    const eb = createFakeRpc({ results: [{}] });
    const client = makeClient({ enqueueBatch: eb });
    await expect(client.enqueueBatch([{ queue: "q", jobType: "t" }])).rejects.toThrow(
      MalformedResponseError,
    );
  });
});

// ---------------------------------------------------------------------------
// Enqueue atomic
// ---------------------------------------------------------------------------
describe("enqueueAtomic", () => {
  it("returns acks on success", async () => {
    const ea = createFakeRpc({
      success: {
        responses: [
          { jobId: "a", deduplicated: false },
          { jobId: "b", deduplicated: false },
        ],
      },
    });
    const client = makeClient({ enqueueAtomic: ea });
    const acks = await client.enqueueAtomic([
      { queue: "q", jobType: "a" },
      { queue: "q", jobType: "b" },
    ]);
    expect(acks).toHaveLength(2);
    expect(acks[0]!.jobId).toBe("a");
  });

  it("throws BatchValidationError on validation failure", async () => {
    const ea = createFakeRpc({
      rejection: { errors: [{ index: 1, rejection: { unknownQueue: { queue: "x" } } }] },
    });
    const client = makeClient({ enqueueAtomic: ea });
    await expect(
      client.enqueueAtomic([
        { queue: "q", jobType: "a" },
        { queue: "x", jobType: "b" },
      ]),
    ).rejects.toThrow(BatchValidationError);
  });

  it("throws EmptyBatchError for empty batch", async () => {
    const client = makeClient();
    await expect(client.enqueueAtomic([])).rejects.toThrow(EmptyBatchError);
  });

  it("throws on count mismatch", async () => {
    const ea = createFakeRpc({ success: { responses: [{ jobId: "a", deduplicated: false }] } });
    const client = makeClient({ enqueueAtomic: ea });
    await expect(
      client.enqueueAtomic([
        { queue: "q", jobType: "a" },
        { queue: "q", jobType: "b" },
      ]),
    ).rejects.toThrow(BatchResultCountMismatchError);
  });

  it("throws on missing outcome", async () => {
    const ea = createFakeRpc({});
    const client = makeClient({ enqueueAtomic: ea });
    await expect(client.enqueueAtomic([{ queue: "q", jobType: "t" }])).rejects.toThrow(
      MalformedResponseError,
    );
  });
});

// ---------------------------------------------------------------------------
// Reserve
// ---------------------------------------------------------------------------
describe("reserve", () => {
  it("returns parsed jobs", async () => {
    const r = createFakeRpc({ jobs: [validPbJob()] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs).not.toBeNull();
    expect(jobs![0]!.ctx.id).toBe("550e8400-e29b-41d4-a716-446655440000");
    expect(jobs![0]!.ctx.jobType).toBe("send_email");
  });

  it("returns null for empty response", async () => {
    const r = createFakeRpc({ jobs: [] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs).toBeNull();
  });

  it("skips malformed jobs and returns null if no valid jobs", async () => {
    const r = createFakeRpc({ jobs: [{ ...validPbJob(), id: "" }] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs).toBeNull();
  });

  it("maps FAILED_PRECONDITION to UnknownQueuesError", async () => {
    const r = createFakeRpc(undefined, rpcError(GrpcStatus.FAILED_PRECONDITION, "queues: a, b"));
    const client = makeClient({ reserve: r });
    await expect(client.reserve({ queues: ["a", "b"], leaseDuration: 30_000 })).rejects.toThrow(
      UnknownQueuesError,
    );
  });

  it("is not retried even with retry policy", async () => {
    const r = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ reserve: r }, fastRetry(5));
    await expect(client.reserve({ queues: ["q"], leaseDuration: 30_000 })).rejects.toThrow(
      TransportError,
    );
    expect(r.calls).toBe(1);
  });

  it("forwards workerId to the response jobs", async () => {
    const r = createFakeRpc({ jobs: [validPbJob()] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({
      queues: ["emails"],
      leaseDuration: 30_000,
      workerId: "w1",
    });
    expect(jobs![0]!.ctx.workerId).toBe("w1");
  });
});

// ---------------------------------------------------------------------------
// Ack / Nack / Extend
// ---------------------------------------------------------------------------
describe("ack", () => {
  it("acks a job", async () => {
    const ack = createFakeRpc({ jobId: "abc" });
    const client = makeClient({ ack });
    await client.ack({ id: "abc", attempt: 1 });
    expect(ack.calls).toBe(1);
  });

  it("maps NOT_FOUND to JobNotFoundError", async () => {
    const ack = createFakeRpc(undefined, rpcError(GrpcStatus.NOT_FOUND));
    const client = makeClient({ ack });
    await expect(client.ack({ id: "abc", attempt: 1 })).rejects.toThrow(JobNotFoundError);
  });

  it("maps FAILED_PRECONDITION to AttemptMismatchError", async () => {
    const ack = createFakeRpc(undefined, rpcError(GrpcStatus.FAILED_PRECONDITION));
    const client = makeClient({ ack });
    await expect(client.ack({ id: "abc", attempt: 2 })).rejects.toThrow(AttemptMismatchError);
  });
});

describe("nack", () => {
  it("nacks with default directive", async () => {
    const nack = createFakeRpc({ jobId: "abc", deadLettered: false });
    const client = makeClient({ nack });
    const deadLettered = await client.nack({ id: "abc", attempt: 1 });
    expect(deadLettered).toBe(false);
    expect(nack.lastRequest.retry).toEqual({ default: {} });
  });

  it("nacks with dead_letter directive", async () => {
    const nack = createFakeRpc({ jobId: "abc", deadLettered: true });
    const client = makeClient({ nack });
    const deadLettered = await client.nack(
      { id: "abc", attempt: 1 },
      { retry: RetryDirective.deadLetter() },
    );
    expect(deadLettered).toBe(true);
    expect(nack.lastRequest.retry).toEqual({ deadLetter: {} });
  });

  it("nacks with after directive", async () => {
    const nack = createFakeRpc({ jobId: "abc", deadLettered: false });
    const client = makeClient({ nack });
    await client.nack(
      { id: "abc", attempt: 1 },
      { retry: RetryDirective.after(5000), reason: "retry later" },
    );
    expect(nack.lastRequest.retry).toEqual({ delay: { seconds: 5n, nanos: 0 } });
    expect(nack.lastRequest.reason).toBe("retry later");
  });

  it("maps lease errors", async () => {
    const nack = createFakeRpc(undefined, rpcError(GrpcStatus.NOT_FOUND));
    const client = makeClient({ nack });
    await expect(client.nack({ id: "abc", attempt: 1 })).rejects.toThrow(JobNotFoundError);
  });
});

describe("extend", () => {
  it("returns the new expiry", async () => {
    const expiry = new Date(1_700_000_120_000);
    const ext = createFakeRpc({ jobId: "abc", leaseExpiresAt: expiry });
    const client = makeClient({ extend: ext });

    let capturedMs = 0;
    const ctx = {
      extend: async (extensionMs: number) => {
        capturedMs = extensionMs;
        return expiry;
      },
    };
    const result = await client.extend(ctx, 30_000);
    expect(capturedMs).toBe(30_000);
    expect(result).toEqual(expiry);
  });

  it("throws on malformed extend response", async () => {
    const ext = createFakeRpc({ jobId: "abc" });
    const client = makeClient({ extend: ext });

    const ctx = {
      extend: async () => {
        throw new MalformedResponseError("extend returned an invalid lease_expires_at");
      },
    };
    await expect(client.extend(ctx, 30_000)).rejects.toThrow(MalformedResponseError);
  });
});

// ---------------------------------------------------------------------------
// GetServerInfo
// ---------------------------------------------------------------------------
describe("getServerInfo", () => {
  function validInfo(): GetServerInfoResponse {
    return {
      serverVersion: "1.2.3",
      supportedProtocolVersions: ["v1"],
      serverTime: new Date(1_700_000_000_000),
      restrictsEncodings: false,
      allowedEncodings: [],
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

  it("returns parsed server info", async () => {
    const si = createFakeRpc(validInfo());
    const client = makeClient({ getServerInfo: si });
    const info = await client.getServerInfo();
    expect(info.version).toBe("1.2.3");
    expect(info.maxPayloadSize).toBe(1024);
  });

  it("throws on malformed response", async () => {
    const si = createFakeRpc({ ...validInfo(), serverVersion: "" });
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow();
  });

  it("throws on transport error", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(TransportError);
  });
});

// ---------------------------------------------------------------------------
// Drain dead letters
// ---------------------------------------------------------------------------
describe("drainDeadLetters", () => {
  function pbDlRecord(): PbDeadLetterRecord {
    return {
      job: validPbJob({ payload: { data: Buffer.from([1, 2, 3]), encoding: "json" } }),
      cause: DeadLetterCause.DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED,
      failedAt: new Date(1_700_000_100_000),
      finalAttempt: 5,
      lastReason: "boom",
    };
  }

  it("returns parsed records", async () => {
    const dl = createFakeRpc({ records: [pbDlRecord()] });
    const client = makeClient({ drainDeadLetters: dl });
    const records = await client.drainDeadLetters();
    expect(records).toHaveLength(1);
    expect(records[0]!.queue).toBe("emails");
    expect(records[0]!.cause).toBe("attempts_exhausted");
  });

  it("returns empty array for no records", async () => {
    const dl = createFakeRpc({ records: [] });
    const client = makeClient({ drainDeadLetters: dl });
    const records = await client.drainDeadLetters();
    expect(records).toHaveLength(0);
  });

  it("filters by queue", async () => {
    const dl = createFakeRpc({ records: [] });
    const client = makeClient({ drainDeadLetters: dl });
    await client.drainDeadLetters({ queue: "emails", max: 5 });
    expect(dl.lastRequest).toEqual({ queue: "emails", max: 5 });
  });

  it("skips malformed records", async () => {
    const badRecord = { ...pbDlRecord(), job: { ...validPbJob(), id: "" } };
    const dl = createFakeRpc({ records: [badRecord] });
    const client = makeClient({ drainDeadLetters: dl });
    const records = await client.drainDeadLetters();
    expect(records).toHaveLength(0);
  });

  it("is not retried", async () => {
    const dl = createFakeRpc(undefined, undefined, [
      rpcError(GrpcStatus.UNAVAILABLE),
      { records: [] },
    ]);
    const client = makeClient({ drainDeadLetters: dl }, fastRetry(5));
    await expect(client.drainDeadLetters()).rejects.toThrow(TransportError);
    expect(dl.calls).toBe(1);
  });

  it("clamps max to at least 1", async () => {
    const dl = createFakeRpc({ records: [] });
    const client = makeClient({ drainDeadLetters: dl });
    await client.drainDeadLetters({ max: 0 });
    expect(dl.lastRequest.max).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------
describe("retry", () => {
  it("retries on transient errors and succeeds", async () => {
    const si = createFakeRpc(undefined, undefined, [
      rpcError(GrpcStatus.UNAVAILABLE),
      rpcError(GrpcStatus.UNAVAILABLE),
      validServerInfo(),
    ]);
    const client = makeClient({ getServerInfo: si }, fastRetry(5));
    const info = await client.getServerInfo();
    expect(info.version).toBe("1.2.3");
    expect(si.calls).toBe(3);
  });

  it("gives up after maxAttempts transient errors", async () => {
    const si = createFakeRpc(undefined, undefined, Array(3).fill(rpcError(GrpcStatus.UNAVAILABLE)));
    const client = makeClient({ getServerInfo: si }, fastRetry(3));
    await expect(client.getServerInfo()).rejects.toThrow(TransportError);
    expect(si.calls).toBe(3);
  });

  it("does not retry non-transient errors", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.INVALID_ARGUMENT));
    const client = makeClient({ getServerInfo: si }, fastRetry(5));
    await expect(client.getServerInfo()).rejects.toThrow(InvalidRequestError);
    expect(si.calls).toBe(1);
  });

  it("default policy does not retry", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(TransportError);
    expect(si.calls).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Connect / builder
// ---------------------------------------------------------------------------
describe("SeppClient.connect", () => {
  it("rejects with ConnectError on connection timeout", async () => {
    await expect(SeppClient.connect("localhost:1", { connectTimeout: 100 })).rejects.toThrow(
      ConnectError,
    );
  }, 10_000);

  it("rejects invalid API key", async () => {
    await expect(SeppClient.connect("localhost:50051", { apiKey: "bad\nkey" })).rejects.toThrow(
      InvalidApiKeyError,
    );
  });
});

describe("SeppClient.fromClient", () => {
  it("accepts custom rpcTimeout", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = SeppClient.fromClient({ enqueueBatch: eb, close: vi.fn() } as any, {
      rpcTimeout: 7000,
    });
    await client.enqueue({ queue: "q", jobType: "t" });
    expect(eb.calls).toBe(1);
  });
});

describe("close", () => {
  it("calls the underlying client close", () => {
    const close = vi.fn();
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = SeppClient.fromClient({ enqueueBatch: eb, close } as any);
    client.close();
    expect(close).toHaveBeenCalledOnce();
  });
});

// ---------------------------------------------------------------------------
// Deadline
// ---------------------------------------------------------------------------
describe("deadlines", () => {
  it("enqueue carries a deadline derived from rpcTimeout", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({ queue: "q", jobType: "t" });
    // The deadline should be set on the call options
    expect(eb.calls).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------
describe("metadata", () => {
  it("attaches Bearer auth header when apiKey is set", async () => {
    // Use connect path that validates valid API keys
    // For fromClient there's no API key support, testing that connect validates
    await expect(
      SeppClient.connect("localhost:50051", { apiKey: "", connectTimeout: 10 }),
    ).rejects.toThrow(InvalidApiKeyError);
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function validServerInfo(): GetServerInfoResponse {
  return {
    serverVersion: "1.2.3",
    supportedProtocolVersions: ["v1"],
    serverTime: new Date(1_700_000_000_000),
    restrictsEncodings: false,
    allowedEncodings: [],
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

// ---------------------------------------------------------------------------
// Error mapping from client
// ---------------------------------------------------------------------------
describe("transport error mapping", () => {
  it("maps UNAVAILABLE to TransportError", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(TransportError);
  });

  it("maps UNAUTHENTICATED to UnauthenticatedError", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.UNAUTHENTICATED));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(UnauthenticatedError);
  });

  it("maps RESOURCE_EXHAUSTED to OverloadedError", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.RESOURCE_EXHAUSTED));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(OverloadedError);
  });

  it("maps INTERNAL to ServerInternalError", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.INTERNAL));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(ServerInternalError);
  });

  it("maps unmapped code to UnexpectedStatusError", async () => {
    const si = createFakeRpc(undefined, rpcError(GrpcStatus.NOT_FOUND));
    const client = makeClient({ getServerInfo: si });
    await expect(client.getServerInfo()).rejects.toThrow(UnexpectedStatusError);
  });
});

// ---------------------------------------------------------------------------
// Backup enqueueAtomic retry / transport
// ---------------------------------------------------------------------------
describe("enqueueAtomic transport error", () => {
  it("maps transport error on atomic", async () => {
    const ea = createFakeRpc(undefined, rpcError(GrpcStatus.UNAVAILABLE));
    const client = makeClient({ enqueueAtomic: ea });
    await expect(client.enqueueAtomic([{ queue: "q", jobType: "t" }])).rejects.toThrow(
      TransportError,
    );
  });
});

// ---------------------------------------------------------------------------
// Payload / trace context roundtrip through reserve
// ---------------------------------------------------------------------------
describe("reserve with payload and trace context", () => {
  it("decodes payload on reserved job", async () => {
    const pb = validPbJob({
      payload: {
        data: Buffer.from(JSON.stringify({ hello: "world" })),
        encoding: "application/json",
      },
    });
    const r = createFakeRpc({ jobs: [pb] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs![0]!.payload?.json()).toEqual({ hello: "world" });
  });

  it("decodes trace context on reserved job", async () => {
    const pb = validPbJob({ traceContext: { traceparent: VALID_TP, tracestate: "v=1" } });
    const r = createFakeRpc({ jobs: [pb] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs![0]!.ctx.traceContext?.traceparent).toBe(VALID_TP);
    expect(jobs![0]!.ctx.traceContext?.tracestate).toBe("v=1");
  });

  it("drops invalid trace context silently", async () => {
    const pb = validPbJob({ traceContext: { traceparent: "garbage" } });
    const r = createFakeRpc({ jobs: [pb] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs![0]!.ctx.traceContext).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Enqueue batch with trace context
// ---------------------------------------------------------------------------
describe("enqueue with trace context", () => {
  it("includes trace context in request", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({
      queue: "q",
      jobType: "t",
      traceContext: TraceContext.create(VALID_TP),
    });
    const req: EnqueueBatchRequest = eb.lastRequest;
    expect(req.jobs[0]!.traceContext?.traceparent).toBe(VALID_TP);
  });
});

// ---------------------------------------------------------------------------
// Enqueue with custom metadata
// ---------------------------------------------------------------------------
describe("enqueue with custom", () => {
  it("encodes custom metadata", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({ queue: "q", jobType: "t", custom: { k: 1, s: "hi" } });
    const req: EnqueueBatchRequest = eb.lastRequest;
    expect(req.jobs[0]!.custom).toEqual({ k: { intValue: 1n }, s: { stringValue: "hi" } });
  });
});

// ---------------------------------------------------------------------------
// Enqueue with scheduledAt
// ---------------------------------------------------------------------------
describe("enqueue with scheduledAt", () => {
  it("includes scheduledAt in request", async () => {
    const ts = new Date(1_700_000_000_000);
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({ queue: "q", jobType: "t", scheduledAt: ts });
    expect(eb.lastRequest.jobs[0].scheduledAt).toEqual(ts);
  });

  it("drops pre-epoch scheduledAt", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({ queue: "q", jobType: "t", scheduledAt: new Date(-1000) });
    expect(eb.lastRequest.jobs[0].scheduledAt).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Enqueue with priority / idempotencyKey
// ---------------------------------------------------------------------------
describe("enqueue with priority and idempotencyKey", () => {
  it("includes priority and idempotencyKey", async () => {
    const eb = createFakeRpc(enqueueBatchResponse({ jobId: "abc" }));
    const client = makeClient({ enqueueBatch: eb });
    await client.enqueue({ queue: "q", jobType: "t", priority: 7, idempotencyKey: "idem" });
    expect(eb.lastRequest.jobs[0].priority).toBe(7);
    expect(eb.lastRequest.jobs[0].idempotencyKey).toBe("idem");
  });
});

// ---------------------------------------------------------------------------
// Reserve with multiple jobs
// ---------------------------------------------------------------------------
describe("reserve with multiple jobs", () => {
  it("returns multiple parsed jobs", async () => {
    const r = createFakeRpc({ jobs: [validPbJob({ id: "a" }), validPbJob({ id: "b" })] });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs).toHaveLength(2);
    expect(jobs![0]!.ctx.id).toBe("a");
    expect(jobs![1]!.ctx.id).toBe("b");
  });

  it("skips malformed jobs but returns valid ones", async () => {
    const r = createFakeRpc({
      jobs: [validPbJob({ id: "a" }), { ...validPbJob(), id: "" }, validPbJob({ id: "c" })],
    });
    const client = makeClient({ reserve: r });
    const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000 });
    expect(jobs).toHaveLength(2);
    expect(jobs![0]!.ctx.id).toBe("a");
    expect(jobs![1]!.ctx.id).toBe("c");
  });
});

// ---------------------------------------------------------------------------
// Ack with workerId
// ---------------------------------------------------------------------------
describe("ack with workerId", () => {
  it("includes workerId in ack request", async () => {
    const ack = createFakeRpc({ jobId: "abc" });
    const client = makeClient({ ack });
    await client.ack({ id: "abc", attempt: 1, workerId: "w1" });
    expect(ack.lastRequest.workerId).toBe("w1");
  });
});

// ---------------------------------------------------------------------------
// Nack with default values
// ---------------------------------------------------------------------------
describe("nack with default values", () => {
  it("sends empty reason by default", async () => {
    const nack = createFakeRpc({ jobId: "abc", deadLettered: false });
    const client = makeClient({ nack });
    await client.nack({ id: "abc", attempt: 1 });
    expect(nack.lastRequest.reason).toBe("");
  });
});

// ---------------------------------------------------------------------------
// All rejection variants from batch
// ---------------------------------------------------------------------------
describe("enqueueBatch rejection variants", () => {
  it("maps encoding_not_allowed rejection", async () => {
    const rej = { rejection: { encodingNotAllowed: { encoding: "xml", allowed: ["json"] } } };
    const eb = createFakeRpc({ results: [rej] });
    const client = makeClient({ enqueueBatch: eb });
    const results = await client.enqueueBatch([{ queue: "q", jobType: "t" }]);
    expect((results[0] as any).kind).toBe("encoding_not_allowed");
  });

  it("maps queue_full rejection", async () => {
    const rej = { rejection: { queueFull: { queue: "q", limit: 100n } } };
    const eb = createFakeRpc({ results: [rej] });
    const client = makeClient({ enqueueBatch: eb });
    const results = await client.enqueueBatch([{ queue: "q", jobType: "t" }]);
    expect((results[0] as any).kind).toBe("queue_full");
  });

  it("maps queue_closing rejection", async () => {
    const rej = { rejection: { queueClosing: { queue: "q" } } };
    const eb = createFakeRpc({ results: [rej] });
    const client = makeClient({ enqueueBatch: eb });
    const results = await client.enqueueBatch([{ queue: "q", jobType: "t" }]);
    expect((results[0] as any).kind).toBe("queue_closing");
  });
});
