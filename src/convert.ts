/** Conversions between the generated protobuf messages and the domain types. */
import type { Duration } from "./gen/google/protobuf/duration";
import {
  DeadLetterCause as PbDeadLetterCause,
  type DeadLetterRecord as PbDeadLetterRecord,
  type EnqueueRequest as PbEnqueueRequest,
  type EnqueueResponse as PbEnqueueResponse,
  type GetServerInfoResponse,
  type Job as PbJob,
  type JobRejection as PbJobRejection,
  type JobValidationError as PbJobValidationError,
  type NackRetry,
  type Payload as PbPayload,
  type PrimitiveValue,
  type ReserveRequest as PbReserveRequest,
  type TraceContext as PbTraceContext,
} from "./gen/sepp/v1/queue";
import { JobConversionError, PriorityRangeError, ServerInfoError, ValidationError } from "./errors";
import {
  DeadLetterRecord,
  type DeadLetterCause,
  type EnqueueAck,
  type EnqueueRequest,
  type Job,
  JobCtx,
  type JobRejection,
  type JobValidationError,
  type LeaseExtendFn,
  Payload,
  type Primitive,
  type ReserveOptions,
  type RetryDirective,
  type ServerInfo,
  TraceContext,
} from "./types";

// ---------------------------------------------------------------------------
// Time helpers (ms <-> proto Duration; proto Timestamp arrives as Date)
// ---------------------------------------------------------------------------

const NANOS_PER_MS = 1_000_000;

export function msToDuration(ms: number): Duration {
  const clamped = Math.max(0, Math.trunc(ms));
  return {
    seconds: BigInt(Math.trunc(clamped / 1000)),
    nanos: (clamped % 1000) * NANOS_PER_MS,
  };
}

export function durationToMs(d: Duration | undefined): number {
  if (!d) return 0;
  return Number(d.seconds) * 1000 + Math.trunc(d.nanos / NANOS_PER_MS);
}

function bigintToNumber(value: bigint): number {
  return Number(value);
}

// ---------------------------------------------------------------------------
// Priority
// ---------------------------------------------------------------------------

/** Validates a caller-supplied priority is an integer in 0–9. */
export function validatePriority(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > 9) {
    throw new PriorityRangeError(value);
  }
  return value;
}

// ---------------------------------------------------------------------------
// Primitive (custom map values)
// ---------------------------------------------------------------------------

export function primitiveToPb(value: Primitive): PrimitiveValue {
  switch (typeof value) {
    case "string":
      return { stringValue: value };
    case "boolean":
      return { boolValue: value };
    case "bigint":
      return { intValue: value };
    case "number":
      return Number.isInteger(value) ? { intValue: BigInt(value) } : { doubleValue: value };
    default:
      throw new ValidationError(`unsupported custom value type: ${typeof value}`);
  }
}

export function primitiveFromPb(value: PrimitiveValue, key: string): Primitive {
  if (value.stringValue !== undefined) return value.stringValue;
  if (value.boolValue !== undefined) return value.boolValue;
  if (value.doubleValue !== undefined) return value.doubleValue;
  if (value.intValue !== undefined) {
    const n = value.intValue;
    return n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(n)
      : n;
  }
  throw new JobConversionError(`custom value for key \`${key}\` has no value set`);
}

function customToPb(custom: Record<string, Primitive> | undefined): { [k: string]: PrimitiveValue } {
  const out: { [k: string]: PrimitiveValue } = {};
  if (custom) {
    for (const [k, v] of Object.entries(custom)) out[k] = primitiveToPb(v);
  }
  return out;
}

function customFromPb(custom: { [k: string]: PrimitiveValue }): Record<string, Primitive> {
  const out: Record<string, Primitive> = {};
  for (const [k, v] of Object.entries(custom)) out[k] = primitiveFromPb(v, k);
  return out;
}

// ---------------------------------------------------------------------------
// Payload & trace context
// ---------------------------------------------------------------------------

function payloadToPb(p: Payload): PbPayload {
  return { data: Buffer.from(p.data), encoding: p.encoding };
}

function payloadFromPb(p: PbPayload): Payload {
  return new Payload(p.data, p.encoding);
}

function traceContextToPb(tc: TraceContext): PbTraceContext {
  return { traceparent: tc.traceparent, tracestate: tc.tracestate };
}

/** Decodes a trace context, returning undefined if it is absent or malformed (never throws). */
function traceContextFromPb(tc: PbTraceContext | undefined): TraceContext | undefined {
  if (!tc) return undefined;
  try {
    const ctx = TraceContext.create(tc.traceparent);
    return tc.tracestate !== undefined ? ctx.withTracestate(tc.tracestate) : ctx;
  } catch {
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

/** A proto Timestamp arrives as a Date; require it to be present and representable (>= epoch). */
function requireTimestamp(date: Date | undefined, field: string): Date {
  if (!date) {
    throw new JobConversionError(`timestamp \`${field}\` is not a representable time (0ms)`);
  }
  if (date.getTime() < 0) {
    throw new JobConversionError(
      `timestamp \`${field}\` is not a representable time (${date.getTime()}ms)`,
    );
  }
  return date;
}

// ---------------------------------------------------------------------------
// Enqueue request
// ---------------------------------------------------------------------------

export function enqueueRequestToPb(input: EnqueueRequest): PbEnqueueRequest {
  if (input.queue.length === 0) throw new ValidationError("queue name must not be empty");
  if (input.jobType.length === 0) throw new ValidationError("job type must not be empty");
  if (input.priority !== undefined) validatePriority(input.priority);

  // A pre-epoch scheduled_at is dropped (treated as unset), matching the Rust client.
  const scheduledAt =
    input.scheduledAt && input.scheduledAt.getTime() >= 0 ? input.scheduledAt : undefined;

  return {
    queue: input.queue,
    jobType: input.jobType,
    payload: input.payload ? payloadToPb(input.payload) : undefined,
    idempotencyKey: input.idempotencyKey,
    priority: input.priority,
    maxAttempts: input.maxAttempts,
    traceContext: input.traceContext ? traceContextToPb(input.traceContext) : undefined,
    custom: customToPb(input.custom),
    scheduledAt,
  };
}

export function ackFromPb(r: PbEnqueueResponse): EnqueueAck {
  return { jobId: r.jobId, deduplicated: r.deduplicated };
}

// ---------------------------------------------------------------------------
// Job rejections
// ---------------------------------------------------------------------------

export function jobRejectionFromPb(r: PbJobRejection): JobRejection {
  if (r.unknownQueue) return { kind: "unknown_queue", queue: r.unknownQueue.queue };
  if (r.payloadTooLarge) {
    return {
      kind: "payload_too_large",
      limit: bigintToNumber(r.payloadTooLarge.limit),
      actual: bigintToNumber(r.payloadTooLarge.actual),
    };
  }
  if (r.encodingNotAllowed) {
    return {
      kind: "encoding_not_allowed",
      encoding: r.encodingNotAllowed.encoding,
      allowed: r.encodingNotAllowed.allowed,
    };
  }
  if (r.jobTypeNotAllowed) {
    return {
      kind: "job_type_not_allowed",
      jobType: r.jobTypeNotAllowed.jobType,
      allowed: r.jobTypeNotAllowed.allowed,
    };
  }
  if (r.customEntriesTooMany) {
    return {
      kind: "custom_entries_too_many",
      limit: r.customEntriesTooMany.limit,
      actual: r.customEntriesTooMany.actual,
    };
  }
  if (r.customMapTooLarge) {
    return {
      kind: "custom_map_too_large",
      limit: bigintToNumber(r.customMapTooLarge.limit),
      actual: bigintToNumber(r.customMapTooLarge.actual),
    };
  }
  if (r.customKeyTooLong) {
    return {
      kind: "custom_key_too_long",
      key: r.customKeyTooLong.key,
      limit: r.customKeyTooLong.limit,
      actual: bigintToNumber(r.customKeyTooLong.actual),
    };
  }
  if (r.queueNameTooLong) {
    return {
      kind: "queue_name_too_long",
      limit: r.queueNameTooLong.limit,
      actual: bigintToNumber(r.queueNameTooLong.actual),
    };
  }
  if (r.jobTypeNameTooLong) {
    return {
      kind: "job_type_name_too_long",
      limit: r.jobTypeNameTooLong.limit,
      actual: bigintToNumber(r.jobTypeNameTooLong.actual),
    };
  }
  if (r.idempotencyKeyTooLong) {
    return {
      kind: "idempotency_key_too_long",
      limit: r.idempotencyKeyTooLong.limit,
      actual: bigintToNumber(r.idempotencyKeyTooLong.actual),
    };
  }
  if (r.scheduledTooFar) {
    return {
      kind: "scheduled_too_far",
      horizon: durationToMs(r.scheduledTooFar.horizon),
      actual: r.scheduledTooFar.actual ?? new Date(0),
    };
  }
  if (r.invalidRequest) return { kind: "invalid_request", message: r.invalidRequest.message };
  if (r.queueFull) {
    return {
      kind: "queue_full",
      queue: r.queueFull.queue,
      limit: bigintToNumber(r.queueFull.limit),
    };
  }
  if (r.queueClosing) return { kind: "queue_closing", queue: r.queueClosing.queue };
  return { kind: "unknown" };
}

export function jobValidationErrorFromPb(e: PbJobValidationError): JobValidationError {
  return {
    index: e.index,
    rejection: e.rejection ? jobRejectionFromPb(e.rejection) : { kind: "unknown" },
  };
}

// ---------------------------------------------------------------------------
// Reserve request
// ---------------------------------------------------------------------------

export function reserveOptionsToPb(input: ReserveOptions): PbReserveRequest {
  if (input.queues.length === 0) throw new ValidationError("at least one queue must be specified");
  input.queues.forEach((q, i) => {
    if (q.length === 0) throw new ValidationError(`queue name at index ${i} must not be empty`);
  });
  if (input.leaseDuration <= 0) throw new ValidationError("leaseDuration must be at least 1ms");
  if (input.workerId !== undefined && input.workerId.length === 0) {
    throw new ValidationError("workerId must not be empty when set");
  }

  return {
    queues: input.queues,
    waitTimeout: msToDuration(input.waitTimeout ?? 30_000),
    leaseDuration: msToDuration(input.leaseDuration),
    workerId: input.workerId,
    maxJobs: input.maxJobs,
  };
}

// ---------------------------------------------------------------------------
// Reserved jobs
// ---------------------------------------------------------------------------

function priorityFromPb(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > 9) {
    throw new JobConversionError(`job priority ${value} is out of range (expected 0-9)`);
  }
  return value;
}

export function jobFromPb(extend: LeaseExtendFn, j: PbJob, workerId: string | undefined): Job {
  if (j.id.length === 0) throw new JobConversionError("job is missing required field `id`");
  if (j.jobType.length === 0) throw new JobConversionError("job is missing required field `job_type`");

  const priority = priorityFromPb(j.priority);
  const enqueuedAt = requireTimestamp(j.enqueuedAt, "enqueued_at");
  const leaseExpiresAt = requireTimestamp(j.leaseExpiresAt, "lease_expires_at");
  const custom = customFromPb(j.custom);
  const traceContext = traceContextFromPb(j.traceContext);

  const ctx = new JobCtx({
    id: j.id,
    queue: j.queue,
    jobType: j.jobType,
    priority,
    attempt: j.attempt,
    maxAttempts: j.maxAttempts,
    enqueuedAt,
    custom,
    traceContext,
    leaseExpiresAt,
    workerId,
    extend,
  });

  return { payload: j.payload ? payloadFromPb(j.payload) : undefined, ctx };
}

// ---------------------------------------------------------------------------
// Server info
// ---------------------------------------------------------------------------

export function serverInfoFromPb(r: GetServerInfoResponse): ServerInfo {
  if (r.serverVersion.length === 0) {
    throw new ServerInfoError("server info is missing required field `server_version`");
  }
  if (!r.serverTime || r.serverTime.getTime() < 0) {
    throw new ServerInfoError(
      `server_time is not a representable time (${r.serverTime ? r.serverTime.getTime() : 0}ms)`,
    );
  }

  return {
    version: r.serverVersion,
    supportedProtocolVersions: r.supportedProtocolVersions,
    serverTime: r.serverTime,
    restrictsEncodings: r.restrictsEncodings,
    allowedEncodings: r.allowedEncodings,
    maxPayloadSize: bigintToNumber(r.maxPayloadBytes),
    maxCustomEntries: r.maxCustomEntries,
    maxCustomTotalBytes: bigintToNumber(r.maxCustomTotalBytes),
    maxCustomKeyBytes: r.maxCustomKeyBytes,
    maxQueueNameBytes: r.maxQueueNameBytes,
    maxJobTypeBytes: r.maxJobTypeBytes,
    maxIdempotencyKeyBytes: r.maxIdempotencyKeyBytes,
    maxScheduleHorizon: durationToMs(r.maxScheduleHorizon),
    maxEnqueueBatch: r.maxEnqueueBatch,
    maxReserveBatch: r.maxReserveBatch,
    maxReserveQueues: r.maxReserveQueues,
    maxWaitTimeout: durationToMs(r.maxWaitTimeout),
    maxLeaseDuration: durationToMs(r.maxLeaseDuration),
    strictQueues: r.strictQueues,
    deadLetterRetentionEnabled: r.deadLetterRetentionEnabled,
  };
}

// ---------------------------------------------------------------------------
// Dead-letter records
// ---------------------------------------------------------------------------

function deadLetterCauseFromPb(c: PbDeadLetterCause): DeadLetterCause {
  switch (c) {
    case PbDeadLetterCause.DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED:
      return "attempts_exhausted";
    case PbDeadLetterCause.DEAD_LETTER_CAUSE_REJECTED:
      return "rejected";
    case PbDeadLetterCause.DEAD_LETTER_CAUSE_LEASE_EXPIRED:
      return "lease_expired";
    case PbDeadLetterCause.DEAD_LETTER_CAUSE_ADMIN:
      return "admin";
    default:
      return "unspecified";
  }
}

export function deadLetterRecordFromPb(r: PbDeadLetterRecord): DeadLetterRecord {
  const j = r.job;
  if (!j) throw new JobConversionError("dead-letter record is missing required field `job`");
  if (j.id.length === 0) throw new JobConversionError("job is missing required field `id`");
  if (j.jobType.length === 0) throw new JobConversionError("job is missing required field `job_type`");

  const priority = priorityFromPb(j.priority);
  const enqueuedAt = requireTimestamp(j.enqueuedAt, "enqueued_at");
  const failedAt = requireTimestamp(r.failedAt, "failed_at");
  const custom = customFromPb(j.custom);
  const traceContext = traceContextFromPb(j.traceContext);

  return new DeadLetterRecord({
    queue: j.queue,
    jobId: j.id,
    jobType: j.jobType,
    payload: j.payload ? payloadFromPb(j.payload) : undefined,
    priority,
    custom,
    traceContext,
    enqueuedAt,
    cause: deadLetterCauseFromPb(r.cause),
    failedAt,
    finalAttempt: r.finalAttempt,
    lastReason: r.lastReason,
  });
}

// ---------------------------------------------------------------------------
// Nack retry directive
// ---------------------------------------------------------------------------

export function retryDirectiveToPb(d: RetryDirective): NackRetry {
  switch (d.kind) {
    case "default":
      return { default: {} };
    case "after":
      return { delay: msToDuration(d.delayMs ?? 0) };
    case "dead_letter":
      return { deadLetter: {} };
  }
}
