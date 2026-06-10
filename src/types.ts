/** The public value types and input shapes. */
import { currentTraceContext, getOtel, validateTraceparent } from "./trace";

// ---------------------------------------------------------------------------
// Payload
// ---------------------------------------------------------------------------

/**
 * The opaque body of a job, plus an encoding hint.
 *
 * The queue never interprets `data`; it only carries the bytes. `encoding`
 * (e.g. `"application/json"`) is a hint the producer sets and the worker reads
 * to decide how to deserialize.
 */
export class Payload {
  constructor(
    readonly data: Uint8Array,
    readonly encoding: string,
  ) {}

  /** A payload from raw bytes and an encoding hint. */
  static bytes(data: Uint8Array, encoding: string): Payload {
    return new Payload(data, encoding);
  }

  /** A UTF-8 text payload (default encoding `text/plain; charset=utf-8`). */
  static text(text: string, encoding = "text/plain; charset=utf-8"): Payload {
    return new Payload(Buffer.from(text, "utf8"), encoding);
  }

  /** A JSON payload (default encoding `application/json`). */
  static json(value: unknown, encoding = "application/json"): Payload {
    return new Payload(Buffer.from(JSON.stringify(value), "utf8"), encoding);
  }

  /** Decodes the payload bytes as UTF-8 text. */
  text(): string {
    return Buffer.from(this.data).toString("utf8");
  }

  /** Decodes the payload bytes as JSON. */
  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }
}

// ---------------------------------------------------------------------------
// Priority & Primitive
// ---------------------------------------------------------------------------

/**
 * Job priority constants. Priorities run 0 (lowest) to 9 (highest, dequeued
 * first). Anywhere a priority is accepted you may pass a plain number 0–9.
 */
export const Priority = {
  MIN: 0,
  MAX: 9,
  P0: 0,
  P1: 1,
  P2: 2,
  P3: 3,
  P4: 4,
  P5: 5,
  P6: 6,
  P7: 7,
  P8: 8,
  P9: 9,
} as const;

/**
 * A JSON-primitive value stored in a job's `custom` metadata map.
 *
 * On the way out, a `number` that is an integer is sent as an int64 and a
 * non-integer `number` as a double; `bigint` is always an int64. On the way
 * back, an int64 is decoded as a `number` when it fits safely, otherwise as a
 * `bigint`.
 */
export type Primitive = string | number | boolean | bigint;

// ---------------------------------------------------------------------------
// Trace context
// ---------------------------------------------------------------------------

/**
 * A W3C Trace Context attached to a job, linking a producer's trace to the
 * worker that processes the job. The `traceparent` is validated on construction.
 */
export class TraceContext {
  private constructor(
    readonly traceparent: string,
    readonly tracestate?: string,
  ) {}

  /** Creates a trace context from a W3C `traceparent`, validating its format. */
  static create(traceparent: string, tracestate?: string): TraceContext {
    validateTraceparent(traceparent);
    return new TraceContext(traceparent, tracestate);
  }

  /** Returns a copy with the given vendor-specific `tracestate`. */
  withTracestate(tracestate: string): TraceContext {
    return new TraceContext(this.traceparent, tracestate);
  }

  /**
   * Captures the current OpenTelemetry context as a `TraceContext`, or null if
   * there is no valid active span (or `@opentelemetry/api` is not loaded).
   */
  static fromCurrentOtel(): TraceContext | null {
    const tc = currentTraceContext();
    if (!tc) return null;
    return new TraceContext(tc.traceparent, tc.tracestate);
  }

  /** Whether the OpenTelemetry API is available for trace propagation. */
  static otelAvailable(): boolean {
    return getOtel() !== null;
  }
}

// ---------------------------------------------------------------------------
// Enqueue request & ack
// ---------------------------------------------------------------------------

/** A job to enqueue. Required fields first; everything else is optional. */
export interface EnqueueRequest {
  /** Target queue (non-empty). */
  queue: string;
  /** Type tag for worker-side dispatch (non-empty). */
  jobType: string;
  /** Opaque payload with encoding hint for the worker. */
  payload?: Payload;
  /** Deduplication key. The server drops duplicates within the queue's window. */
  idempotencyKey?: string;
  /** Priority override, 0–9. If unset, the queue default is used. */
  priority?: number;
  /** Max delivery attempts override. If unset, the queue default is used. */
  maxAttempts?: number;
  /** Custom JSON-primitive metadata. */
  custom?: Record<string, Primitive>;
  /** Trace context to attach. With OTel loaded, the active context is attached automatically. */
  traceContext?: TraceContext;
  /** Delay delivery until this time. Must be within the server's max schedule horizon. */
  scheduledAt?: Date;
}

/** Confirmation that a job was accepted by the server. */
export interface EnqueueAck {
  /** The server-assigned job id (a UUID). When `deduplicated`, the id of the pre-existing job. */
  jobId: string;
  /** True if an idempotency key matched an existing job, so this enqueue was a no-op. */
  deduplicated: boolean;
}

// ---------------------------------------------------------------------------
// Job rejections (per-job, deterministic)
// ---------------------------------------------------------------------------

/**
 * Why the server refused a single job. A discriminated union keyed on `kind`;
 * every variant except `queue_full` is deterministic (re-sending the same job
 * against the same server state yields the same rejection). Transient problems
 * surface as a {@link ClientError} instead.
 */
export type JobRejection =
  | { kind: "unknown_queue"; queue: string }
  | { kind: "payload_too_large"; limit: number; actual: number }
  | { kind: "encoding_not_allowed"; encoding: string; allowed: string[] }
  | { kind: "job_type_not_allowed"; jobType: string; allowed: string[] }
  | { kind: "custom_entries_too_many"; limit: number; actual: number }
  | { kind: "custom_map_too_large"; limit: number; actual: number }
  | { kind: "custom_key_too_long"; key: string; limit: number; actual: number }
  | { kind: "queue_name_too_long"; limit: number; actual: number }
  | { kind: "job_type_name_too_long"; limit: number; actual: number }
  | { kind: "idempotency_key_too_long"; limit: number; actual: number }
  | { kind: "scheduled_too_far"; horizon: number; actual: Date }
  | { kind: "invalid_request"; message: string }
  /** The only non-deterministic rejection: clears once the queue drains below `limit`. */
  | { kind: "queue_full"; queue: string; limit: number }
  | { kind: "unknown" };

/** A single job's rejection within an atomic batch, paired with its request index. */
export interface JobValidationError {
  /** Zero-based position of the offending job in the submitted batch. */
  index: number;
  /** Why that job was rejected. */
  rejection: JobRejection;
}

/** Narrows an enqueue-batch result to a {@link JobRejection}. */
export function isJobRejection(value: EnqueueAck | JobRejection): value is JobRejection {
  return "kind" in value;
}

/** A human-readable description of a rejection. */
export function describeJobRejection(r: JobRejection): string {
  switch (r.kind) {
    case "unknown_queue":
      return `queue "${r.queue}" is not declared on the server (strict mode)`;
    case "payload_too_large":
      return `payload size ${r.actual} bytes exceeds the queue limit of ${r.limit}`;
    case "encoding_not_allowed":
      return `payload encoding "${r.encoding}" is not allowed; accepted: ${r.allowed.join(", ")}`;
    case "job_type_not_allowed":
      return `job_type "${r.jobType}" is not accepted by this queue; accepted: ${r.allowed.join(", ")}`;
    case "custom_entries_too_many":
      return `custom map has ${r.actual} entries, exceeding the queue limit of ${r.limit}`;
    case "custom_map_too_large":
      return `custom map's total size ${r.actual} bytes exceeds the queue limit of ${r.limit}`;
    case "custom_key_too_long":
      return `custom key "${r.key}" is ${r.actual} bytes, exceeding the limit of ${r.limit}`;
    case "queue_name_too_long":
      return `queue name is ${r.actual} bytes, exceeding the limit of ${r.limit}`;
    case "job_type_name_too_long":
      return `job_type is ${r.actual} bytes, exceeding the limit of ${r.limit}`;
    case "idempotency_key_too_long":
      return `idempotency_key is ${r.actual} bytes, exceeding the limit of ${r.limit}`;
    case "scheduled_too_far":
      return `scheduled_at ${r.actual.toISOString()} is beyond the max schedule horizon (${r.horizon}ms)`;
    case "invalid_request":
      return `structural validation failed: ${r.message}`;
    case "queue_full":
      return `queue "${r.queue}" is at its capacity of ${r.limit} jobs`;
    case "unknown":
      return "server returned an unrecognized rejection variant";
  }
}

// ---------------------------------------------------------------------------
// Reserve options
// ---------------------------------------------------------------------------

/** Parameters for a {@link SeppClient.reserve} call. */
export interface ReserveOptions {
  /** Queues to pull from, polled in order: index 0 first, then index 1, etc. */
  queues: string[];
  /** Lease duration, in milliseconds. The server may clamp to its max lease duration. */
  leaseDuration: number;
  /** How long the server holds the connection open waiting for a job (ms). Default 30_000. */
  waitTimeout?: number;
  /** A stable worker identifier, recorded server-side for observability. */
  workerId?: string;
  /** Maximum jobs to return in one response (default 1). The server clamps to its max. */
  maxJobs?: number;
}

// ---------------------------------------------------------------------------
// Reserved jobs & lease handle
// ---------------------------------------------------------------------------

/** @internal Extends a lease and returns the new expiry. Wired up by the client. */
export type LeaseExtendFn = (
  jobId: string,
  attempt: number,
  extensionMs: number,
  workerId: string | undefined,
) => Promise<Date>;

/** @internal Fields used to build a {@link JobCtx}. */
export interface JobCtxInit {
  id: string;
  queue: string;
  jobType: string;
  priority: number;
  attempt: number;
  maxAttempts: number;
  enqueuedAt: Date;
  custom: Record<string, Primitive>;
  traceContext?: TraceContext;
  leaseExpiresAt: Date;
  workerId?: string;
  extend: LeaseExtendFn;
}

/**
 * Identity, delivery metadata, and the lease handle for a reserved job.
 *
 * A handler receives this alongside the payload. Call {@link JobCtx.extend} to
 * renew the lease from inside a long-running handler.
 */
export class JobCtx {
  /** Server-assigned job id (a UUID). */
  readonly id: string;
  /** The queue this job was reserved from. */
  readonly queue: string;
  /** The job type, used to route to a handler. */
  readonly jobType: string;
  /** The job's effective priority (0–9). */
  readonly priority: number;
  /** Which delivery attempt this is, starting at 1 and incremented on each redelivery. */
  readonly attempt: number;
  /** The maximum attempts before the job is dead-lettered. */
  readonly maxAttempts: number;
  /** When the producer enqueued the job. */
  readonly enqueuedAt: Date;
  /** Custom metadata the producer attached. */
  readonly custom: Record<string, Primitive>;
  /** The producer's trace context, if any and if it was well-formed. */
  readonly traceContext?: TraceContext;
  /** The worker id recorded for this reservation, if any. */
  readonly workerId?: string;
  /** When the current lease expires. Updated by {@link JobCtx.extend}. */
  leaseExpiresAt: Date;

  /** @internal */ _knownExpiryMs: number;
  /** @internal */ _leaseLost = false;
  readonly #extend: LeaseExtendFn;
  #abort?: AbortController;

  constructor(init: JobCtxInit) {
    this.id = init.id;
    this.queue = init.queue;
    this.jobType = init.jobType;
    this.priority = init.priority;
    this.attempt = init.attempt;
    this.maxAttempts = init.maxAttempts;
    this.enqueuedAt = init.enqueuedAt;
    this.custom = init.custom;
    this.traceContext = init.traceContext;
    this.workerId = init.workerId;
    this.leaseExpiresAt = init.leaseExpiresAt;
    this._knownExpiryMs = init.leaseExpiresAt.getTime();
    this.#extend = init.extend;
  }

  /**
   * An AbortSignal that fires if a {@link Worker} with auto-extend detects the
   * lease was lost (reassigned by the server). Cooperative handlers can watch
   * it to bail out and avoid double-processing. Undefined for manually reserved
   * jobs.
   */
  get signal(): AbortSignal | undefined {
    return this.#abort?.signal;
  }

  /** Extends this job's lease by `extensionMs`, measured from now; returns the new expiry. */
  async extend(extensionMs: number): Promise<Date> {
    const expiry = await this.#extend(this.id, this.attempt, extensionMs, this.workerId);
    this.leaseExpiresAt = expiry;
    this._knownExpiryMs = expiry.getTime();
    return expiry;
  }

  /** @internal */ _bindAbort(ac: AbortController): void {
    this.#abort = ac;
  }

  /** @internal */ _markLeaseLost(): void {
    this._leaseLost = true;
    this.#abort?.abort();
  }
}

/** A reserved job: its optional {@link Payload} and its {@link JobCtx}. */
export interface Job {
  payload?: Payload;
  ctx: JobCtx;
}

// ---------------------------------------------------------------------------
// Nack retry directive
// ---------------------------------------------------------------------------

/**
 * How the server should handle a nacked job's next delivery. This is job-level
 * retry (the handler failed), distinct from {@link RetryPolicy} (the RPC failed).
 */
export class RetryDirective {
  private constructor(
    readonly kind: "default" | "after" | "dead_letter",
    readonly delayMs?: number,
  ) {}

  /** Apply the queue's configured retry policy. */
  static default(): RetryDirective {
    return new RetryDirective("default");
  }

  /** Retry, but not before `delayMs` has elapsed. */
  static after(delayMs: number): RetryDirective {
    return new RetryDirective("after", Math.max(0, delayMs));
  }

  /** Do not retry; send the job straight to the dead-letter queue. */
  static deadLetter(): RetryDirective {
    return new RetryDirective("dead_letter");
  }
}

// ---------------------------------------------------------------------------
// Server info
// ---------------------------------------------------------------------------

/** The server's version, capabilities, and limits, from {@link SeppClient.getServerInfo}. */
export interface ServerInfo {
  version: string;
  supportedProtocolVersions: string[];
  serverTime: Date;
  restrictsEncodings: boolean;
  allowedEncodings: string[];
  maxPayloadSize: number;
  maxCustomEntries: number;
  maxCustomTotalBytes: number;
  maxCustomKeyBytes: number;
  maxQueueNameBytes: number;
  maxJobTypeBytes: number;
  maxIdempotencyKeyBytes: number;
  /** How far ahead a job may be scheduled, in milliseconds. */
  maxScheduleHorizon: number;
  maxEnqueueBatch: number;
  maxReserveBatch: number;
  maxReserveQueues: number;
  /** Maximum long-poll wait, in milliseconds. */
  maxWaitTimeout: number;
  /** Maximum lease duration, in milliseconds. */
  maxLeaseDuration: number;
  strictQueues: boolean;
  deadLetterRetentionEnabled: boolean;
}

// ---------------------------------------------------------------------------
// Dead-letter records
// ---------------------------------------------------------------------------

/** Why a job was moved to the dead-letter store. */
export type DeadLetterCause =
  | "unspecified"
  | "attempts_exhausted"
  | "rejected"
  | "lease_expired"
  | "admin";

/** @internal */ export interface DeadLetterRecordInit {
  queue: string;
  jobId: string;
  jobType: string;
  payload?: Payload;
  priority: number;
  custom: Record<string, Primitive>;
  traceContext?: TraceContext;
  enqueuedAt: Date;
  cause: DeadLetterCause;
  failedAt: Date;
  finalAttempt: number;
  lastReason?: string;
}

/**
 * A dead-lettered job retained by the server, from
 * {@link SeppClient.drainDeadLetters}. A snapshot for inspection and manual
 * replay — call {@link DeadLetterRecord.toEnqueueRequest} to re-submit it.
 */
export class DeadLetterRecord {
  readonly queue: string;
  readonly jobId: string;
  readonly jobType: string;
  readonly payload?: Payload;
  readonly priority: number;
  readonly custom: Record<string, Primitive>;
  readonly traceContext?: TraceContext;
  readonly enqueuedAt: Date;
  readonly cause: DeadLetterCause;
  readonly failedAt: Date;
  readonly finalAttempt: number;
  readonly lastReason?: string;

  constructor(init: DeadLetterRecordInit) {
    this.queue = init.queue;
    this.jobId = init.jobId;
    this.jobType = init.jobType;
    this.payload = init.payload;
    this.priority = init.priority;
    this.custom = init.custom;
    this.traceContext = init.traceContext;
    this.enqueuedAt = init.enqueuedAt;
    this.cause = init.cause;
    this.failedAt = init.failedAt;
    this.finalAttempt = init.finalAttempt;
    this.lastReason = init.lastReason;
  }

  /**
   * Builds an {@link EnqueueRequest} that replays this job into its original
   * queue, preserving payload, priority, custom metadata, and trace context.
   * The replay is a fresh job — the server assigns a new id and resets attempts.
   */
  toEnqueueRequest(): EnqueueRequest {
    return {
      queue: this.queue,
      jobType: this.jobType,
      payload: this.payload,
      priority: this.priority,
      custom: this.custom,
      traceContext: this.traceContext,
    };
  }
}
