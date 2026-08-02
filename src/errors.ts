/**
 * The error hierarchy.
 *
 * Every error thrown by this library descends from {@link SeppError}. Transport
 * and protocol failures are {@link ClientError}s; lease, reserve, and enqueue
 * operations add their own typed errors on top.
 */

import type { ServiceError } from "@grpc/grpc-js";
import { status as GrpcStatus } from "@grpc/grpc-js";

import type { JobRejection, JobValidationError } from "./types";

/** Base class for every error this library throws. */
export class SeppError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** A transport- or protocol-level failure, shared by most RPCs. */
export class ClientError extends SeppError {}

/** Establishing the connection failed. */
export class ConnectError extends ClientError {
  constructor(
    readonly address: string,
    reason: string,
  ) {
    super(`could not connect to sepp server at ${address}: ${reason}`);
  }
}

/** The configured API key could not be encoded as an HTTP header value. */
export class InvalidApiKeyError extends ClientError {
  constructor() {
    super("the API key is not a valid HTTP header value");
  }
}

/**
 * A transient transport-level failure (connection dropped, deadline exceeded,
 * request aborted/cancelled). Generally safe to retry.
 */
export class TransportError extends ClientError {}

/** The server rejected the credentials (missing/invalid API key, or permission denied). */
export class UnauthenticatedError extends ClientError {}

/** The server is shedding load (`RESOURCE_EXHAUSTED`); back off and retry. */
export class OverloadedError extends ClientError {}

/** The server rejected the request as malformed (`INVALID_ARGUMENT`). */
export class InvalidRequestError extends ClientError {}

/** The server hit an internal error (`INTERNAL` / `DATA_LOSS` / `UNKNOWN`). */
export class ServerInternalError extends ClientError {}

/** The server returned a status code this client does not map to a more specific variant. */
export class UnexpectedStatusError extends ClientError {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(`server returned unexpected status ${code}: ${message}`);
  }
}

/** An enqueue was attempted with no jobs. */
export class EmptyBatchError extends ClientError {
  constructor() {
    super("empty batch");
  }
}

/** The server returned a different number of results than jobs sent — a protocol violation. */
export class BatchResultCountMismatchError extends ClientError {
  constructor(
    readonly expected: number,
    readonly got: number,
  ) {
    super(`server returned ${got} results for a batch of ${expected} jobs`);
  }
}

/** A response was missing a field the protocol requires. */
export class MalformedResponseError extends ClientError {
  constructor(detail: string) {
    super(`malformed response: ${detail}`);
  }
}

/** A job received from the server could not be decoded into a {@link Job}. */
export class JobConversionError extends SeppError {}

/** A `getServerInfo` response could not be decoded into a {@link ServerInfo}. */
export class ServerInfoError extends SeppError {}

// ---------------------------------------------------------------------------
// Lease errors (ack / nack / extend)
// ---------------------------------------------------------------------------

/**
 * Base for the lease operations. Both {@link JobNotFoundError} and
 * {@link AttemptMismatchError} mean the worker no longer holds the lease —
 * typically because it was allowed to expire and the job was redelivered.
 */
export class LeaseError extends SeppError {}

/**
 * No in-flight job has this id: it was already acked, the lease expired, or it
 * never existed. From a retried ack/nack this is ambiguous: an earlier attempt
 * of the same call may have succeeded with only its response lost.
 */
export class JobNotFoundError extends LeaseError {
  constructor() {
    super("no in-flight job with this id (already acked, expired, or never existed)");
  }
}

/** The attempt number no longer matches the server's: the lease was reassigned. */
export class AttemptMismatchError extends LeaseError {
  constructor() {
    super("attempt mismatch: the lease was reassigned");
  }
}

/** Base for reserve failures. */
export class ReserveError extends SeppError {}

/** The server is in strict mode and one or more requested queues are not declared. */
export class UnknownQueuesError extends ReserveError {}

/** Thrown by {@link SeppClient.enqueue} when the server rejects the single job. */
export class JobRejectedError extends SeppError {
  constructor(readonly rejection: JobRejection) {
    super(`server rejected the job: ${rejection.kind}`);
  }
}

/** Thrown by {@link SeppClient.enqueueAtomic} when one or more jobs fail validation. */
export class BatchValidationError extends SeppError {
  constructor(readonly errors: JobValidationError[]) {
    super(`atomic batch rejected: ${errors.length} job(s) failed validation`);
  }
}

/** A value supplied by the caller failed client-side validation before any RPC was made. */
export class ValidationError extends SeppError {}

/** A priority outside the range 0–9 was supplied. */
export class PriorityRangeError extends ValidationError {
  constructor(value: number) {
    super(`priority must be 0-9, got ${value}`);
  }
}

/** A `traceparent` did not match the W3C `version-trace_id-span_id-flags` shape. */
export class TraceContextError extends ValidationError {}

/** A handler was registered twice for the same job type via {@link Worker.handle}. */
export class DuplicateHandlerError extends SeppError {
  constructor(readonly jobType: string) {
    super(`handler for job_type "${jobType}" is already registered`);
  }
}

function isServiceError(e: unknown): e is ServiceError {
  return typeof e === "object" && e !== null && typeof (e as { code?: unknown }).code === "number";
}

function statusMessage(e: ServiceError): string {
  return e.details || e.message;
}

/** Maps a raw gRPC status onto the general {@link ClientError} hierarchy. */
export function clientErrorFromStatus(e: ServiceError): ClientError {
  const msg = statusMessage(e);
  switch (e.code) {
    case GrpcStatus.UNAVAILABLE:
    case GrpcStatus.DEADLINE_EXCEEDED:
    case GrpcStatus.ABORTED:
    case GrpcStatus.CANCELLED:
      return new TransportError(msg);
    case GrpcStatus.UNAUTHENTICATED:
    case GrpcStatus.PERMISSION_DENIED:
      return new UnauthenticatedError(msg);
    case GrpcStatus.RESOURCE_EXHAUSTED:
      return new OverloadedError(msg);
    case GrpcStatus.INVALID_ARGUMENT:
      return new InvalidRequestError(msg);
    case GrpcStatus.INTERNAL:
    case GrpcStatus.DATA_LOSS:
    case GrpcStatus.UNKNOWN:
      return new ServerInternalError(msg);
    default:
      return new UnexpectedStatusError(e.code, msg);
  }
}

/** Whether a gRPC status code is worth retrying (matches the Rust `is_transient`). */
export function isTransientStatus(code: number): boolean {
  return (
    code === GrpcStatus.UNAVAILABLE ||
    code === GrpcStatus.DEADLINE_EXCEEDED ||
    code === GrpcStatus.ABORTED ||
    code === GrpcStatus.RESOURCE_EXHAUSTED
  );
}

/**
 * Whether a gRPC status code is safe to retry even when a retry could repeat
 * committed work. UNAVAILABLE and RESOURCE_EXHAUSTED mean the request was
 * rejected or never ran; DEADLINE_EXCEEDED and ABORTED are ambiguous — the
 * server may have committed with only the response lost — so they are excluded.
 * Used to gate enqueue retries when a batch has jobs without idempotency keys.
 */
export function isSafeTransientStatus(code: number): boolean {
  return code === GrpcStatus.UNAVAILABLE || code === GrpcStatus.RESOURCE_EXHAUSTED;
}

/** Normalizes any thrown value into a {@link ClientError}. */
export function asClientError(e: unknown): ClientError {
  if (e instanceof ClientError) return e;
  if (isServiceError(e)) return clientErrorFromStatus(e);
  return new TransportError(e instanceof Error ? e.message : String(e));
}

/** Normalizes any thrown value into a {@link LeaseError} or {@link ClientError}. */
export function asLeaseError(e: unknown): LeaseError | ClientError {
  if (e instanceof LeaseError || e instanceof ClientError) return e;
  if (isServiceError(e)) {
    if (e.code === GrpcStatus.NOT_FOUND) return new JobNotFoundError();
    if (e.code === GrpcStatus.FAILED_PRECONDITION) return new AttemptMismatchError();
    return clientErrorFromStatus(e);
  }
  return new TransportError(e instanceof Error ? e.message : String(e));
}

/** Normalizes any thrown value into a {@link ReserveError} or {@link ClientError}. */
export function asReserveError(e: unknown): ReserveError | ClientError {
  if (e instanceof ReserveError || e instanceof ClientError) return e;
  if (isServiceError(e)) {
    if (e.code === GrpcStatus.FAILED_PRECONDITION) return new UnknownQueuesError(statusMessage(e));
    return clientErrorFromStatus(e);
  }
  return new TransportError(e instanceof Error ? e.message : String(e));
}
