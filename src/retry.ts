/**
 * Backoff policy for retrying *transient* RPC failures (those that map to
 * {@link TransportError} / {@link OverloadedError}).
 *
 * Applies to enqueue, ack, nack, extend, and get-server-info — but not to
 * {@link SeppClient.reserve} (a long poll) or {@link SeppClient.drainDeadLetters}
 * (destructive). The default policy performs **no** retries (`maxAttempts === 1`);
 * opt in by constructing one and passing it to `connect`.
 */
import { isTransientStatus } from "./errors";

export interface RetryPolicyOptions {
  /** Total attempts including the first. Values below 1 are clamped to 1. Default 1 (no retries). */
  maxAttempts?: number;
  /** Backoff before the first retry, in milliseconds. Default 100. */
  initialBackoff?: number;
  /** Cap on the backoff between retries, in milliseconds. Default 10_000. */
  maxBackoff?: number;
  /** Exponential growth factor. Values below 1.0 are clamped to 1.0. Default 2.0. */
  multiplier?: number;
  /** Randomize each delay within `[0.5, 1.0)` of its computed value. Default true. */
  jitter?: boolean;
}

/**
 * Backoff policy for retrying transient RPC failures. The default policy
 * performs no retries; opt in by constructing one and passing it to
 * {@link SeppClient.connect}. A retried enqueue can duplicate jobs that carry
 * no idempotency key, so enqueue retries skip the ambiguous-commit codes
 * (DEADLINE_EXCEEDED, ABORTED) unless every job in the request is keyed.
 */
export class RetryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoff: number;
  readonly maxBackoff: number;
  readonly multiplier: number;
  readonly jitter: boolean;

  constructor(opts: RetryPolicyOptions = {}) {
    this.maxAttempts = Math.max(1, Math.trunc(opts.maxAttempts ?? 1));
    this.initialBackoff = opts.initialBackoff ?? 100;
    this.maxBackoff = opts.maxBackoff ?? 10_000;
    this.multiplier = Math.max(1, opts.multiplier ?? 2);
    this.jitter = opts.jitter ?? true;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Equal-jitter factor in `[0.5, 1.0)`. */
function jitterFactor(): number {
  return 0.5 + 0.5 * Math.random();
}

function statusCode(e: unknown): number | undefined {
  if (typeof e === "object" && e !== null && typeof (e as { code?: unknown }).code === "number") {
    return (e as { code: number }).code;
  }
  return undefined;
}

/**
 * Runs `fn` until it succeeds, runs out of attempts, or hits a non-retryable
 * error, sleeping between attempts per `policy`. Rethrows the last raw error so
 * the caller can map it to a typed error. `isRetryable` decides which status
 * codes are retried (default: every transient code).
 */
export async function withRetry<T>(
  policy: RetryPolicy,
  fn: () => Promise<T>,
  isRetryable: (code: number) => boolean = isTransientStatus,
): Promise<T> {
  let attempt = 1;
  let backoff = policy.initialBackoff;
  for (;;) {
    try {
      return await fn();
    } catch (e) {
      const code = statusCode(e);
      const transient = code !== undefined && isRetryable(code);
      if (attempt >= policy.maxAttempts || !transient) throw e;

      const delay = policy.jitter ? backoff * jitterFactor() : backoff;
      await sleep(delay);
      backoff = Math.min(backoff * policy.multiplier, policy.maxBackoff);
      attempt += 1;
    }
  }
}
