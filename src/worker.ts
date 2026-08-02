/**
 * A high-level worker that runs the reserve → process → ack/nack loop.
 *
 * {@link Worker} wraps a {@link SeppClient} and drives job consumption: reserve
 * jobs, dispatch each to the handler registered for its `job_type`, then ack on
 * success or nack on failure. It adds bounded concurrency, optional lease
 * auto-extension, graceful shutdown via a {@link ShutdownHandle}, and (when
 * `@opentelemetry/api` is present) a linked CONSUMER span per job.
 */

import { randomUUID } from "node:crypto";
import { hostname } from "node:os";

import type { SeppClient } from "./client";
import {
  AttemptMismatchError,
  DuplicateHandlerError,
  JobNotFoundError,
  ValidationError,
} from "./errors";
import { createLogger, type Logger, noopLogger } from "./logger";
import { getOtel, loadOtel, spanContextFromTraceparent } from "./trace";
import { type Job, type JobCtx, type Payload, type ReserveOptions, RetryDirective } from "./types";

/** A job handler: resolves to ack the job, throws {@link HandlerError} (or anything) to nack it. */
export type Handler = (payload: Payload | undefined, ctx: JobCtx) => Promise<void>;

/** An empty reserve returning sooner than this is treated as an early empty. */
const EARLY_EMPTY_THRESHOLD_MS = 100;

/** Pause after an early-empty reserve, so a server answering empty immediately is not hammered. */
const EARLY_EMPTY_BACKOFF_MS = 250;

/** Nack reasons are truncated to this many characters (server-side value size limits). */
const NACK_REASON_MAX_CHARS = 512;

/**
 * Thrown from a handler to nack its job, choosing how the server should retry.
 * Use the static constructors rather than `new`.
 */
export class HandlerError extends Error {
  private constructor(
    readonly directive: RetryDirective,
    readonly reason: string,
  ) {
    super(reason);
    this.name = "HandlerError";
  }

  /** Nack for retry under the queue's default policy. */
  static retry(reason: string): HandlerError {
    return new HandlerError(RetryDirective.default(), reason);
  }

  /** Nack for retry after at least `delayMs`. */
  static retryAfter(reason: string, delayMs: number): HandlerError {
    return new HandlerError(RetryDirective.after(delayMs), reason);
  }

  /** Nack as a permanent failure, dead-lettering the job immediately. */
  static permanent(reason: string): HandlerError {
    return new HandlerError(RetryDirective.deadLetter(), reason);
  }
}

/** Configuration for a {@link Worker}. */
export interface WorkerOptions {
  /** Queues to reserve from, polled in order. */
  queues: string[];
  /** Lease duration for reserved jobs, in milliseconds. */
  leaseDuration: number;
  /** Long-poll wait timeout per reserve, in ms (must be positive). Default 30_000. */
  waitTimeout?: number;
  /** Cap on jobs returned per reserve (the worker also limits to free capacity). */
  maxJobs?: number;
  /** Maximum jobs processed concurrently. Default 16. */
  maxInFlight?: number;
  /** Wait after a failed reserve before retrying, in ms (must be positive). Default 1000. */
  reserveErrorBackoff?: number;
  /**
   * Extend leases automatically while a handler runs (heartbeat = lease/3 by
   * default). Use with caution: a handler that hangs indefinitely keeps its
   * lease alive forever, so its job is never redelivered. Bound hang-prone
   * work yourself, e.g. with `AbortSignal.timeout`.
   */
  autoExtend?: boolean;
  /** Explicit auto-extend heartbeat interval, in ms (implies `autoExtend`). */
  autoExtendInterval?: number;
  /** Stable worker id. Defaults to `<hostname>-<pid>-<rand>`. */
  workerId?: string;
  /** Logger for worker diagnostics. Defaults to silent (no-op). */
  logger?: Logger;
}

/** A handle for triggering a {@link Worker}'s graceful shutdown. */
export class ShutdownHandle {
  readonly #ac = new AbortController();

  /**
   * Signals the worker to stop reserving new jobs and begin draining. An
   * in-flight reserve long-poll is cancelled; if it completes with jobs at the
   * shutdown boundary anyway, those jobs are still processed (and acked or
   * nacked) before {@link Worker.run} returns.
   */
  shutdown(): void {
    this.#ac.abort();
  }

  /** Whether shutdown has been signalled. */
  get isShutdown(): boolean {
    return this.#ac.signal.aborted;
  }

  /** @internal */ get signal(): AbortSignal {
    return this.#ac.signal;
  }
}

type Disposition =
  | { kind: "ok" }
  | { kind: "handlerError"; error: HandlerError }
  | { kind: "thrown"; error: unknown };

/**
 * A high-level worker that runs the reserve → process → ack/nack loop over a
 * {@link SeppClient}, with bounded concurrency, optional lease auto-extension,
 * and graceful shutdown via a {@link ShutdownHandle}.
 */
export class Worker {
  readonly #client: SeppClient;
  readonly #queues: string[];
  readonly #leaseDuration: number;
  readonly #waitTimeout: number | undefined;
  readonly #maxJobs: number | undefined;
  readonly #maxInFlight: number;
  readonly #reserveErrorBackoff: number;
  readonly #workerId: string;
  readonly #autoExtend: boolean;
  readonly #autoExtendInterval: number | undefined;
  readonly #handlers = new Map<string, Handler>();
  #catchAll: Handler | undefined;
  readonly #shutdown = new ShutdownHandle();
  readonly #logger: Logger;

  constructor(client: SeppClient, opts: WorkerOptions) {
    if (opts.queues.length === 0) throw new ValidationError("at least one queue must be specified");
    opts.queues.forEach((q, i) => {
      if (q.length === 0) throw new ValidationError(`queue name at index ${i} must not be empty`);
    });
    if (opts.leaseDuration <= 0) throw new ValidationError("leaseDuration must be at least 1ms");
    if (opts.waitTimeout !== undefined && opts.waitTimeout <= 0)
      throw new ValidationError("waitTimeout must be at least 1ms");
    if (opts.reserveErrorBackoff !== undefined && opts.reserveErrorBackoff <= 0)
      throw new ValidationError("reserveErrorBackoff must be at least 1ms");
    if (opts.maxJobs !== undefined && opts.maxJobs < 1)
      throw new ValidationError("maxJobs must be at least 1");
    if (opts.workerId !== undefined && opts.workerId.length === 0) {
      throw new ValidationError("workerId must not be empty when set");
    }

    this.#client = client;
    this.#queues = [...opts.queues];
    this.#leaseDuration = opts.leaseDuration;
    this.#waitTimeout = opts.waitTimeout;
    this.#maxJobs = opts.maxJobs;
    this.#maxInFlight = Math.max(1, opts.maxInFlight ?? 16);
    this.#reserveErrorBackoff = opts.reserveErrorBackoff ?? 1000;
    this.#workerId = opts.workerId ?? defaultWorkerId();
    this.#logger = createLogger(opts.logger ?? noopLogger());
    this.#autoExtend = opts.autoExtend === true || opts.autoExtendInterval !== undefined;
    this.#autoExtendInterval =
      opts.autoExtendInterval !== undefined ? Math.max(1, opts.autoExtendInterval) : undefined;
  }

  /** Registers the handler for a `jobType`. Throws if one is already registered. */
  handle(jobType: string, handler: Handler): this {
    if (this.#handlers.has(jobType)) {
      throw new DuplicateHandlerError(jobType);
    }
    this.#handlers.set(jobType, handler);
    return this;
  }

  /** Registers a handler, overwriting any existing one for the same `jobType`. */
  replaceHandler(jobType: string, handler: Handler): this {
    this.#handlers.set(jobType, handler);
    return this;
  }

  /** Unregisters the handler for a `jobType`, if any. */
  removeHandler(jobType: string): this {
    this.#handlers.delete(jobType);
    return this;
  }

  /** Registers a catch-all handler for job types without a specific handler. */
  catchAll(handler: Handler): this {
    this.#catchAll = handler;
    return this;
  }

  /** Returns the {@link ShutdownHandle} for stopping this worker. */
  shutdownHandle(): ShutdownHandle {
    return this.#shutdown;
  }

  /**
   * Runs the reserve → process → ack/nack loop until shutdown is signalled and
   * all in-flight jobs have drained. Reserve errors are logged and retried; they
   * do not stop the loop.
   *
   * The drain wait is unbounded: a handler that never settles blocks the return
   * of `run()` indefinitely (and with `autoExtend` its lease is kept alive the
   * whole time). If a handler's work can hang, bound it yourself, e.g. with
   * `AbortSignal.timeout`.
   */
  async run(): Promise<void> {
    await loadOtel();
    const sem = new Semaphore(this.#maxInFlight);
    const inFlight = new Set<Promise<void>>();

    const spawn = (job: Job): void => {
      const p = this.#processJob(job)
        .catch((err) =>
          this.#logger.error("unexpected error processing job", { error: String(err) }),
        )
        .finally(() => {
          sem.release();
          inFlight.delete(p);
        });
      inFlight.add(p);
    };

    while (!this.#shutdown.isShutdown) {
      await sem.acquire();
      if (this.#shutdown.isShutdown) {
        sem.release();
        break;
      }

      const reserveOpts: ReserveOptions = {
        queues: this.#queues,
        leaseDuration: this.#leaseDuration,
        waitTimeout: this.#waitTimeout,
        workerId: this.#workerId,
        signal: this.#shutdown.signal, // cancels the long-poll when shutdown fires
      };
      const capacity = 1 + sem.available;
      reserveOpts.maxJobs =
        this.#maxJobs !== undefined ? Math.min(this.#maxJobs, capacity) : capacity;

      let jobs: Job[] | null;
      const reserveStart = Date.now();
      try {
        jobs = await this.#client.reserve(reserveOpts);
      } catch (err) {
        sem.release();
        // A failure after shutdown was signalled is expected (our own
        // cancellation of the long-poll) — exit the loop cleanly.
        if (this.#shutdown.isShutdown) break;
        this.#logger.warn("reserve error; backing off", { error: String(err) });
        await sleepUntil(this.#reserveErrorBackoff, this.#shutdown.signal);
        continue;
      }

      if (!jobs) {
        sem.release();
        // An empty reserve that answered well before the wait window elapsed
        // (e.g. a server draining at shutdown) would otherwise be hammered;
        // normal long-poll empties re-poll immediately. Capped at half the
        // wait window so a deliberately tiny waitTimeout is not misread as
        // early.
        const earlyMs = Math.min(EARLY_EMPTY_THRESHOLD_MS, (this.#waitTimeout ?? 30_000) / 2);
        if (Date.now() - reserveStart < earlyMs) {
          await sleepUntil(EARLY_EMPTY_BACKOFF_MS, this.#shutdown.signal);
        }
        continue;
      }

      const [first, ...rest] = jobs;
      spawn(first as Job); // consumes the permit acquired at the top of the loop
      for (const job of rest) {
        await sem.acquire();
        spawn(job);
      }
    }

    await Promise.allSettled(Array.from(inFlight));
  }

  async #processJob(job: Job): Promise<void> {
    const { payload, ctx } = job;
    const core = () => this.#process(payload, ctx);

    const api = getOtel();
    if (!api) return core();

    const links = [];
    if (ctx.traceContext) {
      const sc = spanContextFromTraceparent(ctx.traceContext.traceparent);
      if (sc) links.push({ context: sc });
    }
    const span = api.trace.getTracer("sepp-js").startSpan("sepp.process", {
      kind: api.SpanKind.CONSUMER,
      links,
      attributes: {
        "sepp.job_id": ctx.id,
        "sepp.job_type": ctx.jobType,
        "sepp.attempt": ctx.attempt,
      },
    });
    try {
      await api.context.with(api.trace.setSpan(api.context.active(), span), core);
    } finally {
      span.end();
    }
  }

  async #process(payload: Payload | undefined, ctx: JobCtx): Promise<void> {
    const handler = this.#handlers.get(ctx.jobType) ?? this.#catchAll;
    if (!handler) {
      this.#logger.warn("no handler registered", { jobType: ctx.jobType });
      try {
        await this.#client.nack(ctx, {
          reason: "no handler registered for job_type",
        });
      } catch {
        // best-effort; the job is redelivered after lease expiry
      }
      return;
    }

    ctx._bindAbort(new AbortController());

    let disposition: Disposition;
    if (this.#autoExtend) {
      const hbStop = new AbortController();
      const heartbeat = this.#heartbeat(ctx, hbStop.signal);
      try {
        disposition = await this.#runHandler(handler, payload, ctx);
      } finally {
        hbStop.abort();
        await heartbeat.catch(() => {});
      }
      if (ctx._leaseLost) {
        this.#logger.error("lease lost; skipping ack/nack (job will be redelivered)");
        return;
      }
    } else {
      disposition = await this.#runHandler(handler, payload, ctx);
    }

    try {
      await this.#dispose(ctx, disposition);
    } catch (err) {
      if (err instanceof JobNotFoundError) {
        this.#logger.error(
          "ack/nack got JobNotFound; either the lease was lost (job will be redelivered) or a retried ack/nack already succeeded and only its response was lost",
          { error: String(err) },
        );
      } else {
        this.#logger.error("failed to ack/nack job; it will be redelivered after lease expiry", {
          error: String(err),
        });
      }
    }
  }

  async #runHandler(
    handler: Handler,
    payload: Payload | undefined,
    ctx: JobCtx,
  ): Promise<Disposition> {
    try {
      await handler(payload, ctx);
      return { kind: "ok" };
    } catch (e) {
      if (e instanceof HandlerError) return { kind: "handlerError", error: e };
      return { kind: "thrown", error: e };
    }
  }

  async #dispose(ctx: JobCtx, d: Disposition): Promise<void> {
    if (d.kind === "ok") {
      await this.#client.ack(ctx);
      return;
    }
    if (d.kind === "handlerError") {
      await this.#client.nack(ctx, {
        retry: d.error.directive,
        reason: d.error.reason,
      });
      return;
    }
    // A thrown non-HandlerError is the analog of the Rust handler panic.
    this.#logger.error("handler threw; nacking", { error: String(d.error) });
    // Keep the root cause in the nack reason: it ends up in the server-side
    // dead-letter record's last_reason.
    const reason = `handler threw: ${String(d.error)}`.slice(0, NACK_REASON_MAX_CHARS);
    await this.#client.nack(ctx, { reason });
  }

  async #heartbeat(ctx: JobCtx, hbStop: AbortSignal): Promise<void> {
    while (!hbStop.aborted) {
      // The default interval tracks the GRANTED lease (_knownExpiryMs, which each
      // extend updates to the server-clamped value), not the requested
      // leaseDuration: a clamped lease must be refreshed before it lapses, not
      // after a third of the larger requested lease.
      const interval =
        this.#autoExtendInterval ?? Math.max(1, Math.floor((ctx._knownExpiryMs - Date.now()) / 3));
      await sleepUntil(interval, hbStop);
      if (hbStop.aborted) return;
      try {
        await ctx.extend(this.#leaseDuration);
      } catch (err) {
        if (err instanceof AttemptMismatchError || err instanceof JobNotFoundError) {
          this.#logger.error(
            "lease reassigned by server; aborting handler to avoid double processing",
          );
          ctx._markLeaseLost();
          return;
        }
        if (Date.now() >= ctx._knownExpiryMs) {
          this.#logger.error("lease lost; aborting handler to avoid double processing");
          ctx._markLeaseLost();
          return;
        }
        this.#logger.warn("lease extend failed; lease still valid, will retry", {
          error: String(err),
        });
      }
    }
  }
}

/** A simple async counting semaphore with direct hand-off on release. */
class Semaphore {
  #permits: number;
  readonly #waiters: Array<() => void> = [];

  constructor(permits: number) {
    this.#permits = permits;
  }

  get available(): number {
    return this.#permits;
  }

  acquire(): Promise<void> {
    if (this.#permits > 0) {
      this.#permits -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.#waiters.push(resolve));
  }

  release(): void {
    const next = this.#waiters.shift();
    if (next)
      next(); // hand the permit straight to the next waiter
    else this.#permits += 1;
  }
}

/** Resolves after `ms`, or early when `signal` aborts. */
function sleepUntil(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const onAbort = () => done();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      resolve();
    }
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function defaultWorkerId(): string {
  const host = hostname() || "unknown";
  const rand = randomUUID().replace(/-/g, "").slice(0, 8);
  return `${host}-${process.pid}-${rand}`;
}
