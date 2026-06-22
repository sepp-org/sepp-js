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
import { getOtel, loadOtel, spanContextFromTraceparent } from "./trace";
import { type Job, type JobCtx, type Payload, type ReserveOptions, RetryDirective } from "./types";

/** A job handler: resolves to ack the job, throws {@link HandlerError} (or anything) to nack it. */
export type Handler = (payload: Payload | undefined, ctx: JobCtx) => Promise<void>;

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
  /** Long-poll wait timeout per reserve, in ms. Default 30_000. */
  waitTimeout?: number;
  /** Cap on jobs returned per reserve (the worker also limits to free capacity). */
  maxJobs?: number;
  /** Maximum jobs processed concurrently. Default 16. */
  maxInFlight?: number;
  /** Wait after a failed reserve before retrying, in ms. Default 1000. */
  reserveErrorBackoff?: number;
  /** Extend leases automatically while a handler runs (heartbeat = lease/3 by default). */
  autoExtend?: boolean;
  /** Explicit auto-extend heartbeat interval, in ms (implies `autoExtend`). */
  autoExtendInterval?: number;
  /** Stable worker id. Defaults to `<hostname>-<pid>-<rand>`. */
  workerId?: string;
}

/** A handle for triggering a {@link Worker}'s graceful shutdown. */
export class ShutdownHandle {
  readonly #ac = new AbortController();

  /** Signals the worker to stop reserving new jobs and begin draining. */
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

  constructor(client: SeppClient, opts: WorkerOptions) {
    if (opts.queues.length === 0) throw new ValidationError("at least one queue must be specified");
    opts.queues.forEach((q, i) => {
      if (q.length === 0) throw new ValidationError(`queue name at index ${i} must not be empty`);
    });
    if (opts.leaseDuration <= 0) throw new ValidationError("leaseDuration must be at least 1ms");
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
   */
  async run(): Promise<void> {
    await loadOtel();
    const sem = new Semaphore(this.#maxInFlight);
    const inFlight = new Set<Promise<void>>();

    const spawn = (job: Job): void => {
      const p = this.#processJob(job)
        // eslint-disable-next-line no-console
        .catch((err) => console.error("sepp worker: unexpected error processing job:", err))
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
      };
      const capacity = 1 + sem.available;
      reserveOpts.maxJobs =
        this.#maxJobs !== undefined ? Math.min(this.#maxJobs, capacity) : capacity;

      let jobs: Job[] | null;
      try {
        jobs = await this.#client.reserve(reserveOpts);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn("sepp worker: reserve error; backing off:", err);
        sem.release();
        if (this.#shutdown.isShutdown) break;
        await sleepUntil(this.#reserveErrorBackoff, this.#shutdown.signal);
        continue;
      }

      if (!jobs) {
        sem.release(); // wait window elapsed — re-poll immediately
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
      // eslint-disable-next-line no-console
      console.warn(`sepp worker: no handler registered for job_type '${ctx.jobType}'`);
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
        // eslint-disable-next-line no-console
        console.error("sepp worker: lease lost; skipping ack/nack (job will be redelivered)");
        return;
      }
    } else {
      disposition = await this.#runHandler(handler, payload, ctx);
    }

    try {
      await this.#dispose(ctx, disposition);
    } catch (err) {
      if (err instanceof JobNotFoundError) {
        // After retries, JobNotFound is ambiguous: an earlier attempt of this
        // same call may have succeeded with only the response lost.
        // eslint-disable-next-line no-console
        console.error(
          "sepp worker: ack/nack got JobNotFound; either the lease was lost (job will be redelivered) or a retried ack/nack already succeeded and only its response was lost:",
          err,
        );
      } else {
        // eslint-disable-next-line no-console
        console.error(
          "sepp worker: failed to ack/nack job; it will be redelivered after lease expiry:",
          err,
        );
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
    // eslint-disable-next-line no-console
    console.error("sepp worker: handler threw; nacking:", d.error);
    await this.#client.nack(ctx, { reason: "handler threw an error" });
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
          // eslint-disable-next-line no-console
          console.error(
            "sepp worker: lease reassigned by server; aborting handler to avoid double processing",
          );
          ctx._markLeaseLost();
          return;
        }
        if (Date.now() >= ctx._knownExpiryMs) {
          // eslint-disable-next-line no-console
          console.error("sepp worker: lease lost; aborting handler to avoid double processing");
          ctx._markLeaseLost();
          return;
        }
        // eslint-disable-next-line no-console
        console.warn("sepp worker: lease extend failed; lease still valid, will retry:", err);
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
