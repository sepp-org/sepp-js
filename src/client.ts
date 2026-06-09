/**
 * The gRPC client: connecting, enqueuing, reserving, and lease management.
 *
 * {@link SeppClient} is a handle to a sepp server. Build one with
 * {@link SeppClient.connect} for the common case, passing options for API-key
 * auth, TLS, and a {@link RetryPolicy}. Every method returns a promise and the
 * client may be shared freely across concurrent work.
 */
import { credentials as grpcCredentials, Metadata } from "@grpc/grpc-js";
import type { CallOptions, ChannelCredentials, ChannelOptions, ServiceError } from "@grpc/grpc-js";

import {
  ackFromPb,
  deadLetterRecordFromPb,
  enqueueRequestToPb,
  jobFromPb,
  jobRejectionFromPb,
  jobValidationErrorFromPb,
  msToDuration,
  reserveOptionsToPb,
  retryDirectiveToPb,
  serverInfoFromPb,
} from "./convert";
import {
  asClientError,
  asLeaseError,
  asReserveError,
  BatchResultCountMismatchError,
  BatchValidationError,
  ConnectError,
  EmptyBatchError,
  InvalidApiKeyError,
  JobRejectedError,
  MalformedResponseError,
} from "./errors";
import {
  type DeadLetterRecord as PbDeadLetterRecord,
  type EnqueueAtomicResponse,
  type EnqueueBatchResponse,
  type ExtendResponse,
  type GetServerInfoResponse,
  QueueServiceClient,
  type ReserveResponse,
} from "./gen/sepp/v1/queue";
import { RetryPolicy, withRetry } from "./retry";
import { currentTraceContext, injectJobTrace, loadOtel } from "./trace";
import {
  type DeadLetterRecord,
  type EnqueueAck,
  type EnqueueRequest,
  isJobRejection,
  type Job,
  type JobRejection,
  RetryDirective,
  type ReserveOptions,
  type ServerInfo,
} from "./types";
import { USER_AGENT } from "./version";

/** Slack added to the reserve call deadline beyond the long-poll wait timeout. */
const RESERVE_DEADLINE_SLACK_MS = 10_000;

/** Default deadline for unary RPCs other than reserve. */
const DEFAULT_RPC_TIMEOUT_MS = 30_000;

/** Options for {@link SeppClient.connect}. */
export interface ConnectOptions {
  /** API key sent as `Authorization: Bearer <key>` on every request. */
  apiKey?: string;
  /** Backoff policy for transient RPC failures. Defaults to no retries. */
  retryPolicy?: RetryPolicy;
  /** Enable TLS with the platform's default root certificates. */
  tls?: boolean;
  /** Enable TLS trusting this PEM-encoded CA certificate (for private/self-signed servers). */
  tlsCaCert?: Buffer | string;
  /** Override the server name verified against the certificate. */
  tlsDomain?: string;
  /** Fully custom channel credentials, taking precedence over the `tls*` options. */
  credentials?: ChannelCredentials;
  /** How long to wait for the connection to become ready, in ms. Default 5000. */
  connectTimeout?: number;
  /**
   * Deadline for every unary RPC except reserve, in ms. Default 30_000.
   * Reserve derives its deadline from its `waitTimeout` instead. Very large
   * enqueue batches may need a higher value.
   */
  rpcTimeout?: number;
  /**
   * Largest gRPC message the client accepts, in bytes. Unset keeps the grpc-js
   * default of 4 MiB. Workers should raise this when a full reserve batch can
   * exceed it: a reserve response can carry up to the server's
   * `maxReserveBatch * maxPayloadBytes`, and an oversized response fails
   * client-side after the jobs were already leased, stranding them until
   * their leases expire.
   */
  maxReceiveMessageBytes?: number;
  /** Extra gRPC channel options, merged over the defaults. */
  channelOptions?: ChannelOptions;
}

/** Options for {@link SeppClient.nack}. */
export interface NackOptions {
  /** What the server should do next. Defaults to the queue's retry policy. */
  retry?: RetryDirective;
  /** Human-readable reason, recorded for debugging and metrics. */
  reason?: string;
}

/** Options for {@link SeppClient.drainDeadLetters}. */
export interface DrainOptions {
  /** Restrict the drain to a single queue. If unset, drains across all queues. */
  queue?: string;
  /** Maximum records to return (default 1). The server clamps to its max. */
  max?: number;
}

function parseAddress(addr: string): { target: string; impliesTls: boolean } {
  if (addr.startsWith("http://")) return { target: addr.slice("http://".length), impliesTls: false };
  if (addr.startsWith("https://")) return { target: addr.slice("https://".length), impliesTls: true };
  return { target: addr, impliesTls: false };
}

function isValidHeaderValue(key: string): boolean {
  // Printable ASCII, no CR/LF — matches what tonic/grpc accept as a header value.
  return key.length > 0 && /^[\x20-\x7e]+$/.test(key);
}

export class SeppClient {
  readonly #client: QueueServiceClient;
  readonly #bearer: string | undefined;
  readonly #retry: RetryPolicy;
  readonly #rpcTimeout: number;

  private constructor(
    client: QueueServiceClient,
    bearer: string | undefined,
    retry: RetryPolicy,
    rpcTimeout: number,
  ) {
    this.#client = client;
    this.#bearer = bearer;
    this.#retry = retry;
    this.#rpcTimeout = rpcTimeout;
  }

  /**
   * Connects to a sepp server.
   *
   * `address` may be `host:port`, `http://host:port` (plaintext), or
   * `https://host:port` (implies TLS). Resolves once the connection is ready or
   * rejects with {@link ConnectError} after `connectTimeout`.
   */
  static async connect(address: string, opts: ConnectOptions = {}): Promise<SeppClient> {
    const { target, impliesTls } = parseAddress(address);

    let bearer: string | undefined;
    if (opts.apiKey !== undefined) {
      if (!isValidHeaderValue(opts.apiKey)) throw new InvalidApiKeyError();
      bearer = `Bearer ${opts.apiKey}`;
    }

    const useTls =
      opts.credentials !== undefined ||
      (opts.tls ?? impliesTls) ||
      opts.tlsCaCert !== undefined ||
      opts.tlsDomain !== undefined;

    if (bearer && !useTls) {
      // eslint-disable-next-line no-console
      console.warn(
        "sepp: API key configured without TLS; it will be sent over the connection in plaintext",
      );
    }

    const creds = buildCredentials(opts, impliesTls);
    const channelOptions: ChannelOptions = {
      "grpc.keepalive_time_ms": 30_000,
      "grpc.keepalive_timeout_ms": 10_000,
      "grpc.keepalive_permit_without_calls": 1,
      "grpc.primary_user_agent": USER_AGENT,
      ...(opts.tlsDomain ? { "grpc.ssl_target_name_override": opts.tlsDomain } : {}),
      ...(opts.maxReceiveMessageBytes !== undefined
        ? { "grpc.max_receive_message_length": opts.maxReceiveMessageBytes }
        : {}),
      ...opts.channelOptions,
    };

    // Best-effort: makes the optional OTel API available for trace propagation.
    await loadOtel();

    const client = new QueueServiceClient(target, creds, channelOptions);
    const deadline = Date.now() + (opts.connectTimeout ?? 5000);
    await new Promise<void>((resolve, reject) => {
      client.waitForReady(deadline, (err) => {
        if (err) reject(new ConnectError(address, err.message));
        else resolve();
      });
    });

    return new SeppClient(
      client,
      bearer,
      opts.retryPolicy ?? new RetryPolicy(),
      opts.rpcTimeout ?? DEFAULT_RPC_TIMEOUT_MS,
    );
  }

  /**
   * Wraps an already-constructed generated {@link QueueServiceClient}, for
   * sharing a channel or applying custom transport configuration the connect
   * options do not expose. No API key is attached.
   */
  static fromClient(
    client: QueueServiceClient,
    opts: { retryPolicy?: RetryPolicy; rpcTimeout?: number } = {},
  ): SeppClient {
    return new SeppClient(
      client,
      undefined,
      opts.retryPolicy ?? new RetryPolicy(),
      opts.rpcTimeout ?? DEFAULT_RPC_TIMEOUT_MS,
    );
  }

  /** Closes the underlying channel. */
  close(): void {
    this.#client.close();
  }

  // -------------------------------------------------------------------------
  // Producer
  // -------------------------------------------------------------------------

  /**
   * Enqueues a batch of jobs on a best-effort basis. The returned array has one
   * entry per submitted job, in order, each either an {@link EnqueueAck} or a
   * {@link JobRejection} — narrow with {@link isJobRejection}. Whole-call
   * failures throw a {@link ClientError}.
   */
  async enqueueBatch(jobs: Iterable<EnqueueRequest>): Promise<Array<EnqueueAck | JobRejection>> {
    const pbJobs = Array.from(jobs, enqueueRequestToPb);
    if (pbJobs.length === 0) throw new EmptyBatchError();
    injectJobTrace(pbJobs);

    let response: EnqueueBatchResponse;
    try {
      response = await withRetry(this.#retry, () =>
        this.#unary<EnqueueBatchResponse>((md, opts, cb) =>
          this.#client.enqueueBatch({ jobs: pbJobs }, md, opts, cb),
        ),
      );
    } catch (e) {
      throw asClientError(e);
    }

    if (response.results.length !== pbJobs.length) {
      throw new BatchResultCountMismatchError(pbJobs.length, response.results.length);
    }

    return response.results.map((r) => {
      if (r.success) return ackFromPb(r.success);
      if (r.rejection) return jobRejectionFromPb(r.rejection);
      throw new MalformedResponseError("missing outcome in job result");
    });
  }

  /**
   * Enqueues a single job. A convenience wrapper over {@link enqueueBatch} that
   * throws {@link JobRejectedError} if the server rejects this specific job.
   */
  async enqueue(job: EnqueueRequest): Promise<EnqueueAck> {
    const [result] = await this.enqueueBatch([job]);
    if (!result) throw new MalformedResponseError("empty results for single-job batch");
    if (isJobRejection(result)) throw new JobRejectedError(result);
    return result;
  }

  /**
   * Enqueues a batch atomically: either all jobs are accepted or none are. On
   * success returns one {@link EnqueueAck} per job, in order; if any job fails
   * validation, throws {@link BatchValidationError} with every failure.
   */
  async enqueueAtomic(jobs: Iterable<EnqueueRequest>): Promise<EnqueueAck[]> {
    const pbJobs = Array.from(jobs, enqueueRequestToPb);
    if (pbJobs.length === 0) throw new EmptyBatchError();
    injectJobTrace(pbJobs);

    let response: EnqueueAtomicResponse;
    try {
      response = await withRetry(this.#retry, () =>
        this.#unary<EnqueueAtomicResponse>((md, opts, cb) =>
          this.#client.enqueueAtomic({ jobs: pbJobs }, md, opts, cb),
        ),
      );
    } catch (e) {
      throw asClientError(e);
    }

    if (response.success) {
      if (response.success.responses.length !== pbJobs.length) {
        throw new BatchResultCountMismatchError(pbJobs.length, response.success.responses.length);
      }
      return response.success.responses.map(ackFromPb);
    }
    if (response.rejection) {
      throw new BatchValidationError(response.rejection.errors.map(jobValidationErrorFromPb));
    }
    throw new MalformedResponseError("missing outcome in EnqueueAtomicResponse");
  }

  // -------------------------------------------------------------------------
  // Consumer
  // -------------------------------------------------------------------------

  /**
   * Long-polls for jobs to process. Resolves with one or more leased jobs, or
   * `null` if the wait elapsed with nothing available (poll again). Each job
   * must be acked, nacked, or extended before its lease expires. Unlike the
   * other RPCs, reserve is not retried; a malformed job is logged and skipped.
   */
  async reserve(opts: ReserveOptions): Promise<Job[] | null> {
    const req = reserveOptionsToPb(opts);
    const deadline = Date.now() + (opts.waitTimeout ?? 30_000) + RESERVE_DEADLINE_SLACK_MS;

    let response: ReserveResponse;
    try {
      response = await this.#unary<ReserveResponse>(
        (md, callOpts, cb) => this.#client.reserve(req, md, callOpts, cb),
        { deadline },
      );
    } catch (e) {
      throw asReserveError(e);
    }

    const jobs: Job[] = [];
    for (const pbJob of response.jobs) {
      try {
        jobs.push(jobFromPb(this.#extendInner, pbJob, opts.workerId));
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn("sepp: skipping malformed job in reserve response:", err);
      }
    }
    return jobs.length > 0 ? jobs : null;
  }

  /** Acknowledges that a job completed successfully, removing it from the queue. */
  async ack(ctx: { id: string; attempt: number; workerId?: string }): Promise<void> {
    const req = { jobId: ctx.id, attempt: ctx.attempt, workerId: ctx.workerId };
    try {
      await withRetry(this.#retry, () =>
        this.#unary((md, opts, cb) => this.#client.ack(req, md, opts, cb)),
      );
    } catch (e) {
      throw asLeaseError(e);
    }
  }

  /**
   * Negatively acknowledges a job. Returns true if this nack moved the job to
   * the dead-letter queue (because `DeadLetter` was requested or `maxAttempts`
   * was reached), false if it will be retried.
   */
  async nack(
    ctx: { id: string; attempt: number; workerId?: string },
    opts: NackOptions = {},
  ): Promise<boolean> {
    const req = {
      jobId: ctx.id,
      attempt: ctx.attempt,
      reason: opts.reason ?? "",
      retry: retryDirectiveToPb(opts.retry ?? RetryDirective.default()),
      workerId: ctx.workerId,
    };
    try {
      const response = await withRetry(this.#retry, () =>
        this.#unary<{ deadLettered: boolean }>((md, callOpts, cb) =>
          this.#client.nack(req, md, callOpts, cb),
        ),
      );
      return response.deadLettered;
    } catch (e) {
      throw asLeaseError(e);
    }
  }

  /** Extends a job's lease by `extensionMs`, measured from now; returns the new expiry. */
  extend(ctx: { extend(extensionMs: number): Promise<Date> }, extensionMs: number): Promise<Date> {
    return ctx.extend(extensionMs);
  }

  readonly #extendInner = async (
    jobId: string,
    attempt: number,
    extensionMs: number,
    workerId: string | undefined,
  ): Promise<Date> => {
    const req = { jobId, attempt, leaseDuration: msToDuration(extensionMs), workerId };
    let response: ExtendResponse;
    try {
      response = await withRetry(this.#retry, () =>
        this.#unary<ExtendResponse>((md, opts, cb) => this.#client.extend(req, md, opts, cb)),
      );
    } catch (e) {
      throw asLeaseError(e);
    }
    if (!response.leaseExpiresAt) {
      throw new MalformedResponseError("extend returned an invalid lease_expires_at");
    }
    return response.leaseExpiresAt;
  };

  // -------------------------------------------------------------------------
  // Admin
  // -------------------------------------------------------------------------

  /** Fetches the server's version, capabilities, and limits. */
  async getServerInfo(): Promise<ServerInfo> {
    let response: GetServerInfoResponse;
    try {
      response = await withRetry(this.#retry, () =>
        this.#unary<GetServerInfoResponse>((md, opts, cb) =>
          this.#client.getServerInfo({}, md, opts, cb),
        ),
      );
    } catch (e) {
      throw asClientError(e);
    }
    return serverInfoFromPb(response);
  }

  /**
   * Drains dead-lettered jobs for inspection and manual replay. Returns up to
   * `max` records (oldest-first) and **removes them from the server**. This is
   * destructive and is not retried. An empty result is indistinguishable from
   * dead-letter retention being disabled.
   */
  async drainDeadLetters(opts: DrainOptions = {}): Promise<DeadLetterRecord[]> {
    const req = { queue: opts.queue, max: Math.max(1, opts.max ?? 1) };
    let response: { records: PbDeadLetterRecord[] };
    try {
      response = await this.#unary((md, callOpts, cb) =>
        this.#client.drainDeadLetters(req, md, callOpts, cb),
      );
    } catch (e) {
      throw asClientError(e);
    }

    const records: DeadLetterRecord[] = [];
    for (const r of response.records) {
      try {
        records.push(deadLetterRecordFromPb(r));
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn("sepp: skipping malformed dead-letter record in drain response:", err);
      }
    }
    return records;
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  #buildMetadata(): Metadata {
    const md = new Metadata();
    if (this.#bearer) md.set("authorization", this.#bearer);
    const tc = currentTraceContext();
    if (tc) {
      md.set("traceparent", tc.traceparent);
      if (tc.tracestate) md.set("tracestate", tc.tracestate);
    }
    return md;
  }

  #unary<Res>(
    start: (md: Metadata, opts: Partial<CallOptions>, cb: (e: ServiceError | null, r?: Res) => void) => void,
    options?: Partial<CallOptions>,
  ): Promise<Res> {
    // Deadline is computed per attempt so each retry gets a full window.
    const callOpts = options ?? { deadline: Date.now() + this.#rpcTimeout };
    return new Promise<Res>((resolve, reject) => {
      start(this.#buildMetadata(), callOpts, (err, res) => {
        if (err) reject(err);
        else resolve(res as Res);
      });
    });
  }
}

function buildCredentials(opts: ConnectOptions, impliesTls: boolean): ChannelCredentials {
  if (opts.credentials) return opts.credentials;
  const useTls =
    (opts.tls ?? impliesTls) || opts.tlsCaCert !== undefined || opts.tlsDomain !== undefined;
  if (!useTls) return grpcCredentials.createInsecure();
  const ca =
    opts.tlsCaCert === undefined
      ? null
      : typeof opts.tlsCaCert === "string"
        ? Buffer.from(opts.tlsCaCert)
        : opts.tlsCaCert;
  return grpcCredentials.createSsl(ca);
}
