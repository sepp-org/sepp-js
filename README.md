<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sepp-org/sepp-js/HEAD/media/sepp-js-paper-200.png">
    <img alt="sepp-js" src="https://raw.githubusercontent.com/sepp-org/sepp-js/HEAD/media/sepp-js-ink-200.png" width="128" height="128">
  </picture>

  <h1>sepp-js</h1>

  <p>
    <strong>The official JavaScript/TypeScript client for <a href="https://github.com/sepp-org/sepp">sepp</a>,</strong>
    <br/>
    a small, language-agnostic durable job queue.
  </p>

  <p>
    <a href="https://github.com/sepp-org/sepp-js/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sepp-org/sepp-js/ci.yml?branch=main&labelColor=181512"></a>
    <a href="https://www.npmjs.com/package/sepp-js"><img alt="npm" src="https://img.shields.io/npm/v/sepp-js?labelColor=181512"></a>
    <a href="https://www.npmjs.com/package/sepp-js"><img alt="node versions" src="https://img.shields.io/node/v/sepp-js?labelColor=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://img.shields.io/npm/l/sepp-js?color=ec6a2e&labelColor=181512"></a>
  </p>

  <p>
    <a href="https://www.npmjs.com/package/sepp-js">npm</a>
    ·
    <a href="https://sepp-org.github.io/sepp/docs/">sepp docs</a>
    ·
    <a href="https://buf.build/sepp-org/sepp-proto/docs/main%3Asepp.v1">Protocol</a>
    ·
    <a href="https://github.com/sepp-org/sepp-js/issues">Issues</a>
  </p>
</div>

## Functionality

- **Producers** — enqueue jobs one at a time, in best-effort batches, or atomically. Supports idempotency keys, priorities, scheduled delivery and custom metadata.
- **Consumers** — a high-level `Worker` runs the whole reserve → process → ack loop for you with bounded concurrency, automatic lease extension and graceful shutdown, or drop down to the raw `reserve` / `ack` / `nack` / `extend` calls for full control.
- **Observability** — if `@opentelemetry/api` is installed and a tracer provider is registered, the client propagates W3C trace context from the producer's enqueue span to the worker's process span. With nothing installed, everything is a no-op.
- **Typed errors** — every error extends `SeppError`, and deterministic server rejections (payload too large, unknown queue, …) are separate from transient transport errors, so retry logic stays simple.

The client speaks gRPC over HTTP/2 (via
[`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js)), so it runs in
**Node.js** and Node-compatible runtimes (Bun, Deno). It is async throughout;
every call returns a `Promise`.

## Install

```sh
npm install sepp-js
```

Requires Node 22+. OpenTelemetry tracing is optional; install `@opentelemetry/api` alongside it to enable trace propagation.

## Quickstart

Enqueue a job, then run a worker that processes it (requires a running [sepp server](https://github.com/sepp-org/sepp)):

```ts
import { SeppClient, Worker, Payload } from "sepp-js";

const client = await SeppClient.connect("127.0.0.1:50051");

// Producer: enqueue a job onto the `emails` queue.
const ack = await client.enqueue({
  queue: "emails",
  jobType: "send_welcome",
  payload: Payload.json({ user: 42 }),
});
console.log(`enqueued job ${ack.jobId}`);

// Consumer: process `send_welcome` jobs. Returning normally acks the job;
// throwing a HandlerError nacks it.
await new Worker(client, { queues: ["emails"], leaseDuration: 30_000 })
  .handle("send_welcome", async (payload, ctx) => {
    console.log(`processing job ${ctx.id}`, payload?.json());
  })
  .run();
```

Runnable versions live in [`examples/`](examples/), including
[`traced.ts`](examples/traced.ts) which wires up an OTLP exporter for
end-to-end distributed tracing.

## Concepts

- **Queues and job types.** Every job is enqueued onto a named queue and tagged with a `jobType`. Workers reserve from one or more queues and dispatch each job to the handler registered for its `jobType`.
- **Payload.** Each job can carry an opaque `Payload` (bytes + an `encoding` hint). Use `Payload.json(value)`, `Payload.text(string)`, or `Payload.bytes(data, encoding)`. For small key/value metadata, use `custom` instead.
- **Leases and redelivery.** A reserved job is leased for `leaseDuration` milliseconds. Ack, nack, or extend it before the lease expires, or the server redelivers it (with `attempt` incremented) until `maxAttempts` is reached and it is dead-lettered. A [`Worker`](#consuming-with-a-worker) can extend leases for you — see `autoExtend`.
- **Durations are milliseconds; timestamps are `Date`s.** `leaseDuration`, `waitTimeout`, etc. are numbers of milliseconds; `enqueuedAt`, `leaseExpiresAt`, `scheduledAt`, and `serverTime` are `Date` objects.

## Producing

```ts
// Single job — throws JobRejectedError if the server rejects this job.
const ack = await client.enqueue({
  queue: "emails",
  jobType: "send_welcome",
  payload: Payload.json({ to: "a@b.com" }),
  idempotencyKey: "welcome-user-42", // server drops duplicates within its window
  priority: 7, // 0–9, higher dequeues first
  maxAttempts: 5,
  custom: { tenant: "acme", attemptBudget: 3 },
});

// Best-effort batch — one result per job, in order; narrow with isJobRejection.
import { isJobRejection, describeJobRejection } from "sepp-js";

const results = await client.enqueueBatch([
  { queue: "emails", jobType: "send_welcome" },
  { queue: "emails", jobType: "send_receipt" },
]);
for (const r of results) {
  if (isJobRejection(r)) console.warn("rejected:", describeJobRejection(r));
  else console.log("accepted:", r.jobId);
}

// Atomic batch — all or nothing; throws BatchValidationError if any job fails.
const acks = await client.enqueueAtomic([
  { queue: "steps", jobType: "step_1" },
  { queue: "steps", jobType: "step_2" },
]);
```

## Consuming with a Worker

```ts
import { Worker, HandlerError } from "sepp-js";

const worker = new Worker(client, {
  queues: ["emails"],
  leaseDuration: 30_000,
  maxInFlight: 32, // process up to 32 jobs concurrently (default 16)
  autoExtend: true, // keep leases alive while handlers run
})
  .handle("send_welcome", async (payload, ctx) => {
    await sendEmail(payload?.json());
    // returning acks the job
  })
  .handle("send_receipt", async () => {
    throw HandlerError.retry("payment service unavailable"); // nack for retry
  });

// Stop gracefully on SIGTERM: stop reserving, drain in-flight jobs, then return.
const shutdown = worker.shutdownHandle();
process.on("SIGTERM", () => shutdown.shutdown());

await worker.run();
```

A handler's outcome decides the job's fate:

- returns normally → **ack**
- throws `HandlerError.retry(reason)` → nack, retry under the queue's policy
- throws `HandlerError.retryAfter(reason, delayMs)` → nack, retry after a delay
- throws `HandlerError.permanent(reason)` → nack straight to the dead-letter queue
- throws anything else → nack for retry (and the error is logged)

### Manual reserve loop

If you want full control instead of the `Worker`, drive the loop yourself:

```ts
const jobs = await client.reserve({ queues: ["emails"], leaseDuration: 30_000, waitTimeout: 20_000 });
if (jobs) {
  for (const { payload, ctx } of jobs) {
    try {
      await handle(payload, ctx);
      await client.ack(ctx);
    } catch {
      await client.nack(ctx, { reason: "failed" });
    }
  }
}
```

`reserve` resolves with an array of jobs, or `null` when the long-poll wait elapses with nothing available.

## Connection options

```ts
import { SeppClient, RetryPolicy } from "sepp-js";

const client = await SeppClient.connect("emails.internal:50051", {
  apiKey: process.env.SEPP_API_KEY, // sent as Authorization: Bearer <key>
  tls: true, // platform root certificates
  // tlsCaCert: fs.readFileSync("ca.pem"), // or trust a private CA
  // tlsDomain: "sepp.internal", // override the verified name
  retryPolicy: new RetryPolicy({ maxAttempts: 5, initialBackoff: 100, maxBackoff: 5_000 }),
  connectTimeout: 5_000,
  rpcTimeout: 30_000, // deadline per unary RPC (all but reserve)
  // maxReceiveMessageBytes: 32 * 1024 * 1024, // raise for large reserve batches
});
```

The address may be `host:port`, `http://host:port` (plaintext), or `https://host:port` (implies TLS). The retry policy retries transient RPC failures on enqueue, ack, nack, extend, and getServerInfo; reserve (a long poll) and drainDeadLetters (destructive) are never retried. The default policy performs no retries.

`rpcTimeout` (default 30 s) bounds every unary call except `reserve`, which derives its deadline from its `waitTimeout`. Very large enqueue batches may need a higher value.

`maxReceiveMessageBytes` caps the response size the client accepts (grpc-js default 4 MiB). Workers reserving large batches should raise it: a reserve response can carry up to the server's `maxReserveBatch * maxPayloadBytes`, and an oversized response fails client-side after the jobs were already leased, stranding them until their leases expire.

## Errors

All errors extend `SeppError`. The notable ones:

- `ClientError` and subclasses (`TransportError`, `UnauthenticatedError`, `OverloadedError`, `InvalidRequestError`, `ServerInternalError`, `ConnectError`, …) — transport/protocol failures.
- `JobRejectedError` — `enqueue` got a per-job rejection (`.rejection` is a `JobRejection`).
- `BatchValidationError` — `enqueueAtomic` rejected the batch (`.errors`).
- `LeaseError` → `JobNotFoundError` / `AttemptMismatchError` — the worker no longer holds the lease (ack/nack/extend). A `JobNotFoundError` from a retried ack/nack can also mean an earlier attempt already succeeded and only its response was lost.
- `UnknownQueuesError` — reserve hit a strict-mode queue that isn't declared.

## OpenTelemetry

If `@opentelemetry/api` is installed and a tracer provider is registered, the client propagates the active W3C trace context: it stamps `traceparent`/`tracestate` onto enqueued jobs and outgoing metadata, and the `Worker` opens a `sepp.process` span linked back to the producer. With nothing installed, everything is a no-op. See [`examples/traced.ts`](./examples/traced.ts).

## Logging

The library is **silent by default**. To see diagnostics, pass a logger:

```ts
const client = await SeppClient.connect("127.0.0.1:50051", {
  logger: console, // ad-hoc debugging
});

const worker = new Worker(client, {
  queues: ["emails"],
  leaseDuration: 30_000,
  logger: {
    // Structured logger (pino, winston, etc.)
    warn: (msg, meta) => logger.warn(meta, msg),
    error: (msg, meta) => logger.error(meta, msg),
  },
});
```

The `Logger` interface requires `warn` and `error` (both `(msg, meta?) => void`). `debug` and `info` are optional for future use.

## Dead-letter inspection

```ts
const records = await client.drainDeadLetters({ queue: "emails", max: 100 });
for (const record of records) {
  console.warn(`${record.jobId} failed: ${record.cause} (${record.lastReason})`);
  await client.enqueue(record.toEnqueueRequest()); // replay it
}
```

Draining **removes** the records from the server, so it is not retried — process what you get back.

## Development

```sh
npm run gen        # regenerate src/gen from the BSR module via buf + ts-proto (needs `buf` on PATH)
npm run build      # emit dist/ (ESM + CJS + .d.ts) with tsup
npm test           # run the unit suite (vitest)
npm run typecheck  # tsc --noEmit
npm run example    # run examples/main.ts against $SEPP_ADDR (default 127.0.0.1:50051)
```

Codegen pulls the contract straight from `buf.build/sepp-org/sepp-proto` (version pinned in `buf.gen.yaml`), and the generated stubs under `src/gen/` are committed, so installing the package needs no `buf`/`protoc`.

## Docs

This README is the client's usage documentation. For running and configuring
the sepp server itself, see the [sepp docs site](https://sepp-org.github.io/sepp/docs/).

## License

sepp-js is licensed under the MIT License. See [LICENSE](LICENSE) for details.
