# sepp

JavaScript/TypeScript client for the [sepp](https://github.com/sepp-org/sepp) durable job queue.

It provides both halves of the queue API:

- **Producers** enqueue jobs with a [`SeppClient`](#producing) — one at a time, in best-effort batches, or atomic batches.
- **Consumers** reserve jobs and report their outcome. The low-level `reserve` / `ack` / `nack` / `extend` calls give you full control, while the high-level [`Worker`](#consuming-with-a-worker) runs the whole reserve → process → ack loop for you with bounded concurrency, lease auto-extension, and graceful shutdown.

The client speaks gRPC over HTTP/2 (via [`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js)), so it runs in **Node.js** and Node-compatible runtimes (Bun, Deno). It is async throughout; every call returns a `Promise`.

## Install

```sh
npm install sepp
```

Requires Node 18+. OpenTelemetry tracing is optional; install `@opentelemetry/api` alongside it to enable trace propagation.

## Quickstart

```ts
import { SeppClient, Worker, Payload } from "sepp";

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
import { isJobRejection, describeJobRejection } from "sepp";

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
import { Worker, HandlerError } from "sepp";

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
import { SeppClient, RetryPolicy } from "sepp";

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
