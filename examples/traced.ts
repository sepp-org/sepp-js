/**
 * End-to-end roundtrip *with OpenTelemetry tracing*.
 *
 * Sets up a console span exporter, opens a producer span around the enqueue,
 * and runs a worker. Because `@opentelemetry/api` is installed, the client
 * stamps the producer's W3C trace context onto the job and the worker opens a
 * `sepp.process` CONSUMER span *linked* back to it — you can see both spans
 * (and the link) printed to the console.
 *
 * Requires a running sepp server. Point at it with SEPP_ADDR
 * (default 127.0.0.1:50051):
 *
 *     npx tsx examples/traced.ts
 */
import { context, SpanKind, trace } from "@opentelemetry/api";
import { ConsoleSpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

import { Payload, SeppClient, Worker } from "../src/index";

const ADDR = process.env.SEPP_ADDR ?? "127.0.0.1:50051";
const QUEUE = "traced-example";
const JOB_TYPE = "greeting";

function initTelemetry(): NodeTracerProvider {
  const provider = new NodeTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(new ConsoleSpanExporter())],
  });
  provider.register();
  return provider;
}

async function roundtrip(): Promise<void> {
  const client = await SeppClient.connect(ADDR);
  const tracer = trace.getTracer("sepp-js-example");

  // 1. Enqueue inside a producer span. The active context is stamped onto the
  //    job automatically, so the worker's process span can link back to it.
  const publishSpan = tracer.startSpan("app.publish", { kind: SpanKind.PRODUCER });
  await context.with(trace.setSpan(context.active(), publishSpan), async () => {
    const ack = await client.enqueue({
      queue: QUEUE,
      jobType: JOB_TYPE,
      payload: Payload.text("hello, sepp"),
    });
    console.log(`enqueued job ${ack.jobId}`);
  });
  publishSpan.end();

  // 2. Run a worker. It opens a `sepp.process` span linked to the producer.
  //    This example expects exactly one job, so the handler triggers graceful
  //    shutdown itself; a long-running service would wire it to SIGTERM.
  const worker = new Worker(client, { queues: [QUEUE], leaseDuration: 30_000 });
  const handle = worker.shutdownHandle();

  let processed: string | undefined;
  worker.handle(JOB_TYPE, async (payload, ctx) => {
    console.log(`handling job ${ctx.id}: ${JSON.stringify(payload?.text() ?? "")}`);
    processed = ctx.id;
    handle.shutdown();
  });

  // Give up if the job never arrives (e.g. another consumer grabbed it).
  const guard = setTimeout(() => handle.shutdown(), 15_000);

  // Shutdown drains in-flight jobs (including their acks) before run() resolves.
  await worker.run();
  clearTimeout(guard);
  client.close();

  if (!processed) throw new Error("timed out waiting for the job");
  console.log(`roundtrip OK — job ${processed} processed; see the linked spans printed above`);
}

async function main(): Promise<void> {
  const provider = initTelemetry();
  try {
    await roundtrip();
  } finally {
    await provider.shutdown();
  }
}

main().catch((err) => {
  console.error("error:", err);
  process.exit(1);
});
