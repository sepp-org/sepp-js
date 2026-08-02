/**
 * End-to-end roundtrip: enqueue one job, then run a worker that processes it.
 *
 * Requires a running sepp server. Point at it with SEPP_ADDR
 * (default 127.0.0.1:50051):
 *
 *     npx tsx examples/main.ts
 */
import { Payload, SeppClient, Worker } from "../src/index";

const ADDR = process.env.SEPP_ADDR ?? "127.0.0.1:50051";
const QUEUE = "example";
const JOB_TYPE = "greeting";

async function main(): Promise<void> {
  const client = await SeppClient.connect(ADDR);

  // 1. Enqueue a single job.
  const ack = await client.enqueue({
    queue: QUEUE,
    jobType: JOB_TYPE,
    payload: Payload.text("hello, sepp"),
  });
  console.log(`enqueued job ${ack.jobId}`);

  // 2. Run a worker. This example expects exactly one job, so the handler
  //    triggers graceful shutdown itself; a long-running service would wire
  //    shutdown to SIGTERM instead (see the README).
  const worker = new Worker(client, {
    queues: [QUEUE],
    leaseDuration: 30_000,
  });
  const handle = worker.shutdownHandle();

  let processed: string | undefined;
  worker.handle(JOB_TYPE, async (payload, ctx) => {
    const body = payload?.text() ?? "";
    console.log(`worker: processing job ${ctx.id} — payload ${JSON.stringify(body)}`);
    processed = ctx.id;
    handle.shutdown();
  });

  // Give up if the job never arrives (e.g. another consumer grabbed it).
  const guard = setTimeout(() => handle.shutdown(), 15_000);

  // 3. Shutdown drains in-flight jobs — including their acks — before run()
  //    resolves, so nothing is left to be redelivered.
  await worker.run();
  clearTimeout(guard);
  client.close();

  if (!processed) throw new Error("timed out waiting for the job");
  console.log(`roundtrip OK — job ${processed} was enqueued and processed`);
}

main().catch((err) => {
  console.error("error:", err);
  process.exit(1);
});
