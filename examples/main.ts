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
  console.log(`enqueued job ${ack.jobId} (deduplicated=${ack.deduplicated})`);

  // 2. Run a worker. Its handler resolves the `done` promise so this example can
  //    stop instead of looping forever.
  let resolveDone: (id: string) => void;
  const done = new Promise<string>((resolve) => {
    resolveDone = resolve;
  });

  const worker = new Worker(client, {
    queues: [QUEUE],
    leaseDuration: 30_000,
  }).handle(JOB_TYPE, async (payload, ctx) => {
    const body = payload?.text() ?? "";
    console.log(`worker: processing job ${ctx.id} — payload ${JSON.stringify(body)}`);
    resolveDone(ctx.id);
  });

  const handle = worker.shutdownHandle();
  const running = worker.run();

  // 3. Wait for the job to be processed (with a timeout), then drain & stop.
  const id = await Promise.race([
    done,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("timed out waiting for the job")), 15_000),
    ),
  ]);

  // Give the worker a moment to ack before stopping, so it is not redelivered.
  await new Promise((r) => setTimeout(r, 500));
  handle.shutdown();
  await running;
  client.close();

  console.log(`roundtrip OK — job ${id} was enqueued and processed`);
}

main().catch((err) => {
  console.error("error:", err);
  process.exit(1);
});
