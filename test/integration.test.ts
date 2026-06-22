/**
 * End-to-end tests against a real sepp server. Skipped unless SEPP_ADDR is set:
 *
 *     SEPP_ADDR=127.0.0.1:50051 npm test
 *
 * (On Windows PowerShell: `$env:SEPP_ADDR="127.0.0.1:50051"; npm test`.)
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Payload, SeppClient, Worker } from "../src/index";

const ADDR = process.env.SEPP_ADDR;
const QUEUE = "sepp-js-it";

describe.skipIf(!ADDR)("integration", () => {
  let client: SeppClient;

  beforeAll(async () => {
    client = await SeppClient.connect(ADDR as string, {
      apiKey: process.env.SEPP_API_KEY,
    });
  });

  afterAll(() => {
    client?.close();
  });

  it("reports server info", async () => {
    const info = await client.getServerInfo();
    expect(info.version.length).toBeGreaterThan(0);
    expect(info.maxEnqueueBatch).toBeGreaterThan(0);
  });

  it("enqueues, reserves, and acks a job", async () => {
    const jobType = `it-${Date.now()}`;
    const ack = await client.enqueue({
      queue: QUEUE,
      jobType,
      payload: Payload.json({ hello: "world" }),
      custom: { n: 7 },
    });
    expect(ack.jobId).toBeTruthy();

    const jobs = await client.reserve({
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 5_000,
    });
    expect(jobs).not.toBeNull();
    const job = jobs!.find((j) => j.ctx.jobType === jobType);
    expect(job).toBeDefined();
    expect(job!.payload?.json()).toEqual({ hello: "world" });
    expect(job!.ctx.custom.n).toBe(7);
    await client.ack(job!.ctx);
  });

  it("processes a job through a Worker", async () => {
    const jobType = `it-worker-${Date.now()}`;
    await client.enqueue({ queue: QUEUE, jobType, payload: Payload.text("ping") });

    let resolveDone: (s: string) => void;
    const done = new Promise<string>((r) => {
      resolveDone = r;
    });

    const worker = new Worker(client, {
      queues: [QUEUE],
      leaseDuration: 30_000,
      waitTimeout: 2_000,
    }).handle(jobType, async (payload, ctx) => {
      expect(payload?.text()).toBe("ping");
      resolveDone(ctx.id);
    });
    const handle = worker.shutdownHandle();
    const running = worker.run();

    const id = await Promise.race([
      done,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 15_000)),
    ]);
    expect(id).toBeTruthy();

    await new Promise((r) => setTimeout(r, 300));
    handle.shutdown();
    await running;
  });
});
