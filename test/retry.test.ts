import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";

import { RetryPolicy, withRetry } from "../src/retry";

function fastPolicy(maxAttempts: number): RetryPolicy {
  return new RetryPolicy({ maxAttempts, initialBackoff: 1, maxBackoff: 1, jitter: false });
}

const transient = () => ({ code: status.UNAVAILABLE, message: "blip" });
const permanent = () => ({ code: status.INVALID_ARGUMENT, message: "nope" });

describe("withRetry", () => {
  it("returns immediately on success", async () => {
    let calls = 0;
    const result = await withRetry(fastPolicy(5), async () => {
      calls += 1;
      return 42;
    });
    expect(result).toBe(42);
    expect(calls).toBe(1);
  });

  it("succeeds after transient failures", async () => {
    let calls = 0;
    const result = await withRetry(fastPolicy(5), async () => {
      calls += 1;
      if (calls < 3) throw transient();
      return 7;
    });
    expect(result).toBe(7);
    expect(calls).toBe(3);
  });

  it("gives up after maxAttempts", async () => {
    let calls = 0;
    await expect(
      withRetry(fastPolicy(3), async () => {
        calls += 1;
        throw transient();
      }),
    ).rejects.toMatchObject({ code: status.UNAVAILABLE });
    expect(calls).toBe(3);
  });

  it("does not retry non-transient errors", async () => {
    let calls = 0;
    await expect(
      withRetry(fastPolicy(5), async () => {
        calls += 1;
        throw permanent();
      }),
    ).rejects.toMatchObject({ code: status.INVALID_ARGUMENT });
    expect(calls).toBe(1);
  });

  it("default policy runs once (no retries)", async () => {
    let calls = 0;
    await expect(
      withRetry(new RetryPolicy(), async () => {
        calls += 1;
        throw transient();
      }),
    ).rejects.toBeDefined();
    expect(calls).toBe(1);
  });

  it("clamps maxAttempts to >= 1", () => {
    expect(new RetryPolicy({ maxAttempts: 0 }).maxAttempts).toBe(1);
  });
});
