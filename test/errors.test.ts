import type { ServiceError } from "@grpc/grpc-js";
import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";

import {
  AttemptMismatchError,
  asClientError,
  asLeaseError,
  asReserveError,
  BatchValidationError,
  ClientError,
  ConnectError,
  InvalidRequestError,
  isTransientStatus,
  JobNotFoundError,
  JobRejectedError,
  MalformedResponseError,
  OverloadedError,
  SeppError,
  ServerInternalError,
  TransportError,
  UnauthenticatedError,
  UnexpectedStatusError,
  UnknownQueuesError,
} from "../src/errors";

function svc(code: number, message = "x"): ServiceError {
  return { code, message, details: message, metadata: undefined as never, name: "ServiceError" };
}

describe("clientErrorFromStatus (via asClientError)", () => {
  it("maps transport-ish codes to TransportError", () => {
    for (const c of [
      status.UNAVAILABLE,
      status.DEADLINE_EXCEEDED,
      status.ABORTED,
      status.CANCELLED,
    ]) {
      expect(asClientError(svc(c))).toBeInstanceOf(TransportError);
    }
  });

  it("maps auth codes", () => {
    expect(asClientError(svc(status.UNAUTHENTICATED))).toBeInstanceOf(UnauthenticatedError);
    expect(asClientError(svc(status.PERMISSION_DENIED))).toBeInstanceOf(UnauthenticatedError);
  });

  it("maps resource-exhausted to Overloaded", () => {
    expect(asClientError(svc(status.RESOURCE_EXHAUSTED))).toBeInstanceOf(OverloadedError);
  });

  it("maps invalid-argument to InvalidRequest", () => {
    expect(asClientError(svc(status.INVALID_ARGUMENT))).toBeInstanceOf(InvalidRequestError);
  });

  it("maps internal/data-loss/unknown to ServerInternal", () => {
    for (const c of [status.INTERNAL, status.DATA_LOSS, status.UNKNOWN]) {
      expect(asClientError(svc(c))).toBeInstanceOf(ServerInternalError);
    }
  });

  it("maps anything else to UnexpectedStatus and keeps the code", () => {
    const e = asClientError(svc(status.NOT_FOUND));
    expect(e).toBeInstanceOf(UnexpectedStatusError);
    expect((e as UnexpectedStatusError).code).toBe(status.NOT_FOUND);
  });

  it("preserves the message", () => {
    expect(asClientError(svc(status.INTERNAL, "boom")).message).toBe("boom");
  });
});

describe("asLeaseError", () => {
  it("maps NOT_FOUND and FAILED_PRECONDITION", () => {
    expect(asLeaseError(svc(status.NOT_FOUND))).toBeInstanceOf(JobNotFoundError);
    expect(asLeaseError(svc(status.FAILED_PRECONDITION))).toBeInstanceOf(AttemptMismatchError);
  });
  it("falls back to a client error", () => {
    expect(asLeaseError(svc(status.UNAVAILABLE))).toBeInstanceOf(TransportError);
  });
});

describe("asReserveError", () => {
  it("maps FAILED_PRECONDITION to UnknownQueues with the message", () => {
    const e = asReserveError(svc(status.FAILED_PRECONDITION, "queues: a, b"));
    expect(e).toBeInstanceOf(UnknownQueuesError);
    expect(e.message).toBe("queues: a, b");
  });
  it("falls back to a client error", () => {
    expect(asReserveError(svc(status.UNAVAILABLE))).toBeInstanceOf(TransportError);
  });
});

describe("isTransientStatus", () => {
  it("classifies retryable codes", () => {
    for (const c of [
      status.UNAVAILABLE,
      status.DEADLINE_EXCEEDED,
      status.ABORTED,
      status.RESOURCE_EXHAUSTED,
    ]) {
      expect(isTransientStatus(c)).toBe(true);
    }
    for (const c of [
      status.CANCELLED,
      status.INVALID_ARGUMENT,
      status.NOT_FOUND,
      status.FAILED_PRECONDITION,
      status.UNAUTHENTICATED,
      status.INTERNAL,
    ]) {
      expect(isTransientStatus(c)).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------
describe("error hierarchy", () => {
  it("SeppError extends Error", () => {
    expect(new SeppError("x")).toBeInstanceOf(Error);
  });

  it("ClientError extends SeppError", () => {
    expect(new TransportError("x")).toBeInstanceOf(SeppError);
  });

  it("LeaseError extends SeppError", () => {
    expect(new JobNotFoundError()).toBeInstanceOf(SeppError);
  });

  it("ReserveError extends SeppError", () => {
    expect(new UnknownQueuesError("x")).toBeInstanceOf(SeppError);
  });

  it("SeppError name matches class name", () => {
    expect(new TransportError("x").name).toBe("TransportError");
    expect(new UnauthenticatedError("x").name).toBe("UnauthenticatedError");
    expect(new OverloadedError("x").name).toBe("OverloadedError");
    expect(new InvalidRequestError("x").name).toBe("InvalidRequestError");
    expect(new ServerInternalError("x").name).toBe("ServerInternalError");
  });
});

// ---------------------------------------------------------------------------
// Error constructor attributes
// ---------------------------------------------------------------------------
describe("error constructors", () => {
  it("ConnectError carries address and reason", () => {
    const e = new ConnectError("host:51", "refused");
    expect(e.address).toBe("host:51");
    expect(e.message).toContain("host:51");
    expect(e.message).toContain("refused");
    expect(e).toBeInstanceOf(ClientError);
  });

  it("UnexpectedStatusError carries code", () => {
    const e = new UnexpectedStatusError(status.NOT_FOUND, "gone");
    expect(e.code).toBe(status.NOT_FOUND);
    expect(e.message).toContain("gone");
  });

  it("JobRejectedError carries rejection", () => {
    const rej = { kind: "unknown_queue" as const, queue: "q" };
    const e = new JobRejectedError(rej);
    expect(e.rejection).toBe(rej);
  });

  it("BatchValidationError carries errors", () => {
    const errs = [{ index: 0, rejection: { kind: "unknown" as const } }];
    const e = new BatchValidationError(errs);
    expect(e.errors).toBe(errs);
  });

  it("MalformedResponseError carries detail", () => {
    const e = new MalformedResponseError("bad field");
    expect(e.message).toContain("bad field");
    expect(e).toBeInstanceOf(ClientError);
  });
});

// ---------------------------------------------------------------------------
// Fallback paths for non-ServiceError values
// ---------------------------------------------------------------------------
describe("asClientError with non-ServiceError", () => {
  it("wraps a plain Error into TransportError", () => {
    const e = asClientError(new Error("boom"));
    expect(e).toBeInstanceOf(TransportError);
    expect(e.message).toBe("boom");
  });

  it("wraps a string into TransportError", () => {
    const e = asClientError("raw string");
    expect(e).toBeInstanceOf(TransportError);
    expect(e.message).toBe("raw string");
  });

  it("passes through an existing ClientError unchanged", () => {
    const orig = new TransportError("blip");
    const e = asClientError(orig);
    expect(e).toBe(orig);
  });
});

describe("asLeaseError with non-ServiceError", () => {
  it("wraps a plain Error into TransportError", () => {
    const e = asLeaseError(new Error("oops"));
    expect(e).toBeInstanceOf(TransportError);
  });

  it("passes through existing LeaseError unchanged", () => {
    const orig = new JobNotFoundError();
    const e = asLeaseError(orig);
    expect(e).toBe(orig);
  });
});

describe("asReserveError with non-ServiceError", () => {
  it("wraps a plain Error into TransportError", () => {
    const e = asReserveError(new Error("oops"));
    expect(e).toBeInstanceOf(TransportError);
  });

  it("passes through existing ReserveError unchanged", () => {
    const orig = new UnknownQueuesError("x");
    const e = asReserveError(orig);
    expect(e).toBe(orig);
  });
});
