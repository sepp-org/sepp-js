import type { ServiceError } from "@grpc/grpc-js";
import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";

import {
  AttemptMismatchError,
  asClientError,
  asLeaseError,
  asReserveError,
  InvalidRequestError,
  isTransientStatus,
  JobNotFoundError,
  OverloadedError,
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
