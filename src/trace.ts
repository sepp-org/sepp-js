/**
 * W3C trace-context validation and optional OpenTelemetry integration.
 *
 * OpenTelemetry is an *optional* peer dependency (the analog of the Rust
 * `opentelemetry` feature / Python `[otel]` extra). It is loaded lazily and
 * best-effort: if `@opentelemetry/api` is not installed, every OTel-dependent
 * function here quietly no-ops. {@link loadOtel} is awaited once during
 * `SeppClient.connect` and `Worker.run` so later synchronous reads succeed.
 */
import type * as OtelApi from "@opentelemetry/api";

import { TraceContextError } from "./errors";
import type { EnqueueRequest as PbEnqueueRequest } from "./gen/sepp/v1/queue";

/**
 * Validates a W3C `traceparent` (`version-trace_id-span_id-flags`), throwing
 * {@link TraceContextError} on any malformed field. Mirrors the Rust validator.
 */
export function validateTraceparent(s: string): void {
  const parts = s.split("-");
  if (parts.length !== 4) {
    throw new TraceContextError("invalid traceparent: expected 4 hyphen-separated fields");
  }
  const [ver, traceId, spanId, flags] = parts as [string, string, string, string];
  const isHex = (x: string) => /^[0-9a-fA-F]+$/.test(x);
  if (ver.length !== 2 || !isHex(ver)) {
    throw new TraceContextError("invalid traceparent: version must be 2 hex chars");
  }
  if (traceId.length !== 32 || !isHex(traceId)) {
    throw new TraceContextError("invalid traceparent: trace_id must be 32 hex chars");
  }
  if (/^0+$/.test(traceId)) {
    throw new TraceContextError("invalid traceparent: trace_id must not be all zeros");
  }
  if (spanId.length !== 16 || !isHex(spanId)) {
    throw new TraceContextError("invalid traceparent: span_id must be 16 hex chars");
  }
  if (/^0+$/.test(spanId)) {
    throw new TraceContextError("invalid traceparent: span_id must not be all zeros");
  }
  if (flags.length !== 2 || !isHex(flags)) {
    throw new TraceContextError("invalid traceparent: flags must be 2 hex chars");
  }
}

let otel: typeof OtelApi | null = null;
let attempted = false;

/** Lazily loads `@opentelemetry/api`, caching the result (or null if unavailable). */
export async function loadOtel(): Promise<typeof OtelApi | null> {
  if (attempted) return otel;
  attempted = true;
  try {
    otel = await import("@opentelemetry/api");
  } catch {
    otel = null;
  }
  return otel;
}

/** Returns the loaded OTel API, or null if it has not loaded / is unavailable. */
export function getOtel(): typeof OtelApi | null {
  return otel;
}

/** The active span's W3C trace context, or null if there is no valid active span. */
export function currentTraceContext(): {
  traceparent: string;
  tracestate?: string;
} | null {
  const api = otel;
  if (!api) return null;
  const span = api.trace.getSpan(api.context.active());
  if (!span) return null;
  const sc = span.spanContext();
  if (!api.isSpanContextValid(sc)) return null;
  const flags = (sc.traceFlags & 0xff).toString(16).padStart(2, "0");
  const tracestate = sc.traceState?.serialize();
  return {
    traceparent: `00-${sc.traceId}-${sc.spanId}-${flags}`,
    tracestate: tracestate || undefined,
  };
}

/** Decodes a `traceparent` into an OTel SpanContext (for span links), or null. */
export function spanContextFromTraceparent(traceparent: string): OtelApi.SpanContext | null {
  const api = otel;
  if (!api) return null;
  const parts = traceparent.split("-");
  if (parts.length !== 4) return null;
  const [, traceId, spanId, flags] = parts as [string, string, string, string];
  const sc: OtelApi.SpanContext = {
    traceId,
    spanId,
    traceFlags: Number.parseInt(flags, 16),
    isRemote: true,
  };
  return api.isSpanContextValid(sc) ? sc : null;
}

/**
 * Stamps the active span's trace context onto each job that does not already
 * carry one, so the producer's trace links to the worker that runs the job.
 */
export function injectJobTrace(jobs: PbEnqueueRequest[]): void {
  const tc = currentTraceContext();
  if (!tc) return;
  for (const job of jobs) {
    if (!job.traceContext) {
      job.traceContext = {
        traceparent: tc.traceparent,
        tracestate: tc.tracestate,
      };
    }
  }
}
