/**
 * Configurable logger for the library.
 *
 * Every diagnostic the library emits flows through a {@link Logger}. By default
 * logging is silent (a no-op). Applications inject `console` for ad-hoc
 * debugging or any structured logger (pino, winston, etc.) for production.
 *
 * When `@opentelemetry/api` is loaded and a span is active, each log call is
 * also recorded as a span event with a `log.severity` attribute, so the OTEL
 * subscriber has a single call site for dual logging.
 */

import { getOtel } from "./trace";

/** Severity levels, matching OpenTelemetry's {@link SeverityNumber}. */
export type Severity = "debug" | "info" | "warn" | "error";

/**
 * Callback the library uses to emit diagnostics.
 *
 * Only `warn` and `error` are required; `debug` and `info` are optional (the
 * library currently does not emit them, but future versions may for worker
 * lifecycle events).
 */
export interface Logger {
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
  debug?(msg: string, meta?: Record<string, unknown>): void;
  info?(msg: string, meta?: Record<string, unknown>): void;
}

const NOOP_LOGGER: Logger = {
  warn: () => {},
  error: () => {},
};

/** Returns a logger that emits no diagnostics. Used when the caller does not supply one. */
export function noopLogger(): Logger {
  return NOOP_LOGGER;
}

/**
 * Wraps a user-provided logger so every call is also recorded as an
 * OpenTelemetry span event when a span is active.
 */
export function createLogger(base: Logger): Logger {
  return {
    debug: (msg, meta) => {
      base.debug?.(msg, meta);
      emitSpanEvent("debug", msg, meta);
    },
    info: (msg, meta) => {
      base.info?.(msg, meta);
      emitSpanEvent("info", msg, meta);
    },
    warn: (msg, meta) => {
      base.warn(msg, meta);
      emitSpanEvent("warn", msg, meta);
    },
    error: (msg, meta) => {
      base.error(msg, meta);
      emitSpanEvent("error", msg, meta);
    },
  };
}

const OTEL_SEVERITY: Record<Severity, number> = {
  debug: 5,
  info: 9,
  warn: 13,
  error: 17,
};

function emitSpanEvent(severity: Severity, msg: string, meta?: Record<string, unknown>): void {
  const api = getOtel();
  if (!api) return;
  const span = api.trace.getSpan(api.context.active());
  if (!span?.isRecording()) return;
  span.addEvent("", {
    ...meta,
    "event.name": "log",
    "log.severity": OTEL_SEVERITY[severity],
    "log.message": msg,
  });
}
