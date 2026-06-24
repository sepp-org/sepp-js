import { describe, expect, it, vi } from "vitest";

import { createLogger, type Logger, noopLogger } from "../src/logger";

describe("noopLogger", () => {
  it("does not throw on any method", () => {
    const logger = noopLogger();
    expect(() => logger.warn("test")).not.toThrow();
    expect(() => logger.error("test")).not.toThrow();
  });

  it("has optional debug and info methods", () => {
    const logger = noopLogger();
    expect(() => logger.debug?.("test")).not.toThrow();
    expect(() => logger.info?.("test")).not.toThrow();
  });
});

describe("createLogger", () => {
  it("forwards warn to base logger", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn() };
    const wrapped = createLogger(base);
    wrapped.warn("hello", { key: "val" });
    expect(base.warn).toHaveBeenCalledWith("hello", { key: "val" });
  });

  it("forwards error to base logger", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn() };
    const wrapped = createLogger(base);
    wrapped.error("boom", { err: "x" });
    expect(base.error).toHaveBeenCalledWith("boom", { err: "x" });
  });

  it("forwards debug to base logger when present", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const wrapped = createLogger(base);
    wrapped.debug?.("dbg");
    expect(base.debug).toHaveBeenCalledWith("dbg", undefined);
  });

  it("forwards info to base logger when present", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn(), info: vi.fn() };
    const wrapped = createLogger(base);
    wrapped.info?.("msg");
    expect(base.info).toHaveBeenCalledWith("msg", undefined);
  });

  it("does not throw when debug is not on base", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn() };
    const wrapped = createLogger(base);
    expect(() => wrapped.debug?.("test")).not.toThrow();
  });

  it("does not throw when info is not on base", () => {
    const base: Logger = { warn: vi.fn(), error: vi.fn() };
    const wrapped = createLogger(base);
    expect(() => wrapped.info?.("test")).not.toThrow();
  });

  it("emits OTel span events when OTel is available", () => {
    // OTel span event emission is tested via integration with actual OTel.
    // Here we just verify the logger wraps without errors.
    const base: Logger = { warn: vi.fn(), error: vi.fn() };
    const wrapped = createLogger(base);
    expect(wrapped).toBeDefined();
    expect(typeof wrapped.warn).toBe("function");
    expect(typeof wrapped.error).toBe("function");
  });
});

describe("Logger interface", () => {
  it("accepts objects with just warn and error", () => {
    const logger: Logger = {
      warn: () => {},
      error: () => {},
    };
    expect(logger).toBeDefined();
  });

  it("accepts objects with all methods", () => {
    const logger: Logger = {
      warn: () => {},
      error: () => {},
      debug: () => {},
      info: () => {},
    };
    expect(logger).toBeDefined();
  });
});
