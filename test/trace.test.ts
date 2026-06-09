import { describe, expect, it } from "vitest";

import { TraceContextError } from "../src/errors";
import { validateTraceparent } from "../src/trace";

const VALID = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

describe("validateTraceparent", () => {
  it("accepts a well-formed traceparent", () => {
    expect(() => validateTraceparent(VALID)).not.toThrow();
  });

  const bad: Array<[string, string]> = [
    ["wrong field count", "00-deadbeef-0123"],
    ["bad version length", "0-0123456789abcdef0123456789abcdef-0123456789abcdef-01"],
    ["non-hex version", "0g-0123456789abcdef0123456789abcdef-0123456789abcdef-01"],
    ["bad trace_id length", "00-deadbeef-0123456789abcdef-01"],
    ["non-hex trace_id", "00-0123456789abcdeg0123456789abcdef-0123456789abcdef-01"],
    ["all-zero trace_id", "00-00000000000000000000000000000000-0123456789abcdef-01"],
    ["bad span_id length", "00-0123456789abcdef0123456789abcdef-deadbeef-01"],
    ["non-hex span_id", "00-0123456789abcdef0123456789abcdef-0123456789abcdeg-01"],
    ["all-zero span_id", "00-0123456789abcdef0123456789abcdef-0000000000000000-01"],
    ["bad flags length", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0"],
    ["non-hex flags", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0g"],
  ];

  it.each(bad)("rejects %s", (_label, tp) => {
    expect(() => validateTraceparent(tp)).toThrow(TraceContextError);
  });
});
