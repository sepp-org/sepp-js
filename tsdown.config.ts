import { defineConfig, type UserConfig } from "tsdown";

const shared: UserConfig = {
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  // Keep the pre-tsdown output layout: ESM as index.js / index.d.ts.
  outExtensions: ({ format }) => (format === "es" ? { js: ".js", dts: ".d.ts" } : undefined),
  deps: {
    // @grpc/grpc-js and the optional OTel API are resolved by the consumer.
    neverBundle: ["@grpc/grpc-js", "@opentelemetry/api"],
    // @bufbuild/protobuf (used by ts-proto generated code) is bundled on purpose.
    onlyBundle: ["@bufbuild/protobuf"],
  },
};

export default defineConfig([
  { ...shared, dts: false, sourcemap: true },
  // dts runs as its own pass so the JS build stays single-entry; bundling them
  // together makes rolldown split its runtime helper into a separate chunk.
  { ...shared, dts: { emitDtsOnly: true }, clean: false },
]);
