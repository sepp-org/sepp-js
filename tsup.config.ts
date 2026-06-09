import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  target: "node18",
  dts: true,
  sourcemap: true,
  clean: true,
  // @grpc/grpc-js and the optional OTel API are resolved by the consumer.
  external: ["@grpc/grpc-js", "@opentelemetry/api"],
});
