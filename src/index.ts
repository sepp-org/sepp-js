/**
 * sepp — JavaScript/TypeScript client for the sepp durable job queue.
 *
 * Producers enqueue jobs with a {@link SeppClient}; consumers reserve and report
 * outcomes either directly on the client or through the high-level {@link Worker}.
 */

export type { ConnectOptions, DrainOptions, NackOptions } from "./client";
export { SeppClient } from "./client";
export {
  AttemptMismatchError,
  BatchResultCountMismatchError,
  BatchValidationError,
  ClientError,
  ConnectError,
  DuplicateHandlerError,
  EmptyBatchError,
  InvalidApiKeyError,
  InvalidRequestError,
  JobConversionError,
  JobNotFoundError,
  JobRejectedError,
  LeaseError,
  MalformedResponseError,
  OverloadedError,
  PriorityRangeError,
  ReserveError,
  SeppError,
  ServerInfoError,
  ServerInternalError,
  TraceContextError,
  TransportError,
  UnauthenticatedError,
  UnexpectedStatusError,
  UnknownQueuesError,
  ValidationError,
} from "./errors";
/** The generated protobuf message types and gRPC stub, for advanced use. */
export * as pb from "./gen/sepp/v1/queue";
export type { RetryPolicyOptions } from "./retry";
export { RetryPolicy } from "./retry";
export type {
  DeadLetterCause,
  EnqueueAck,
  EnqueueRequest,
  Job,
  JobRejection,
  JobValidationError,
  Primitive,
  ReserveOptions,
  ServerInfo,
} from "./types";
export {
  DeadLetterRecord,
  describeJobRejection,
  isJobRejection,
  JobCtx,
  Payload,
  Priority,
  RetryDirective,
  TraceContext,
} from "./types";
export type { Handler, WorkerOptions } from "./worker";
export { HandlerError, ShutdownHandle, Worker } from "./worker";
