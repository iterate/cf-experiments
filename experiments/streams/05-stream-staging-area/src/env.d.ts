interface Env {
  STREAM: DurableObjectNamespace<import("./worker").Stream>;
  STREAM_PROCESSOR_RUNNER: DurableObjectNamespace<import("./worker").StreamProcessorRunner>;
}
