// Single-writer election via the Web Locks API. Exactly one tab holds the named lock at a
// time and is the WRITER: it owns the stream subscription and writes events into the shared
// OPFS database. Every other tab is a READER (its own wa-sqlite connection reads the same
// file). When the writer tab closes or navigates away the lock auto-releases and a waiting
// tab's callback fires, so failover is seamless with no leases or heartbeats to manage.
// Holding the lock for the tab's whole lifetime also signals "this tab is active", which
// discourages the browser from suspending it.

export type WriterRole = {
  /** Resolves true once this tab wins the lock; never resolves false (it just keeps waiting). */
  whenWriter: Promise<void>;
  /** Resign writer role (releases the lock so another tab can take over). */
  release(): void;
};

export function acquireWriterRole(streamPath: string): WriterRole {
  let release = () => {};
  // The lock is held until this promise resolves; resolving it === resigning.
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let signalWriter = () => {};
  const whenWriter = new Promise<void>((resolve) => {
    signalWriter = resolve;
  });
  void navigator.locks.request(
    `stream-writer:${streamPath}`,
    { mode: "exclusive" },
    async () => {
      signalWriter();
      await held;
    },
  );
  return { whenWriter, release: () => release() };
}
