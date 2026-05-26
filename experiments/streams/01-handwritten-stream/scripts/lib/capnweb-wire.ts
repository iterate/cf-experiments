import type { ParsedWsMessage, WsMessage } from "./with-project.js";

/**
 * A client `pull` followed by the matching server `resolve` / `reject`.
 *
 * This is not necessarily one TCP/TLS packet round trip. It is the Cap'n Web
 * protocol event that means "the caller asked for this result and waited for
 * the peer to send it back".
 */
export type WireResultWait = {
  index: number;
  pullId: number;
  status: "resolved" | "rejected" | "pending";
  /**
   * ms from client pull frame to the point where the result can be delivered to
   * application code. Usually this is the matching resolve/reject, but a resolve
   * expression can contain `["promise", exportId]` placeholders which must also
   * settle first (e.g. `.map()` over an array).
   */
  waitMs?: number;
  /** Promise export IDs that had to settle before this result was usable. */
  awaitedPromiseIds: number[];
  /** outbound frames since the previous pulled result */
  request: TimedParsedWsMessage[];
  /** inbound/outbound frames observed until this result was usable */
  response: TimedParsedWsMessage[];
  /** matching resolve/reject for this pull ID */
  terminal?: TimedParsedWsMessage;
  /** final frame needed before application code can observe the result */
  completion?: TimedParsedWsMessage;
};

/** Backwards-compatible alias: this is a protocol result wait, not a packet RTT. */
export type WireRoundTrip = WireResultWait;

export type WireWave = {
  index: number;
  waits: WireResultWait[];
  startedAtMs: number;
  endedAtMs?: number;
  /** wall-clock ms covered by this wave of overlapping waits */
  waitMs?: number;
};

export type WireGap = {
  /** ms of client-side idle time after wave n before wave n+1 starts */
  afterWave: number;
  ms: number;
};

export type WireAnalysis = {
  /** Every pulled RPC result. Equivalent to `roundTrips` for old call sites. */
  resultWaits: WireResultWait[];
  /** Deprecated name kept because tests naturally talk about round trips. */
  roundTrips: WireRoundTrip[];
  /**
   * Groups overlapping waits. This is the closest heuristic for "how many
   * latency windows did this code create?" If two pulls are sent before either
   * response arrives, they are one wave even though there are two results.
   */
  waves: WireWave[];
  gaps: WireGap[];
  spanMs: number;
};

export type TimedParsedWsMessage = ParsedWsMessage & { tMs: number; frameIndex: number };

export function analyzeCapnwebWire(wsMessages: WsMessage[]): WireAnalysis {
  const frames = parseFrames(wsMessages);
  const resultWaits = collectResultWaits(frames);
  const waves = collectWaves(resultWaits);
  const gaps = collectGaps(waves);
  const spanMs = frames.length === 0 ? 0 : frames.at(-1)!.tMs - frames[0]!.tMs;

  return {
    resultWaits,
    roundTrips: resultWaits,
    waves,
    gaps,
    spanMs,
  };
}

export function formatCapnwebWire(wsMessages: WsMessage[]): string {
  const analysis = analyzeCapnwebWire(wsMessages);
  const lines: string[] = [];

  lines.push(
    `capnweb wire: ${analysis.resultWaits.length} pulled result(s), ${analysis.waves.length} latency wave(s), ${analysis.spanMs.toFixed(1)}ms span`,
  );
  lines.push("-".repeat(72));

  for (const wave of analysis.waves) {
    const wait = wave.waitMs === undefined ? "pending" : `${wave.waitMs.toFixed(1)}ms`;
    lines.push(`wave ${wave.index}  ${wait}  ${wave.waits.length} pulled result(s)`);

    for (const result of wave.waits) {
      const resultWait = result.waitMs === undefined ? "pending" : `${result.waitMs.toFixed(1)}ms`;
      lines.push(
        `  pull#${result.pullId}  ${result.status}  ${resultWait}  ${summarizeBatch(result.request)}`,
      );
      if (result.terminal) {
        lines.push(
          `       <- ${summarizeFrame(result.terminal.data)} @ +${result.terminal.tMs.toFixed(1)}ms`,
        );
      }
      if (result.awaitedPromiseIds.length > 0 && result.completion) {
        lines.push(
          `       awaited exported promise(s) ${result.awaitedPromiseIds.join(", ")} until +${result.completion.tMs.toFixed(1)}ms`,
        );
      }
    }

    const gap = analysis.gaps.find((g) => g.afterWave === wave.index);
    if (gap && gap.ms > 0.05) {
      lines.push(`       gap ${gap.ms.toFixed(1)}ms (client idle before next wave)`);
    }
  }

  const trailing = trailingFrameCount(wsMessages.length, analysis);
  if (trailing > 0) {
    lines.push(`(${trailing} trailing frame(s), usually release/cleanup or unpulled work)`);
  }

  return lines.join("\n");
}

function parseFrames(wsMessages: WsMessage[]): TimedParsedWsMessage[] {
  return wsMessages.map((frame, frameIndex) => ({
    frameIndex,
    direction: frame.direction,
    tMs: frame.tMs,
    data: JSON.parse(frame.data) as unknown,
  }));
}

function collectResultWaits(frames: TimedParsedWsMessage[]) {
  const pulls = frames.filter((frame) => frame.direction === "out" && isPull(frame.data));

  return pulls.map((pullFrame, index): WireResultWait => {
    const pullId = pullIdFrom(pullFrame.data);
    const previousPull = pulls[index - 1];
    const requestStart = previousPull ? previousPull.frameIndex + 1 : 0;
    const request = frames.slice(requestStart, pullFrame.frameIndex + 1);
    const terminal = frames.find(
      (frame) =>
        frame.frameIndex > pullFrame.frameIndex &&
        frame.direction === "in" &&
        isTerminalFor(frame.data, pullId),
    );
    const dependencies = terminal ? collectPromiseDependencies(frames, terminal) : [];
    const completion =
      dependencies.at(-1)?.frame ??
      terminal;
    const awaitedPromiseIds = dependencies.map((dependency) => dependency.promiseId);

    return {
      index,
      pullId,
      status: terminal ? terminalStatus(terminal.data) : "pending",
      waitMs: completion ? completion.tMs - pullFrame.tMs : undefined,
      awaitedPromiseIds,
      request,
      response: completion ? frames.slice(pullFrame.frameIndex + 1, completion.frameIndex + 1) : [],
      terminal,
      completion,
    };
  });
}

function collectWaves(resultWaits: WireResultWait[]): WireWave[] {
  const waves: WireWave[] = [];

  for (const wait of resultWaits) {
    const startedAtMs = wait.request.at(-1)?.tMs ?? 0;
    const endedAtMs = wait.completion?.tMs;
    const current = waves.at(-1);

    if (!current || current.endedAtMs === undefined || startedAtMs > current.endedAtMs) {
      waves.push({
        index: waves.length,
        waits: [wait],
        startedAtMs,
        endedAtMs,
        waitMs: endedAtMs === undefined ? undefined : endedAtMs - startedAtMs,
      });
      continue;
    }

    current.waits.push(wait);
    if (endedAtMs !== undefined && endedAtMs > current.endedAtMs) {
      current.endedAtMs = endedAtMs;
      current.waitMs = current.endedAtMs - current.startedAtMs;
    }
  }

  return waves;
}

function collectGaps(waves: WireWave[]): WireGap[] {
  const gaps: WireGap[] = [];
  for (let i = 0; i < waves.length - 1; i += 1) {
    const wave = waves[i]!;
    const next = waves[i + 1]!;
    if (wave.endedAtMs === undefined) continue;
    gaps.push({ afterWave: wave.index, ms: next.startedAtMs - wave.endedAtMs });
  }
  return gaps;
}

function trailingFrameCount(messageCount: number, analysis: WireAnalysis) {
  const lastTerminal = analysis.resultWaits
    .map((wait) => wait.completion?.frameIndex ?? wait.request.at(-1)?.frameIndex ?? -1)
    .reduce((max, index) => Math.max(max, index), -1);
  return Math.max(0, messageCount - lastTerminal - 1);
}

function collectPromiseDependencies(
  frames: TimedParsedWsMessage[],
  terminal: TimedParsedWsMessage,
) {
  const dependencies: Array<{ promiseId: number; frame: TimedParsedWsMessage }> = [];
  const seen = new Set<number>();
  const pending = collectPromiseIds(terminal.data);

  while (pending.length > 0) {
    const promiseId = pending.shift()!;
    if (seen.has(promiseId)) continue;
    seen.add(promiseId);

    const frame = frames.find(
      (candidate) =>
        candidate.frameIndex > terminal.frameIndex &&
        candidate.direction === "in" &&
        isTerminalFor(candidate.data, promiseId),
    );
    if (!frame) continue;

    dependencies.push({ promiseId, frame });
    pending.push(...collectPromiseIds(frame.data));
  }

  return dependencies.sort((a, b) => a.frame.frameIndex - b.frame.frameIndex);
}

function collectPromiseIds(value: unknown): number[] {
  if (!Array.isArray(value)) {
    if (!value || typeof value !== "object") return [];
    return Object.values(value).flatMap((child) => collectPromiseIds(child));
  }

  if (value[0] === "promise" && typeof value[1] === "number") return [value[1]];
  return value.flatMap((child) => collectPromiseIds(child));
}

function summarizeBatch(frames: TimedParsedWsMessage[]) {
  return frames
    .filter((frame) => frame.direction === "out" && !isRelease(frame.data))
    .map((frame) => summarizeFrame(frame.data))
    .join(" -> ");
}

function summarizeFrame(data: unknown) {
  if (!Array.isArray(data)) return JSON.stringify(data);
  const [op, ...rest] = data;
  if (op === "push" && Array.isArray(rest[0])) {
    const push = rest[0] as unknown[];
    if (push[0] === "pipeline") {
      const path = push[2];
      const method = Array.isArray(path) ? path.join(".") : String(path);
      return `push ${method}${push[3] !== undefined ? `(${truncate(JSON.stringify(push[3]))})` : ""}`;
    }
    return `push ${JSON.stringify(rest[0])}`;
  }
  if (op === "pull") return `pull#${rest[0]}`;
  if (op === "resolve") return `resolve#${rest[0]}`;
  if (op === "reject") return `reject#${rest[0]}`;
  if (op === "release") return `release#${rest[0]}`;
  if (op === "stream") return `stream ${truncate(JSON.stringify(rest[0]))}`;
  return JSON.stringify(data);
}

function truncate(s: string, max = 48) {
  return s.length <= max ? s : `${s.slice(0, max - 1)}...`;
}

function isPull(data: unknown) {
  return Array.isArray(data) && data[0] === "pull" && typeof data[1] === "number";
}

function isTerminalFor(data: unknown, pullId: number) {
  return (
    Array.isArray(data) &&
    (data[0] === "resolve" || data[0] === "reject") &&
    data[1] === pullId
  );
}

function terminalStatus(data: unknown): WireResultWait["status"] {
  if (!Array.isArray(data)) return "pending";
  if (data[0] === "resolve") return "resolved";
  if (data[0] === "reject") return "rejected";
  return "pending";
}

function isRelease(data: unknown) {
  return Array.isArray(data) && data[0] === "release";
}

function pullIdFrom(data: unknown) {
  if (!Array.isArray(data) || data[0] !== "pull" || typeof data[1] !== "number") {
    throw new Error(`expected pull frame, got ${JSON.stringify(data)}`);
  }
  return data[1];
}
