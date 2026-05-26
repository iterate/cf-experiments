#!/usr/bin/env node
/**
 * Plot payload sweep results as SVG.
 *
 *   node scripts/plot-sweep.ts findings/deployed-sweep.jsonl findings/payload-curve.svg
 */

import { readFileSync, writeFileSync } from "node:fs";

const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) {
  console.error("Usage: node scripts/plot-sweep.ts <results.jsonl> <output.svg>");
  process.exit(1);
}

type Row = {
  variant: string;
  payloadBytes: number;
  wallPerSecond: number;
  mbPerSecond: number;
  verified: boolean;
};

const rows = readFileSync(inputPath, "utf8")
  .trim()
  .split("\n")
  .filter(Boolean)
  .map((line) => JSON.parse(line) as Row)
  .filter((r) => r.verified);

const shared = aggregateByPayload(rows.filter((r) => r.variant === "shared"));
const autoinc = aggregateByPayload(rows.filter((r) => r.variant === "autoinc"));

writeFileSync(outputPath, renderSvg({ shared, autoinc }));
console.error(`Wrote ${outputPath}`);

function aggregateByPayload(items: Row[]) {
  const byPayload = new Map<number, Row[]>();
  for (const item of items) {
    const list = byPayload.get(item.payloadBytes) ?? [];
    list.push(item);
    byPayload.set(item.payloadBytes, list);
  }
  return [...byPayload.entries()]
    .sort(([a], [b]) => a - b)
    .map(([payloadBytes, runs]) => ({
      payloadBytes,
      eventsPerSecond: median(runs.map((r) => r.wallPerSecond)),
      mbPerSecond: median(runs.map((r) => r.mbPerSecond)),
      runs: runs.length,
    }));
}

function median(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
}

function renderSvg(args: {
  shared: ReturnType<typeof aggregateByPayload>;
  autoinc: ReturnType<typeof aggregateByPayload>;
}) {
  const width = 920;
  const height = 520;
  const margin = { top: 48, right: 88, bottom: 64, left: 72 };
  const plotW = width - margin.left - margin.right;
  const plotH = (height - margin.top - margin.bottom) / 2 - 24;

  const payloads = args.shared.map((p) => p.payloadBytes);
  const xMin = Math.min(...payloads);
  const xMax = Math.max(...payloads);
  const x = (payload: number) =>
    margin.left + (Math.log10(payload + 1) - Math.log10(xMin + 1)) / (Math.log10(xMax + 1) - Math.log10(xMin + 1)) * plotW;

  const eventsMax = Math.max(...args.shared.map((p) => p.eventsPerSecond), ...args.autoinc.map((p) => p.eventsPerSecond)) * 1.08;
  const mbMax = Math.max(...args.shared.map((p) => p.mbPerSecond), ...args.autoinc.map((p) => p.mbPerSecond)) * 1.08;

  const topY = margin.top;
  const bottomY = margin.top + plotH + 48;

  const yEvents = (value: number, base: number) => base + plotH - (value / eventsMax) * plotH;
  const yMb = (value: number, base: number) => base + plotH - (value / mbMax) * plotH;

  const line = (points: { x: number; y: number }[]) =>
    points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(" ");

  const sharedEvents = args.shared.map((p) => ({ x: x(p.payloadBytes), y: yEvents(p.eventsPerSecond, topY) }));
  const autoincEvents = args.autoinc.map((p) => ({ x: x(p.payloadBytes), y: yEvents(p.eventsPerSecond, topY) }));
  const sharedMb = args.shared.map((p) => ({ x: x(p.payloadBytes), y: yMb(p.mbPerSecond, bottomY) }));
  const autoincMb = args.autoinc.map((p) => ({ x: x(p.payloadBytes), y: yMb(p.mbPerSecond, bottomY) }));

  const xTicks = args.shared
    .map(
      (p) =>
        `<text x="${x(p.payloadBytes).toFixed(1)}" y="${height - 18}" text-anchor="middle" font-size="11" fill="#444">${formatPayload(p.payloadBytes)}</text>`,
    )
    .join("\n");

  const sweetSpot =
    args.shared.find((p) => p.payloadBytes === 256) ??
    args.shared.reduce((best, p) => (p.eventsPerSecond > best.eventsPerSecond ? p : best));

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="#fafafa"/>
  <text x="${width / 2}" y="24" text-anchor="middle" font-size="16" font-weight="600" fill="#111">Deployed DO in-DO writeLoop — payload vs throughput</text>
  <text x="${width / 2}" y="42" text-anchor="middle" font-size="11" fill="#666">100k events per point · fresh DO per run · log-scaled payload axis</text>

  <text x="16" y="${topY + plotH / 2}" transform="rotate(-90 16 ${topY + plotH / 2})" text-anchor="middle" font-size="12" fill="#2563eb">events/s</text>
  <text x="16" y="${bottomY + plotH / 2}" transform="rotate(-90 16 ${bottomY + plotH / 2})" text-anchor="middle" font-size="12" fill="#059669">MB/s</text>

  ${grid(topY, plotH, margin.left, plotW, eventsMax, "events")}
  ${grid(bottomY, plotH, margin.left, plotW, mbMax, "mb")}

  <path d="${line(sharedEvents)}" fill="none" stroke="#2563eb" stroke-width="2.5"/>
  <path d="${line(autoincEvents)}" fill="none" stroke="#93c5fd" stroke-width="2" stroke-dasharray="6 4"/>
  <path d="${line(sharedMb)}" fill="none" stroke="#059669" stroke-width="2.5"/>
  <path d="${line(autoincMb)}" fill="none" stroke="#6ee7b7" stroke-width="2" stroke-dasharray="6 4"/>

  ${dots(sharedEvents, "#2563eb")}
  ${dots(sharedMb, "#059669")}
  ${marker(x(sweetSpot.payloadBytes), yEvents(sweetSpot.eventsPerSecond, topY), "#2563eb", "256 B balanced")}

  <text x="${margin.left + plotW - 4}" y="${topY + 16}" text-anchor="end" font-size="11" fill="#2563eb">■ shared events/s</text>
  <text x="${margin.left + plotW - 4}" y="${topY + 32}" text-anchor="end" font-size="11" fill="#93c5fd">▬ autoinc events/s</text>
  <text x="${margin.left + plotW - 4}" y="${bottomY + 16}" text-anchor="end" font-size="11" fill="#059669">■ shared MB/s</text>
  <text x="${margin.left + plotW - 4}" y="${bottomY + 32}" text-anchor="end" font-size="11" fill="#6ee7b7">▬ autoinc MB/s</text>

  <text x="${width / 2}" y="${topY + plotH + 28}" text-anchor="middle" font-size="12" fill="#444">payload size (bytes)</text>
  ${xTicks}
</svg>`;
}

function grid(baseY: number, plotH: number, left: number, plotW: number, max: number, kind: "events" | "mb") {
  const ticks = 4;
  let out = `<rect x="${left}" y="${baseY}" width="${plotW}" height="${plotH}" fill="#fff" stroke="#ddd"/>`;
  for (let i = 0; i <= ticks; i++) {
    const value = (max / ticks) * i;
    const y = baseY + plotH - (value / max) * plotH;
    out += `<line x1="${left}" y1="${y}" x2="${left + plotW}" y2="${y}" stroke="#eee"/>`;
    const label = kind === "events" ? `${Math.round(value / 1000)}k` : value.toFixed(0);
    out += `<text x="${left - 8}" y="${y + 4}" text-anchor="end" font-size="10" fill="#666">${label}</text>`;
  }
  return out;
}

function dots(points: { x: number; y: number }[], color: string) {
  return points
    .map((p) => `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="3.5" fill="${color}"/>`)
    .join("\n");
}

function marker(x: number, y: number, color: string, label: string) {
  return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="5" fill="none" stroke="${color}" stroke-width="2"/>
<text x="${(x + 8).toFixed(1)}" y="${(y - 8).toFixed(1)}" font-size="10" fill="${color}">${label}</text>`;
}

function formatPayload(bytes: number) {
  if (bytes === 0) return "0";
  if (bytes >= 1024) return `${bytes / 1024}k`;
  return String(bytes);
}
