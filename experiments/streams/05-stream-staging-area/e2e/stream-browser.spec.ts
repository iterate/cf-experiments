import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Locator, type Page } from "@playwright/test";

test("stream page appends through the shared browser mirror", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(streamRoute(streamPath));

  await expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible();

  const type = "events.iterate.com/debug/playwright-single";
  await appendComposerEvent(page, {
    type,
    payload: { streamPath, value: crypto.randomUUID() },
  });

  await expect(eventMeta(page, type).first()).toBeVisible();
});

test("split view can mount the same stream twice and mirror appends", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(`/split-stream?left=${encodeURIComponent(streamPath)}&right=${encodeURIComponent(streamPath)}`);

  await expect(page.getByText(streamPath)).toHaveCount(2);
  await expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible();

  const type = "events.iterate.com/debug/playwright-split";
  await appendComposerEvent(page, {
    type,
    payload: { streamPath, value: crypto.randomUUID() },
  });

  await expect(eventMeta(page, type)).toHaveCount(2);
});

test("two browser tabs update and hand off leadership after the writer closes", async ({ context, page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  const otherPage = await context.newPage();

  await Promise.all([
    page.goto(streamRoute(streamPath)),
    otherPage.goto(streamRoute(streamPath)),
  ]);
  await Promise.all([
    expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible(),
    expect(eventMeta(otherPage, "events.iterate.com/stream/created").first()).toBeVisible(),
  ]);

  const type = "events.iterate.com/debug/playwright-two-tabs";
  await appendComposerEvent(page, {
    type,
    payload: { streamPath, value: crypto.randomUUID() },
  });

  await Promise.all([
    expect(eventMeta(page, type).first()).toBeVisible(),
    expect(eventMeta(otherPage, type).first()).toBeVisible(),
  ]);

  const leader = await isLeader(page) ? page : otherPage;
  const follower = leader === page ? otherPage : page;
  await expect(follower.getByTestId("subscription-status")).toContainText(/follower|leader/);
  await leader.close();
  await expect(follower.getByTestId("subscription-status")).toHaveText("leader");

  const afterHandoffType = "events.iterate.com/debug/playwright-after-handoff";
  await appendComposerEvent(follower, {
    type: afterHandoffType,
    payload: { streamPath, value: crypto.randomUUID() },
  });
  await expect(eventMeta(follower, afterHandoffType).first()).toBeVisible();
});

test("split view keeps different streams isolated", async ({ page }) => {
  const leftPath = `/e2e/${crypto.randomUUID()}/left`;
  const rightPath = `/e2e/${crypto.randomUUID()}/right`;
  await page.goto(`/split-stream?left=${encodeURIComponent(leftPath)}&right=${encodeURIComponent(rightPath)}`);

  const leftPane = splitPane(page, leftPath);
  const rightPane = splitPane(page, rightPath);
  await expect(leftPane.getByTestId("subscription-status")).toHaveText("leader");
  await expect(rightPane.getByTestId("subscription-status")).toHaveText("leader");

  const leftType = "events.iterate.com/debug/playwright-left-stream";
  const rightType = "events.iterate.com/debug/playwright-right-stream";
  await appendComposerEvent(leftPane, {
    type: leftType,
    payload: { streamPath: leftPath, value: crypto.randomUUID() },
  });
  await appendComposerEvent(rightPane, {
    type: rightType,
    payload: { streamPath: rightPath, value: crypto.randomUUID() },
  });

  await expect(eventMeta(leftPane, leftType).first()).toBeVisible();
  await expect(eventMeta(leftPane, rightType)).toHaveCount(0);
  await expect(eventMeta(rightPane, rightType).first()).toBeVisible();
  await expect(eventMeta(rightPane, leftType)).toHaveCount(0);
});

test("split view disposes a replaced same-stream pane and keeps leadership", async ({ page }) => {
  const sharedPath = `/e2e/${crypto.randomUUID()}/shared`;
  const nextPath = `/e2e/${crypto.randomUUID()}/next`;
  await page.goto(`/split-stream?left=${encodeURIComponent(sharedPath)}&right=${encodeURIComponent(sharedPath)}`);

  await expect(page.locator(`[data-stream-path='${cssString(sharedPath)}']`)).toHaveCount(2);
  await expect.poll(async () =>
    (await page.getByTestId("subscription-status").allInnerTexts())
      .map((status) => status.toLowerCase())
      .sort()
      .join(","),
  ).toBe("follower,leader");

  await page.getByLabel("Left stream").fill(nextPath);
  await page.getByRole("button", { name: "Go to streams" }).click();

  const sharedPane = splitPane(page, sharedPath);
  const nextPane = splitPane(page, nextPath);
  await expect(sharedPane).toHaveCount(1);
  await expect(nextPane).toHaveCount(1);
  await expect(sharedPane.getByTestId("subscription-status")).toHaveText("leader");
  await expect(nextPane.getByTestId("subscription-status")).toHaveText("leader");

  const sharedType = "events.iterate.com/debug/playwright-shared-after-dispose";
  const nextType = "events.iterate.com/debug/playwright-next-after-dispose";
  await appendComposerEvent(sharedPane, {
    type: sharedType,
    payload: { streamPath: sharedPath, value: crypto.randomUUID() },
  });
  await appendComposerEvent(nextPane, {
    type: nextType,
    payload: { streamPath: nextPath, value: crypto.randomUUID() },
  });

  await expect(eventMeta(sharedPane, sharedType).first()).toBeVisible();
  await expect(eventMeta(sharedPane, nextType)).toHaveCount(0);
  await expect(eventMeta(nextPane, nextType).first()).toBeVisible();
  await expect(eventMeta(nextPane, sharedType)).toHaveCount(0);
});

test("large streams stay virtualized and can scroll from tail to head", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(streamRoute(streamPath));
  await expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible();

  const insertedCount = 1_500;
  await page.getByLabel("Count").fill(String(insertedCount));
  await page.getByLabel("Batch size").fill("250");
  await page.getByLabel("Seconds").fill("0");
  await page.getByRole("button", { name: "Stream random events" }).click();
  await expect(page.locator(".stream-page__insert-state")).toHaveText("done", { timeout: 30_000 });

  const expectedCount = insertedCount + 2;
  await expect(page.getByTestId("event-count")).toHaveText(String(expectedCount), { timeout: 30_000 });
  await expect.poll(
    () => page.locator("[data-testid='event-meta']").count(),
    { timeout: 30_000 },
  ).toBeLessThan(120);
  await expect(page.locator("[data-index='0']")).toHaveCount(0);
  await expect(page.locator(`[data-index='${expectedCount - 1}']`)).toBeVisible();

  await page.getByRole("button", { name: "Scroll to top" }).click();
  await expect(page.locator("[data-index='0']")).toBeVisible();
  await expect(page.locator(`[data-index='${expectedCount - 1}']`)).toHaveCount(0);

  await page.getByRole("button", { name: "Scroll to bottom" }).click();
  await expect(page.locator(`[data-index='${expectedCount - 1}']`)).toBeVisible();
});

test("downloaded SQLite file can be queried from disk", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(streamRoute(streamPath));
  await expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible();

  const type = "events.iterate.com/debug/playwright-download";
  await appendComposerEvent(page, {
    type,
    payload: { streamPath, value: crypto.randomUUID() },
  });
  await expect(eventMeta(page, type).first()).toBeVisible();

  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download", exact: true }).click();
  const download = await downloadPromise;
  const tempDirectory = mkdtempSync(join(tmpdir(), "stream-browser-db-"));
  try {
    const dbPath = join(tempDirectory, download.suggestedFilename());
    await download.saveAs(dbPath);
    expect(sqliteScalar(dbPath, `SELECT COUNT(*) FROM events`)).toBe("3");
    expect(sqliteScalar(dbPath, `SELECT COUNT(*) FROM events WHERE type = '${type}'`)).toBe("1");
  } finally {
    rmSync(tempDirectory, { force: true, recursive: true });
  }
});

test("kill reconnects and appends a new woken event", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(streamRoute(streamPath));
  await expect(page.getByTestId("event-count")).toHaveText("2");

  await page.getByRole("button", { name: "Kill" }).click();
  await expect(page.getByTestId("stream-status")).toHaveText("subscribed", { timeout: 30_000 });
  await expect(page.getByTestId("event-count")).toHaveText("3", { timeout: 30_000 });
  await expect(eventMeta(page, "events.iterate.com/stream/woken")).toHaveCount(2);
});

test("reset discards stale local rows and shows a fresh stream", async ({ page }) => {
  const streamPath = `/e2e/${crypto.randomUUID()}`;
  await page.goto(streamRoute(streamPath));

  const type = "events.iterate.com/debug/playwright-before-reset";
  await appendComposerEvent(page, {
    type,
    payload: { streamPath, value: crypto.randomUUID() },
  });
  await expect(eventMeta(page, type).first()).toBeVisible();
  await expect(page.getByTestId("event-count")).toHaveText("3");

  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await expect(page.getByTestId("stream-status")).toHaveText("subscribed", { timeout: 30_000 });
  await expect(page.getByTestId("event-count")).toHaveText("2", { timeout: 30_000 });
  await expect(eventMeta(page, type)).toHaveCount(0);
  await expect(eventMeta(page, "events.iterate.com/stream/created").first()).toBeVisible();
});

function streamRoute(streamPath: string) {
  if (streamPath === "/") return "/streams/";
  return `/streams/${streamPath.slice(1)}`;
}

function eventMeta(scope: Page | Locator, eventType: string) {
  return scope.locator("[data-testid='event-meta']", { hasText: eventType });
}

async function appendComposerEvent(scope: Page | Locator, event: unknown) {
  await scope.getByLabel("Event JSON").first().fill(JSON.stringify(event, null, 2));
  await scope.getByRole("button", { name: "Append event" }).first().click();
}

function splitPane(page: Page, streamPath: string) {
  return page.locator(`[data-stream-path='${cssString(streamPath)}']`);
}

async function isLeader(page: Page) {
  await expect(page.getByTestId("subscription-status")).toContainText(/leader|follower/);
  return await page.getByTestId("subscription-status").innerText() === "leader";
}

function sqliteScalar(dbPath: string, sql: string) {
  return execFileSync("sqlite3", [dbPath, "-batch", "-noheader", sql], {
    encoding: "utf8",
  }).trim();
}

function cssString(value: string) {
  return value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");
}
