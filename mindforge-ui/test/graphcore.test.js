"use strict";

const { describe, test } = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter } = require("events");
const { createGraphSupervisor, probeGraphUi } = require("../graphcore");

class FakeChild extends EventEmitter {
  constructor() {
    super();
    this.stderr = new EventEmitter();
    this.stdin = { end: () => { this.stdinEnded = true; } };
    this.killed = false;
  }
  kill() { this.killed = true; }
}

describe("probeGraphUi", () => {
  test("accepts only a successful HTML frontend", async () => {
    assert.equal(await probeGraphUi("http://local", async () =>
      new Response("<!doctype html><html></html>", { headers: { "Content-Type": "text/html" } })), true);
    assert.equal(await probeGraphUi("http://local", async () =>
      new Response("no frontend embedded", { status: 404 })), false);
  });
});

describe("graph supervisor", () => {
  test("reuses an already running server", async () => {
    const opened = [];
    const supervisor = createGraphSupervisor({ binPath: "/bin/cbm", cwd: "/tmp",
      openExternal: async (url) => opened.push(url), probeImpl: async () => true,
      existsImpl: () => { throw new Error("must not check binary"); },
      spawnImpl: () => { throw new Error("must not spawn"); } });
    assert.deepEqual(await supervisor.open(), { ok: true, reused: true });
    assert.deepEqual(opened, ["http://127.0.0.1:9749"]);
  });

  test("reports a missing engine", async () => {
    const supervisor = createGraphSupervisor({ binPath: "/missing", cwd: "/tmp",
      openExternal: async () => {}, probeImpl: async () => false, existsImpl: () => false });
    const result = await supervisor.open();
    assert.equal(result.ok, false);
    assert.match(result.error, /is not built/);
  });

  test("starts, probes and opens the bundled server", async () => {
    const child = new FakeChild();
    const spawned = [];
    let probes = 0;
    let opened = 0;
    const supervisor = createGraphSupervisor({ binPath: "/bin/cbm", cwd: "/app",
      openExternal: async () => { opened++; }, existsImpl: () => true,
      spawnImpl: (bin, args, options) => { spawned.push({ bin, args, options }); return child; },
      probeImpl: async () => ++probes >= 2, waitImpl: async () => {}, attempts: 2 });
    assert.deepEqual(await supervisor.open(), { ok: true, started: true });
    assert.equal(opened, 1);
    assert.equal(spawned[0].bin, "/bin/cbm");
    assert.deepEqual(spawned[0].args, ["--ui=true", "--port=9749"]);
    supervisor.stop();
    assert.equal(child.killed, true);
    assert.equal(child.stdinEnded, true);
  });

  test("kills a server that never becomes ready", async () => {
    const child = new FakeChild();
    const supervisor = createGraphSupervisor({ binPath: "/bin/cbm", cwd: "/app",
      openExternal: async () => {}, existsImpl: () => true, spawnImpl: () => child,
      probeImpl: async () => false, waitImpl: async () => {}, attempts: 1 });
    const result = await supervisor.open();
    assert.equal(result.ok, false);
    assert.equal(child.killed, true);
  });
});
