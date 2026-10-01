import { afterEach, describe, expect, test } from "bun:test";
import { rmSync } from "node:fs";
import { startBroker } from "./broker";
import type {
  BrokerToWorker,
  RenderSegmentInput,
  ShardState,
  StreamState,
  TaskState,
} from "./protocol";

const artifactDir = ".meshwork-test-artifacts";
let broker: ReturnType<typeof startBroker> | undefined;

afterEach(() => {
  broker?.stop(true);
  broker = undefined;
  rmSync(artifactDir, { recursive: true, force: true });
});

async function connectWorker(
  port: number,
  id: string,
  capabilities: string[],
) {
  const socket = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener("open", () => resolve(), { once: true });
    socket.addEventListener("error", () => reject(new Error("websocket failed")), {
      once: true,
    });
  });
  socket.send(JSON.stringify({
    type: "worker.hello",
    worker: { id, name: id, platform: "bun", capabilities },
  }));
  return socket;
}

function nextMessage<T extends BrokerToWorker["type"]>(
  socket: WebSocket,
  type: T,
) {
  return new Promise<Extract<BrokerToWorker, { type: T }>>((resolve) => {
    const listener = (event: MessageEvent) => {
      const message = JSON.parse(String(event.data)) as BrokerToWorker;
      if (message.type !== type) return;
      socket.removeEventListener("message", listener);
      resolve(message as Extract<BrokerToWorker, { type: T }>);
    };
    socket.addEventListener("message", listener);
  });
}

describe("broker", () => {
  test("assigns work and accepts only the active attempt", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const socket = await connectWorker(broker.port!, "test-worker", ["echo.test"]);

    const assignmentPromise = nextMessage(socket, "task.assign");
    const submittedResponse = await fetch(`${base}/tasks`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ capability: "echo.test", input: { ok: true } }),
    });
    expect(submittedResponse.status).toBe(202);

    const assignment = await assignmentPromise;

    socket.send(JSON.stringify({
      type: "task.result",
      taskId: assignment.task.id,
      attemptId: "stale-attempt",
      output: { bad: true },
    }));

    let state = await fetch(`${base}/tasks/${assignment.task.id}`).then(
      (response) => response.json() as Promise<TaskState>,
    );
    expect(state.status).toBe("running");

    socket.send(JSON.stringify({
      type: "task.result",
      taskId: assignment.task.id,
      attemptId: assignment.attemptId,
      output: { ok: true },
    }));

    for (let index = 0; index < 20; index += 1) {
      state = await fetch(`${base}/tasks/${assignment.task.id}`).then(
        (response) => response.json() as Promise<TaskState>,
      );
      if (state.status === "completed") break;
      await Bun.sleep(10);
    }

    expect(state.status).toBe("completed");
    expect(state.output).toEqual({ ok: true });
    socket.close();
  });

  test("distributes ordered render segments across free workers", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const first = await connectWorker(broker.port!, "render-a", ["render.segment.test"]);
    const second = await connectWorker(broker.port!, "render-b", ["render.segment.test"]);

    const streamResponse = await fetch(`${base}/streams`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        capability: "render.segment.test",
        width: 3840,
        height: 2160,
        fps: 60,
        codec: "hevc",
        segmentFrames: 30,
        warmupFrames: 8,
        sceneHash: "scene-a",
        renderHash: "render-a",
        snapshotHash: "snapshot-0",
      }),
    });
    expect(streamResponse.status).toBe(201);
    const stream = await streamResponse.json() as StreamState;

    const a = nextMessage(first, "task.assign");
    const b = nextMessage(second, "task.assign");
    const segmentsResponse = await fetch(
      `${base}/streams/${stream.spec.id}/segments`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ count: 2 }),
      },
    );
    expect(segmentsResponse.status).toBe(202);

    const [firstAssignment, secondAssignment] = await Promise.all([a, b]);
    const inputs = [
      firstAssignment.task.input as RenderSegmentInput,
      secondAssignment.task.input as RenderSegmentInput,
    ].sort((left, right) => left.startFrame - right.startFrame);

    expect(inputs.map((input) => input.startFrame)).toEqual([0, 30]);
    expect(inputs.map((input) => input.epoch)).toEqual([0, 0]);
    expect(inputs[0]?.warmupStartFrame).toBe(0);
    expect(inputs[1]?.warmupStartFrame).toBe(22);
    expect(inputs.every((input) => input.width === 3840 && input.height === 2160)).toBe(true);

    first.close();
    second.close();
  });

  test("leases a causal shard and advances only a monotonic tick", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const socket = await connectWorker(broker.port!, "shard-a", ["world.region.v1"]);

    const assignmentPromise = nextMessage(socket, "shard.assign");
    const response = await fetch(`${base}/shards`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        capability: "world.region.v1",
        input: { region: "0:0" },
        startTick: 10,
        snapshotHash: "snapshot-10",
        affinityKey: "world:0:0",
      }),
    });
    expect(response.status).toBe(201);
    const created = await response.json() as ShardState;
    const assignment = await assignmentPromise;

    expect(assignment.shard.id).toBe(created.shard.id);
    expect(assignment.shard.epoch).toBe(0);
    expect(assignment.resumeTick).toBe(10);
    expect(assignment.snapshotHash).toBe("snapshot-10");

    socket.send(JSON.stringify({
      type: "shard.heartbeat",
      workerId: "shard-a",
      shardId: assignment.shard.id,
      leaseId: assignment.leaseId,
      epoch: 0,
      tick: 12,
    }));
    await Bun.sleep(10);

    socket.send(JSON.stringify({
      type: "shard.publish",
      shardId: assignment.shard.id,
      leaseId: assignment.leaseId,
      epoch: 0,
      tick: 13,
      snapshotHash: "snapshot-13",
      output: { population: 7 },
    }));
    await Bun.sleep(10);

    let state = await fetch(`${base}/shards/${assignment.shard.id}`).then(
      (result) => result.json() as Promise<ShardState>,
    );
    expect(state.tick).toBe(13);
    expect(state.snapshotHash).toBe("snapshot-13");
    expect(state.publication).toEqual({ population: 7 });

    socket.send(JSON.stringify({
      type: "shard.heartbeat",
      workerId: "shard-a",
      shardId: assignment.shard.id,
      leaseId: assignment.leaseId,
      epoch: 0,
      tick: 11,
    }));
    await Bun.sleep(10);
    state = await fetch(`${base}/shards/${assignment.shard.id}`).then(
      (result) => result.json() as Promise<ShardState>,
    );
    expect(state.tick).toBe(13);

    socket.close();
  });

  test("new shard epoch revokes the old lease and fences stale publications", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const socket = await connectWorker(broker.port!, "shard-a", ["world.region.v1"]);

    const firstAssignmentPromise = nextMessage(socket, "shard.assign");
    const created = await fetch(`${base}/shards`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        capability: "world.region.v1",
        input: { region: "0:0" },
        startTick: 0,
        snapshotHash: "snapshot-0",
      }),
    }).then((result) => result.json() as Promise<ShardState>);
    const first = await firstAssignmentPromise;

    const revokePromise = nextMessage(socket, "shard.revoke");
    const secondAssignmentPromise = nextMessage(socket, "shard.assign");
    const epochResponse = await fetch(
      `${base}/shards/${created.shard.id}/epochs`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          startTick: 20,
          input: { region: "0:0", weather: "snow" },
          snapshotHash: "snapshot-20",
        }),
      },
    );
    expect(epochResponse.status).toBe(201);
    const revoke = await revokePromise;
    const second = await secondAssignmentPromise;

    expect(revoke.leaseId).toBe(first.leaseId);
    expect(revoke.epoch).toBe(0);
    expect(second.shard.epoch).toBe(1);
    expect(second.resumeTick).toBe(20);
    expect(second.snapshotHash).toBe("snapshot-20");

    socket.send(JSON.stringify({
      type: "shard.publish",
      shardId: created.shard.id,
      leaseId: first.leaseId,
      epoch: 0,
      tick: 99,
      snapshotHash: "stale",
      output: { stale: true },
    }));
    await Bun.sleep(10);

    let state = await fetch(`${base}/shards/${created.shard.id}`).then(
      (result) => result.json() as Promise<ShardState>,
    );
    expect(state.shard.epoch).toBe(1);
    expect(state.tick).toBe(20);
    expect(state.publication).toBeUndefined();

    socket.send(JSON.stringify({
      type: "shard.publish",
      shardId: created.shard.id,
      leaseId: second.leaseId,
      epoch: 1,
      tick: 21,
      snapshotHash: "snapshot-21",
      output: { population: 9 },
    }));
    await Bun.sleep(10);

    state = await fetch(`${base}/shards/${created.shard.id}`).then(
      (result) => result.json() as Promise<ShardState>,
    );
    expect(state.tick).toBe(21);
    expect(state.snapshotHash).toBe("snapshot-21");
    expect(state.publication).toEqual({ population: 9 });

    socket.close();
  });

  test("new epoch cancels obsolete segment and rejects its late result", async () => {
    broker = startBroker({
      port: 0,
      artifactDir,
      leaseMs: 2_000,
      heartbeatTimeoutMs: 2_000,
    });
    const base = `http://127.0.0.1:${broker.port}`;
    const socket = await connectWorker(broker.port!, "render-a", ["render.segment.test"]);

    const stream = await fetch(`${base}/streams`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        capability: "render.segment.test",
        width: 3840,
        height: 2160,
        fps: 60,
        codec: "hevc",
        sceneHash: "scene-a",
        renderHash: "render-a",
        snapshotHash: "snapshot-0",
      }),
    }).then((response) => response.json() as Promise<StreamState>);

    const oldAssignmentPromise = nextMessage(socket, "task.assign");
    await fetch(`${base}/streams/${stream.spec.id}/segments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ count: 1 }),
    });
    const oldAssignment = await oldAssignmentPromise;

    const cancelPromise = nextMessage(socket, "task.cancel");
    const epochResponse = await fetch(
      `${base}/streams/${stream.spec.id}/epochs`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          startFrame: 30,
          sceneHash: "scene-b",
          renderHash: "render-b",
          snapshotHash: "snapshot-30",
        }),
      },
    );
    expect(epochResponse.status).toBe(201);

    const cancel = await cancelPromise;
    expect(cancel.taskId).toBe(oldAssignment.task.id);
    expect(cancel.attemptId).toBe(oldAssignment.attemptId);

    socket.send(JSON.stringify({
      type: "task.result",
      taskId: oldAssignment.task.id,
      attemptId: oldAssignment.attemptId,
      output: { stale: true },
    }));

    const oldState = await fetch(
      `${base}/tasks/${oldAssignment.task.id}`,
    ).then((response) => response.json() as Promise<TaskState>);
    expect(oldState.status).toBe("cancelled");

    const newAssignmentPromise = nextMessage(socket, "task.assign");
    await fetch(`${base}/streams/${stream.spec.id}/segments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ count: 1 }),
    });
    const next = await newAssignmentPromise;
    expect(next.task.cause?.epoch).toBe(1);
    expect(next.task.cause?.startFrame).toBe(30);
    expect((next.task.input as RenderSegmentInput).snapshotHash).toBe("snapshot-30");

    socket.close();
  });
});
