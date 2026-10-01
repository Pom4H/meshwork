import { mkdirSync } from "node:fs";
import { join } from "node:path";
import type { ServerWebSocket } from "bun";
import type {
  BrokerToWorker,
  JsonValue,
  RenderSegmentInput,
  ShardSpec,
  ShardState,
  StreamEpoch,
  StreamSpec,
  StreamState,
  Task,
  TaskState,
  WorkerDescriptor,
  WorkerToBroker,
} from "./protocol";

type SocketData = { workerId?: string };

type ConnectedWorker = {
  descriptor: WorkerDescriptor;
  socket: ServerWebSocket<SocketData>;
  taskId?: string;
  attemptId?: string;
  shardId?: string;
  shardLeaseId?: string;
  lastSeenAt: number;
  lastAffinityKey?: string;
};

export type BrokerOptions = {
  port?: number;
  artifactDir?: string;
  leaseMs?: number;
  heartbeatTimeoutMs?: number;
  tls?: { certPath: string; keyPath: string };
};

const json = (value: unknown, status = 200) =>
  Response.json(value, { status, headers: { "cache-control": "no-store" } });

const safeId = (value: string) => /^[a-zA-Z0-9-]+$/.test(value);
const finite = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);
const positiveInt = (value: unknown, max = Number.MAX_SAFE_INTEGER) =>
  finite(value) && Number.isInteger(value) && value > 0 && value <= max;
const nonNegativeInt = (value: unknown, max = Number.MAX_SAFE_INTEGER) =>
  finite(value) && Number.isInteger(value) && value >= 0 && value <= max;
const nonEmpty = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

export function startBroker(options: number | BrokerOptions = 8787) {
  const config: BrokerOptions =
    typeof options === "number" ? { port: options } : options;
  const port = config.port ?? 8787;
  const leaseMs = config.leaseMs ?? 30_000;
  const heartbeatTimeoutMs = config.heartbeatTimeoutMs ?? 15_000;
  const artifactDir = config.artifactDir ?? ".meshwork/artifacts";

  mkdirSync(artifactDir, { recursive: true });

  const workers = new Map<string, ConnectedWorker>();
  const tasks = new Map<string, TaskState>();
  const streams = new Map<string, StreamState>();
  const shards = new Map<string, ShardState>();
  const pending: string[] = [];

  const send = (worker: ConnectedWorker, message: BrokerToWorker) => {
    worker.socket.send(JSON.stringify(message));
  };

  const createTask = (
    capability: string,
    input: JsonValue,
    options: Pick<Task, "cause" | "affinityKey" | "deadlineAt" | "priority"> = {},
  ) => {
    const now = Date.now();
    const task: Task = {
      id: crypto.randomUUID(),
      capability,
      input,
      ...options,
    };
    const state: TaskState = {
      task,
      status: "pending",
      attempts: 0,
      createdAt: now,
      updatedAt: now,
    };
    tasks.set(task.id, state);
    return state;
  };

  const enqueue = (state: TaskState, front = false) => {
    if (state.status === "pending" && !pending.includes(state.task.id)) {
      if (front) pending.unshift(state.task.id);
      else pending.push(state.task.id);
    }
  };

  const releaseWorker = (workerId: string, taskId: string, attemptId: string) => {
    const worker = workers.get(workerId);
    if (worker?.taskId === taskId && worker.attemptId === attemptId) {
      worker.taskId = undefined;
      worker.attemptId = undefined;
    }
  };

  const cancelTask = (state: TaskState, reason: string) => {
    if (state.status !== "pending" && state.status !== "running") return;
    if (state.status === "running" && state.workerId && state.attemptId) {
      const worker = workers.get(state.workerId);
      if (
        worker?.taskId === state.task.id &&
        worker.attemptId === state.attemptId
      ) {
        send(worker, {
          type: "task.cancel",
          taskId: state.task.id,
          attemptId: state.attemptId,
        });
      }
      releaseWorker(state.workerId, state.task.id, state.attemptId);
    }

    state.status = "cancelled";
    state.error = reason;
    state.workerId = undefined;
    state.attemptId = undefined;
    state.leaseUntil = undefined;
    state.updatedAt = Date.now();
  };

  const requeue = (state: TaskState) => {
    if (state.status !== "running") return;
    if (state.workerId && state.attemptId) {
      releaseWorker(state.workerId, state.task.id, state.attemptId);
    }
    state.status = "pending";
    state.workerId = undefined;
    state.attemptId = undefined;
    state.leaseUntil = undefined;
    state.updatedAt = Date.now();
    enqueue(state, true);
  };

  const releaseShardWorker = (
    workerId: string,
    shardId: string,
    leaseId: string,
  ) => {
    const worker = workers.get(workerId);
    if (worker?.shardId === shardId && worker.shardLeaseId === leaseId) {
      worker.shardId = undefined;
      worker.shardLeaseId = undefined;
    }
  };

  const requeueShard = (state: ShardState, reason?: string) => {
    if (state.status !== "leased") return;
    if (state.workerId && state.leaseId) {
      releaseShardWorker(state.workerId, state.shard.id, state.leaseId);
    }
    state.status = "pending";
    state.workerId = undefined;
    state.leaseId = undefined;
    state.leaseUntil = undefined;
    state.error = reason;
    state.updatedAt = Date.now();
  };

  const revokeShard = (
    state: ShardState,
    reason: string,
    requeue: boolean,
  ) => {
    if (state.status === "leased" && state.workerId && state.leaseId) {
      const worker = workers.get(state.workerId);
      if (
        worker?.shardId === state.shard.id &&
        worker.shardLeaseId === state.leaseId
      ) {
        send(worker, {
          type: "shard.revoke",
          shardId: state.shard.id,
          leaseId: state.leaseId,
          epoch: state.shard.epoch,
          reason,
        });
      }
      releaseShardWorker(state.workerId, state.shard.id, state.leaseId);
    }
    state.status = requeue ? "pending" : "stopped";
    state.workerId = undefined;
    state.leaseId = undefined;
    state.leaseUntil = undefined;
    state.error = reason;
    state.updatedAt = Date.now();
  };

  const chooseShardWorker = (shard: ShardSpec) => {
    const candidates = [...workers.values()].filter(
      (candidate) =>
        candidate.taskId === undefined &&
        candidate.shardId === undefined &&
        candidate.descriptor.capabilities.includes(shard.capability),
    );
    candidates.sort((a, b) => {
      const aAffinity = Number(
        shard.affinityKey !== undefined &&
          a.lastAffinityKey === shard.affinityKey,
      );
      const bAffinity = Number(
        shard.affinityKey !== undefined &&
          b.lastAffinityKey === shard.affinityKey,
      );
      return bAffinity - aAffinity || b.lastSeenAt - a.lastSeenAt;
    });
    return candidates[0];
  };

  const scheduleShards = () => {
    for (const state of shards.values()) {
      if (state.status !== "pending") continue;
      const worker = chooseShardWorker(state.shard);
      if (!worker) continue;

      const leaseId = crypto.randomUUID();
      const leaseUntil = Date.now() + leaseMs;
      state.status = "leased";
      state.workerId = worker.descriptor.id;
      state.leaseId = leaseId;
      state.leaseUntil = leaseUntil;
      state.error = undefined;
      state.updatedAt = Date.now();
      worker.shardId = state.shard.id;
      worker.shardLeaseId = leaseId;

      send(worker, {
        type: "shard.assign",
        shard: state.shard,
        leaseId,
        leaseUntil,
        resumeTick: state.tick,
        snapshotHash: state.snapshotHash,
        publication: state.publication,
      });
    }
  };

  const shardLease = (
    workerId: string,
    shardId: string,
    leaseId: string,
    epoch: number,
  ) => {
    if (!nonNegativeInt(epoch)) return;
    const state = shards.get(shardId);
    if (
      !state ||
      state.status !== "leased" ||
      state.workerId !== workerId ||
      state.leaseId !== leaseId ||
      state.shard.epoch !== epoch
    ) {
      return;
    }
    return state;
  };

  const renewShard = (
    workerId: string,
    shardId: string,
    leaseId: string,
    epoch: number,
    observedTick: number,
  ) => {
    if (!nonNegativeInt(observedTick)) return;
    const state = shardLease(workerId, shardId, leaseId, epoch);
    if (!state || observedTick < state.tick) return;
    state.observedTick = Math.max(state.observedTick ?? state.tick, observedTick);
    state.leaseUntil = Date.now() + leaseMs;
    state.updatedAt = Date.now();
    return state;
  };

  const publishShard = (
    workerId: string,
    shardId: string,
    leaseId: string,
    epoch: number,
    tick: number,
  ) => {
    if (!nonNegativeInt(tick)) return;
    const state = shardLease(workerId, shardId, leaseId, epoch);
    if (!state || tick < state.tick) return;
    state.tick = tick;
    state.observedTick = Math.max(state.observedTick ?? tick, tick);
    state.leaseUntil = Date.now() + leaseMs;
    state.updatedAt = Date.now();
    return state;
  };

  const pendingOrder = (aId: string, bId: string) => {
    const a = tasks.get(aId)?.task;
    const b = tasks.get(bId)?.task;
    if (!a || !b) return 0;
    const aDeadline = a.deadlineAt ?? Number.POSITIVE_INFINITY;
    const bDeadline = b.deadlineAt ?? Number.POSITIVE_INFINITY;
    if (aDeadline !== bDeadline) return aDeadline - bDeadline;
    const aPriority = a.priority ?? 0;
    const bPriority = b.priority ?? 0;
    return bPriority - aPriority;
  };

  const chooseWorker = (task: Task) => {
    const candidates = [...workers.values()].filter(
      (candidate) =>
        candidate.taskId === undefined &&
        candidate.shardId === undefined &&
        candidate.descriptor.capabilities.includes(task.capability),
    );
    candidates.sort((a, b) => {
      const aAffinity = Number(
        task.affinityKey !== undefined && a.lastAffinityKey === task.affinityKey,
      );
      const bAffinity = Number(
        task.affinityKey !== undefined && b.lastAffinityKey === task.affinityKey,
      );
      return bAffinity - aAffinity || b.lastSeenAt - a.lastSeenAt;
    });
    return candidates[0];
  };

  const schedule = () => {
    scheduleShards();
    pending.sort(pendingOrder);

    for (let index = 0; index < pending.length; ) {
      const taskId = pending[index];
      const state = taskId ? tasks.get(taskId) : undefined;

      if (!taskId || !state || state.status !== "pending") {
        pending.splice(index, 1);
        continue;
      }

      const worker = chooseWorker(state.task);
      if (!worker) {
        index += 1;
        continue;
      }

      const attemptId = crypto.randomUUID();
      const leaseUntil = Date.now() + leaseMs;
      worker.taskId = taskId;
      worker.attemptId = attemptId;
      state.status = "running";
      state.workerId = worker.descriptor.id;
      state.attemptId = attemptId;
      state.leaseUntil = leaseUntil;
      state.attempts += 1;
      state.updatedAt = Date.now();
      pending.splice(index, 1);

      send(worker, {
        type: "task.assign",
        task: state.task,
        attemptId,
        leaseUntil,
      });
    }
  };

  const completeTask = (
    workerId: string,
    taskId: string,
    attemptId: string,
    result: { output: JsonValue } | { error: string },
  ) => {
    const state = tasks.get(taskId);
    if (
      !state ||
      state.status !== "running" ||
      state.workerId !== workerId ||
      state.attemptId !== attemptId
    ) {
      return;
    }

    const worker = workers.get(workerId);
    if ("output" in result) {
      state.status = "completed";
      state.output = result.output;
      state.error = undefined;
      if (worker && state.task.affinityKey) {
        worker.lastAffinityKey = state.task.affinityKey;
      }
    } else {
      state.status = "failed";
      state.error = result.error;
    }

    state.leaseUntil = undefined;
    state.updatedAt = Date.now();
    releaseWorker(workerId, taskId, attemptId);
    schedule();
  };

  const invalidateOlderEpochs = (streamId: string, epoch: number) => {
    for (const state of tasks.values()) {
      const cause = state.task.cause;
      if (
        cause?.streamId === streamId &&
        cause.epoch < epoch &&
        (state.status === "pending" || state.status === "running")
      ) {
        cancelTask(state, `superseded by stream epoch ${epoch}`);
      }
    }
  };

  const streamSegments = (streamId: string, epoch?: number) =>
    [...tasks.values()]
      .filter((state) => {
        const cause = state.task.cause;
        return (
          cause?.streamId === streamId &&
          (epoch === undefined || cause.epoch === epoch)
        );
      })
      .sort((a, b) => {
        const ac = a.task.cause;
        const bc = b.task.cause;
        return (
          (ac?.epoch ?? 0) - (bc?.epoch ?? 0) ||
          (ac?.startFrame ?? 0) - (bc?.startFrame ?? 0)
        );
      });

  const reap = () => {
    const now = Date.now();

    for (const [workerId, worker] of workers) {
      if (now - worker.lastSeenAt <= heartbeatTimeoutMs) continue;
      if (worker.taskId) {
        const state = tasks.get(worker.taskId);
        if (state?.status === "running") requeue(state);
      }
      if (worker.shardId) {
        const state = shards.get(worker.shardId);
        if (
          state?.status === "leased" &&
          state.workerId === workerId &&
          state.leaseId === worker.shardLeaseId
        ) {
          requeueShard(state, "worker heartbeat timeout");
        }
      }
      workers.delete(workerId);
      worker.socket.close(1012, "heartbeat timeout");
    }

    for (const state of tasks.values()) {
      if (
        state.status === "running" &&
        state.leaseUntil !== undefined &&
        state.leaseUntil <= now
      ) {
        requeue(state);
      }
    }

    for (const state of shards.values()) {
      if (
        state.status === "leased" &&
        state.leaseUntil !== undefined &&
        state.leaseUntil <= now
      ) {
        requeueShard(state, "shard lease expired");
      }
    }

    schedule();
  };

  const reaper = setInterval(
    reap,
    Math.max(1_000, Math.floor(heartbeatTimeoutMs / 3)),
  );

  const server = Bun.serve<SocketData>({
    port,
    tls: config.tls
      ? {
          cert: Bun.file(config.tls.certPath),
          key: Bun.file(config.tls.keyPath),
        }
      : undefined,
    fetch: async (request, bunServer) => {
      const url = new URL(request.url);
      const parts = url.pathname.split("/").filter(Boolean);

      if (url.pathname === "/ws") {
        if (bunServer.upgrade(request, { data: {} })) return;
        return new Response("WebSocket upgrade failed", { status: 400 });
      }

      if (request.method === "GET" && url.pathname === "/") {
        return new Response(Bun.file(new URL("../web/index.html", import.meta.url)));
      }

      if (request.method === "GET" && url.pathname === "/worker.js") {
        return new Response(Bun.file(new URL("../web/worker.js", import.meta.url)), {
          headers: { "content-type": "text/javascript; charset=utf-8" },
        });
      }

      if (request.method === "GET" && url.pathname === "/health") {
        return json({
          ok: true,
          workers: workers.size,
          tasks: tasks.size,
          streams: streams.size,
          shards: shards.size,
        });
      }

      if (request.method === "GET" && url.pathname === "/workers") {
        return json(
          [...workers.values()].map((worker) => ({
            ...worker.descriptor,
            busy: worker.taskId !== undefined,
            taskId: worker.taskId,
            attemptId: worker.attemptId,
            shardId: worker.shardId,
            shardLeaseId: worker.shardLeaseId,
            lastSeenAt: worker.lastSeenAt,
            lastAffinityKey: worker.lastAffinityKey,
          })),
        );
      }

      if (request.method === "POST" && url.pathname === "/tasks") {
        const body = (await request.json()) as {
          capability?: unknown;
          input?: JsonValue;
          affinityKey?: unknown;
          deadlineAt?: unknown;
          priority?: unknown;
        };

        if (!nonEmpty(body.capability)) {
          return json({ error: "capability must be a non-empty string" }, 400);
        }
        if (
          body.affinityKey !== undefined &&
          typeof body.affinityKey !== "string"
        ) {
          return json({ error: "affinityKey must be a string" }, 400);
        }
        if (body.deadlineAt !== undefined && !finite(body.deadlineAt)) {
          return json({ error: "deadlineAt must be a finite number" }, 400);
        }
        if (body.priority !== undefined && !finite(body.priority)) {
          return json({ error: "priority must be a finite number" }, 400);
        }

        const state = createTask(body.capability, body.input ?? null, {
          affinityKey: body.affinityKey as string | undefined,
          deadlineAt: body.deadlineAt as number | undefined,
          priority: body.priority as number | undefined,
        });
        enqueue(state);
        schedule();
        return json(state, 202);
      }

      if (request.method === "GET" && url.pathname.startsWith("/tasks/")) {
        const taskId = url.pathname.slice("/tasks/".length);
        const state = tasks.get(taskId);
        return state ? json(state) : json({ error: "task not found" }, 404);
      }


      if (parts[0] === "shards" && parts.length === 1) {
        if (request.method === "GET") {
          return json([...shards.values()]);
        }

        if (request.method === "POST") {
          const body = (await request.json()) as {
            capability?: unknown;
            input?: JsonValue;
            startTick?: unknown;
            snapshotHash?: unknown;
            affinityKey?: unknown;
          };
          if (!nonEmpty(body.capability)) {
            return json({ error: "capability must be a non-empty string" }, 400);
          }
          if (
            body.startTick !== undefined &&
            !nonNegativeInt(body.startTick)
          ) {
            return json({ error: "startTick must be a non-negative integer" }, 400);
          }
          if (
            body.snapshotHash !== undefined &&
            typeof body.snapshotHash !== "string"
          ) {
            return json({ error: "snapshotHash must be a string" }, 400);
          }
          if (
            body.affinityKey !== undefined &&
            typeof body.affinityKey !== "string"
          ) {
            return json({ error: "affinityKey must be a string" }, 400);
          }

          const now = Date.now();
          const id = crypto.randomUUID();
          const startTick = (body.startTick as number | undefined) ?? 0;
          const shard: ShardSpec = {
            id,
            capability: body.capability,
            input: body.input ?? null,
            epoch: 0,
            startTick,
            affinityKey: body.affinityKey as string | undefined,
          };
          const state: ShardState = {
            shard,
            status: "pending",
            tick: startTick,
            observedTick: startTick,
            snapshotHash: body.snapshotHash as string | undefined,
            createdAt: now,
            updatedAt: now,
          };
          shards.set(id, state);
          schedule();
          return json(state, 201);
        }
      }

      if (parts[0] === "shards" && parts[1]) {
        const shardId = parts[1];
        const state = shards.get(shardId);
        if (!state) return json({ error: "shard not found" }, 404);

        if (parts.length === 2 && request.method === "GET") {
          return json(state);
        }

        if (parts[2] === "epochs" && request.method === "POST") {
          const body = (await request.json()) as {
            startTick?: unknown;
            input?: JsonValue;
            snapshotHash?: unknown;
          };
          if (
            body.startTick !== undefined &&
            !nonNegativeInt(body.startTick)
          ) {
            return json({ error: "startTick must be a non-negative integer" }, 400);
          }
          if (
            body.snapshotHash !== undefined &&
            typeof body.snapshotHash !== "string"
          ) {
            return json({ error: "snapshotHash must be a string" }, 400);
          }

          const nextTick =
            (body.startTick as number | undefined) ?? state.tick;
          const nextEpoch = state.shard.epoch + 1;
          revokeShard(state, `superseded by shard epoch ${nextEpoch}`, true);
          state.shard = {
            ...state.shard,
            epoch: nextEpoch,
            startTick: nextTick,
            input: body.input ?? state.shard.input,
          };
          state.tick = nextTick;
          state.observedTick = nextTick;
          state.snapshotHash =
            body.snapshotHash === undefined
              ? state.snapshotHash
              : (body.snapshotHash as string);
          state.publication = undefined;
          state.error = undefined;
          state.updatedAt = Date.now();
          schedule();
          return json(state, 201);
        }

        if (parts[2] === "stop" && request.method === "POST") {
          revokeShard(state, "shard stopped", false);
          schedule();
          return json(state);
        }
      }

      if (parts[0] === "streams" && parts.length === 1) {
        if (request.method === "GET") {
          return json([...streams.values()]);
        }

        if (request.method === "POST") {
          const body = (await request.json()) as {
            capability?: unknown;
            width?: unknown;
            height?: unknown;
            fps?: unknown;
            codec?: unknown;
            segmentFrames?: unknown;
            warmupFrames?: unknown;
            startFrame?: unknown;
            sceneHash?: unknown;
            renderHash?: unknown;
            snapshotHash?: unknown;
          };

          if (!nonEmpty(body.capability)) {
            return json({ error: "capability must be a non-empty string" }, 400);
          }
          if (!positiveInt(body.width, 16_384) || !positiveInt(body.height, 16_384)) {
            return json({ error: "width and height must be positive integers" }, 400);
          }
          if (!finite(body.fps) || body.fps <= 0 || body.fps > 1_000) {
            return json({ error: "fps must be > 0 and <= 1000" }, 400);
          }
          if (!nonEmpty(body.codec)) {
            return json({ error: "codec must be a non-empty string" }, 400);
          }
          if (
            body.segmentFrames !== undefined &&
            !positiveInt(body.segmentFrames, 10_000)
          ) {
            return json({ error: "segmentFrames must be a positive integer" }, 400);
          }
          if (
            body.warmupFrames !== undefined &&
            !nonNegativeInt(body.warmupFrames, 10_000)
          ) {
            return json({ error: "warmupFrames must be a non-negative integer" }, 400);
          }
          if (
            body.startFrame !== undefined &&
            !nonNegativeInt(body.startFrame)
          ) {
            return json({ error: "startFrame must be a non-negative integer" }, 400);
          }
          if (!nonEmpty(body.sceneHash) || !nonEmpty(body.renderHash)) {
            return json({ error: "sceneHash and renderHash are required" }, 400);
          }
          if (
            body.snapshotHash !== undefined &&
            typeof body.snapshotHash !== "string"
          ) {
            return json({ error: "snapshotHash must be a string" }, 400);
          }

          const now = Date.now();
          const id = crypto.randomUUID();
          const startFrame = (body.startFrame as number | undefined) ?? 0;
          const spec: StreamSpec = {
            id,
            capability: body.capability,
            width: body.width as number,
            height: body.height as number,
            fps: body.fps,
            codec: body.codec,
            segmentFrames: (body.segmentFrames as number | undefined) ?? 30,
            warmupFrames: (body.warmupFrames as number | undefined) ?? 8,
          };
          const currentEpoch: StreamEpoch = {
            streamId: id,
            id: 0,
            startFrame,
            sceneHash: body.sceneHash,
            renderHash: body.renderHash,
            snapshotHash: body.snapshotHash as string | undefined,
            createdAt: now,
          };
          const stream: StreamState = {
            spec,
            currentEpoch,
            nextFrame: startFrame,
            status: "active",
            createdAt: now,
            updatedAt: now,
          };
          streams.set(id, stream);
          return json(stream, 201);
        }
      }

      if (parts[0] === "streams" && parts[1]) {
        const streamId = parts[1];
        const stream = streams.get(streamId);
        if (!stream) return json({ error: "stream not found" }, 404);

        if (parts.length === 2 && request.method === "GET") {
          return json(stream);
        }

        if (parts[2] === "epochs" && request.method === "POST") {
          const body = (await request.json()) as {
            startFrame?: unknown;
            sceneHash?: unknown;
            renderHash?: unknown;
            snapshotHash?: unknown;
          };
          if (
            body.startFrame !== undefined &&
            !nonNegativeInt(body.startFrame)
          ) {
            return json({ error: "startFrame must be a non-negative integer" }, 400);
          }
          if (body.sceneHash !== undefined && !nonEmpty(body.sceneHash)) {
            return json({ error: "sceneHash must be a non-empty string" }, 400);
          }
          if (body.renderHash !== undefined && !nonEmpty(body.renderHash)) {
            return json({ error: "renderHash must be a non-empty string" }, 400);
          }
          if (
            body.snapshotHash !== undefined &&
            typeof body.snapshotHash !== "string"
          ) {
            return json({ error: "snapshotHash must be a string" }, 400);
          }

          const previous = stream.currentEpoch;
          const now = Date.now();
          const next: StreamEpoch = {
            streamId,
            id: previous.id + 1,
            startFrame: (body.startFrame as number | undefined) ?? stream.nextFrame,
            sceneHash: (body.sceneHash as string | undefined) ?? previous.sceneHash,
            renderHash: (body.renderHash as string | undefined) ?? previous.renderHash,
            snapshotHash:
              body.snapshotHash === undefined
                ? previous.snapshotHash
                : (body.snapshotHash as string),
            createdAt: now,
          };
          stream.currentEpoch = next;
          stream.nextFrame = next.startFrame;
          stream.updatedAt = now;
          invalidateOlderEpochs(streamId, next.id);
          schedule();
          return json(next, 201);
        }

        if (parts[2] === "segments" && request.method === "GET") {
          const rawEpoch = url.searchParams.get("epoch");
          const epoch =
            rawEpoch === null
              ? undefined
              : Number.parseInt(rawEpoch, 10);
          if (
            epoch !== undefined &&
            (!Number.isInteger(epoch) || epoch < 0)
          ) {
            return json({ error: "epoch must be a non-negative integer" }, 400);
          }
          return json(streamSegments(streamId, epoch));
        }

        if (parts[2] === "segments" && request.method === "POST") {
          if (stream.status !== "active") {
            return json({ error: "stream is not active" }, 409);
          }

          const body = (await request.json()) as {
            count?: unknown;
            startFrame?: unknown;
            frameCount?: unknown;
            warmupFrames?: unknown;
            snapshotHash?: unknown;
            snapshots?: unknown;
            deadlineAt?: unknown;
          };

          const count = (body.count as number | undefined) ?? 1;
          const frameCount =
            (body.frameCount as number | undefined) ?? stream.spec.segmentFrames;
          const warmupFrames =
            (body.warmupFrames as number | undefined) ?? stream.spec.warmupFrames;
          const startFrame =
            (body.startFrame as number | undefined) ?? stream.nextFrame;

          if (!positiveInt(count, 256)) {
            return json({ error: "count must be a positive integer <= 256" }, 400);
          }
          if (!positiveInt(frameCount, 10_000)) {
            return json({ error: "frameCount must be a positive integer" }, 400);
          }
          if (!nonNegativeInt(warmupFrames, 10_000)) {
            return json({ error: "warmupFrames must be a non-negative integer" }, 400);
          }
          if (!nonNegativeInt(startFrame)) {
            return json({ error: "startFrame must be a non-negative integer" }, 400);
          }
          if (body.deadlineAt !== undefined && !finite(body.deadlineAt)) {
            return json({ error: "deadlineAt must be a finite number" }, 400);
          }
          if (
            body.snapshotHash !== undefined &&
            typeof body.snapshotHash !== "string"
          ) {
            return json({ error: "snapshotHash must be a string" }, 400);
          }

          let snapshots: string[] | undefined;
          if (body.snapshots !== undefined) {
            if (
              !Array.isArray(body.snapshots) ||
              body.snapshots.length !== count ||
              !body.snapshots.every((value) => typeof value === "string")
            ) {
              return json({
                error: "snapshots must be a string array with one entry per segment",
              }, 400);
            }
            snapshots = body.snapshots as string[];
          }

          const epoch = stream.currentEpoch;
          const created: TaskState[] = [];
          const firstDeadline = body.deadlineAt as number | undefined;
          const segmentDurationMs = (frameCount / stream.spec.fps) * 1_000;

          for (let index = 0; index < count; index += 1) {
            const segmentStart = startFrame + index * frameCount;
            const segmentId = `${epoch.id}-${segmentStart}`;
            const snapshotHash =
              snapshots?.[index] ??
              (body.snapshotHash as string | undefined) ??
              epoch.snapshotHash;
            const input: RenderSegmentInput = {
              streamId,
              epoch: epoch.id,
              segmentId,
              startFrame: segmentStart,
              frameCount,
              warmupStartFrame: Math.max(
                epoch.startFrame,
                segmentStart - warmupFrames,
              ),
              warmupFrames,
              width: stream.spec.width,
              height: stream.spec.height,
              fps: stream.spec.fps,
              codec: stream.spec.codec,
              sceneHash: epoch.sceneHash,
              renderHash: epoch.renderHash,
              snapshotHash,
            };
            const state = createTask(stream.spec.capability, input, {
              cause: {
                streamId,
                epoch: epoch.id,
                segmentId,
                startFrame: segmentStart,
              },
              affinityKey: `${epoch.sceneHash}:${epoch.renderHash}`,
              deadlineAt:
                firstDeadline === undefined
                  ? undefined
                  : firstDeadline + index * segmentDurationMs,
              priority: 100,
            });
            enqueue(state);
            created.push(state);
          }

          stream.nextFrame = Math.max(
            stream.nextFrame,
            startFrame + count * frameCount,
          );
          stream.updatedAt = Date.now();
          schedule();
          return json(created, 202);
        }
      }

      if (request.method === "POST" && url.pathname.startsWith("/artifacts/")) {
        const [, , taskId, attemptId] = url.pathname.split("/");
        if (
          !taskId ||
          !attemptId ||
          !safeId(taskId) ||
          !safeId(attemptId)
        ) {
          return json({ error: "invalid artifact path" }, 400);
        }

        const state = tasks.get(taskId);
        if (
          !state ||
          state.status !== "running" ||
          state.attemptId !== attemptId
        ) {
          return json({ error: "stale or unknown task attempt" }, 409);
        }

        const bytes = new Uint8Array(await request.arrayBuffer());
        if (bytes.byteLength > 32 * 1024 * 1024) {
          return json({ error: "artifact exceeds 32 MiB" }, 413);
        }

        const contentType =
          request.headers.get("content-type") ?? "application/octet-stream";
        const extension =
          contentType === "image/png"
            ? "png"
            : contentType === "video/mp4"
              ? "m4s"
              : contentType === "video/webm"
                ? "webm"
                : contentType === "video/mp2t"
                  ? "ts"
                  : "bin";
        const taskDir = join(artifactDir, taskId);
        mkdirSync(taskDir, { recursive: true });
        const fileName = `${attemptId}.${extension}`;
        await Bun.write(join(taskDir, fileName), bytes);

        return json({
          url: new URL(`/artifacts/${taskId}/${fileName}`, request.url).toString(),
          contentType,
          bytes: bytes.byteLength,
        });
      }

      if (request.method === "GET" && url.pathname.startsWith("/artifacts/")) {
        const [, , taskId, fileName] = url.pathname.split("/");
        if (
          !taskId ||
          !fileName ||
          !safeId(taskId) ||
          !/^[a-zA-Z0-9.-]+$/.test(fileName)
        ) {
          return json({ error: "invalid artifact path" }, 400);
        }
        const file = Bun.file(join(artifactDir, taskId, fileName));
        return (await file.exists())
          ? new Response(file)
          : json({ error: "artifact not found" }, 404);
      }

      return new Response("Meshwork broker", { status: 200 });
    },
    websocket: {
      message: (socket, raw) => {
        const text =
          typeof raw === "string" ? raw : new TextDecoder().decode(raw);
        let message: WorkerToBroker;

        try {
          message = JSON.parse(text) as WorkerToBroker;
        } catch {
          socket.close(1003, "invalid json");
          return;
        }

        if (message.type === "worker.hello") {
          const previous = workers.get(message.worker.id);
          if (previous && previous.socket !== socket) {
            if (previous.taskId) {
              const state = tasks.get(previous.taskId);
              if (state?.status === "running") requeue(state);
            }
            if (previous.shardId) {
              const state = shards.get(previous.shardId);
              if (
                state?.status === "leased" &&
                state.workerId === previous.descriptor.id &&
                state.leaseId === previous.shardLeaseId
              ) {
                requeueShard(state, "worker reconnected");
              }
            }
            previous.socket.close(1012, "worker reconnected");
          }
          socket.data.workerId = message.worker.id;
          workers.set(message.worker.id, {
            descriptor: message.worker,
            socket,
            lastSeenAt: Date.now(),
          });
          schedule();
          return;
        }

        const workerId = socket.data.workerId;
        if (!workerId) return;
        const worker = workers.get(workerId);
        if (!worker || worker.socket !== socket) return;

        worker.lastSeenAt = Date.now();

        if (message.type === "worker.heartbeat") {
          if (
            worker.taskId &&
            worker.attemptId &&
            message.taskId === worker.taskId &&
            message.attemptId === worker.attemptId
          ) {
            const state = tasks.get(worker.taskId);
            if (
              state?.status === "running" &&
              state.attemptId === worker.attemptId
            ) {
              state.leaseUntil = Date.now() + leaseMs;
              state.updatedAt = Date.now();
            }
          }
          return;
        }

        if (message.type === "task.result") {
          completeTask(
            workerId,
            message.taskId,
            message.attemptId,
            { output: message.output },
          );
        } else if (message.type === "task.error") {
          completeTask(
            workerId,
            message.taskId,
            message.attemptId,
            { error: message.error },
          );
        } else if (message.type === "shard.heartbeat") {
          if (message.workerId !== workerId) return;
          renewShard(
            workerId,
            message.shardId,
            message.leaseId,
            message.epoch,
            message.tick,
          );
        } else if (message.type === "shard.publish") {
          const state = publishShard(
            workerId,
            message.shardId,
            message.leaseId,
            message.epoch,
            message.tick,
          );
          if (!state) return;
          state.publication = message.output;
          if (message.snapshotHash !== undefined) {
            state.snapshotHash = message.snapshotHash;
          }
          const current = workers.get(workerId);
          if (current && state.shard.affinityKey) {
            current.lastAffinityKey = state.shard.affinityKey;
          }
        } else if (message.type === "shard.error") {
          const state = renewShard(
            workerId,
            message.shardId,
            message.leaseId,
            message.epoch,
            message.tick,
          );
          if (!state) return;
          requeueShard(state, message.error);
          schedule();
        }
      },
      close: (socket) => {
        const workerId = socket.data.workerId;
        if (!workerId) return;
        const worker = workers.get(workerId);
        if (!worker || worker.socket !== socket) return;

        if (worker.taskId) {
          const state = tasks.get(worker.taskId);
          if (state?.status === "running") requeue(state);
        }
        if (worker.shardId) {
          const state = shards.get(worker.shardId);
          if (
            state?.status === "leased" &&
            state.workerId === workerId &&
            state.leaseId === worker.shardLeaseId
          ) {
            requeueShard(state, "worker disconnected");
          }
        }

        workers.delete(workerId);
        schedule();
      },
    },
  });

  const scheme = config.tls ? "https" : "http";
  console.log(`meshwork broker listening on ${scheme}://localhost:${server.port}`);

  const originalStop = server.stop.bind(server);
  return Object.assign(server, {
    stop(closeActiveConnections?: boolean) {
      clearInterval(reaper);
      return originalStop(closeActiveConnections);
    },
  });
}
