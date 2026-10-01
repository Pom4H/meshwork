import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { rm, readdir } from "node:fs/promises";
import { readScene, serveScene, validHash } from "./scenes";
import { playlist, playableSegments, splitMp4 } from "./playback";
import {addSceneAudio} from './audio';
import {LiveChat} from './live-chat';
import {codexDirector,validateSettings,type SceneDirector,type SceneSettings} from './director';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
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
  sceneDir?: string;
  accessToken?: string;
  hostname?: string;
  tls?: { certPath: string; keyPath: string };
  /** Trusted local restart snapshot; pending attempts are reconstructed. */
  initialStreams?: StreamState[];
  director?: SceneDirector;
  enableDirector?: boolean;
  chatDir?: string;
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
  const sceneDir = config.sceneDir ?? ".meshwork/scenes";
  const uploaded = new Map<
    string,
    { url: string; contentType: string; bytes: number }
  >();
  const rejectedWorkers = new Map<string, Set<string>>();

  mkdirSync(artifactDir, { recursive: true });

  const workers = new Map<string, ConnectedWorker>();
  const tasks = new Map<string, TaskState>();
  const streams = new Map<string, StreamState>();
  const shards = new Map<string, ShardState>();
  const pending: string[] = [];
  for(const saved of config.initialStreams??[]){
    const startFrame=saved.currentEpoch.startFrame;
    streams.set(saved.spec.id,{...structuredClone(saved),
      nextFrame:startFrame,playbackStartFrame:startFrame,mediaSequence:0,
      currentEpoch:{...saved.currentEpoch,id:saved.currentEpoch.id+1,createdAt:Date.now()},
      updatedAt:Date.now(),error:undefined});
  }

  const send = (worker: ConnectedWorker, message: BrokerToWorker) => {
    worker.socket.send(JSON.stringify(message));
  };

  const createTask = (
    capability: string,
    input: JsonValue,
    options: Pick<
      Task,
      "cause" | "affinityKey" | "deadlineAt" | "priority"
    > = {},
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

  const releaseWorker = (
    workerId: string,
    taskId: string,
    attemptId: string,
  ) => {
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
      const worker = workers.get(state.workerId);
      if (worker?.socket.readyState === 1)
        send(worker, {
          type: "task.cancel",
          taskId: state.task.id,
          attemptId: state.attemptId,
        });
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
        !rejectedWorkers.get(task.id)?.has(candidate.descriptor.id) &&
        (!(task.input as RenderSegmentInput)?.sceneChanges?.length || candidate.descriptor.capabilities.includes('scene.live-controls.v1')) &&
        (task.capability !== "render.video.segment.v1" ||
          candidate.descriptor.videoCodecs?.includes(
            (task.input as RenderSegmentInput).codec,
          )) &&
        candidate.descriptor.capabilities.includes(task.capability),
    );
    candidates.sort((a, b) => {
      const aAffinity = Number(
        task.affinityKey !== undefined &&
          a.lastAffinityKey === task.affinityKey,
      );
      const bAffinity = Number(
        task.affinityKey !== undefined &&
          b.lastAffinityKey === task.affinityKey,
      );
      return bAffinity - aAffinity || b.lastSeenAt - a.lastSeenAt;
    });
    return candidates[0];
  };

  const schedule = () => {
    scheduleShards();
    refillLive();
    pending.sort(pendingOrder);

    for (let index = 0; index < pending.length;) {
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
    if (
      state.task.capability === "render.video.segment.v1" &&
      "output" in result
    ) {
      const receipt = uploaded.get(`${taskId}:${attemptId}`);
      const output = result.output as {
        artifact?: { url?: string };
        frameCount?: number;
      } | null;
      if (
        !receipt ||
        receipt.contentType !== "video/mp4" ||
        output?.artifact?.url !== receipt.url ||
        output.frameCount !==
          (state.task.input as RenderSegmentInput).frameCount
      ) {
        result = {
          error:
            "Video result must reference this attempt's validated MP4 and exact frame count",
        };
      }
    }
    if ("output" in result) {
      state.status = "completed";
      state.output = result.output;
      state.error = undefined;
      if (worker && state.task.affinityKey) {
        worker.lastAffinityKey = state.task.affinityKey;
      }
    } else {
      if (
        state.task.capability === "render.video.segment.v1" &&
        state.attempts < 3
      ) {
        const rejected = rejectedWorkers.get(taskId) ?? new Set<string>();
        rejected.add(workerId);
        rejectedWorkers.set(taskId, rejected);
        const available = [...workers.values()].some(
          (w) =>
            !rejected.has(w.descriptor.id) &&
            w.descriptor.capabilities.includes(state.task.capability) &&
            w.descriptor.videoCodecs?.includes(
              (state.task.input as RenderSegmentInput).codec,
            ),
        );
        if (available) {
          requeue(state);
          schedule();
          return;
        }
      }
      state.status = "failed";
      state.error = result.error;
      const stream = state.task.cause && streams.get(state.task.cause.streamId);
      if (stream?.spec.live) {
        stream.status = "stopped";
        stream.error = result.error;
        for (const state of streamSegments(
          stream.spec.id,
          stream.currentEpoch.id,
        ))
          cancelTask(state, "stream failed");
      }
    }

    state.leaseUntil = undefined;
    state.updatedAt = Date.now();
    releaseWorker(workerId, taskId, attemptId);
    pruneLive();
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

  const createSegment = (
    stream: StreamState,
    startFrame: number,
    frameCount: number,
    warmupFrames: number,
    snapshotHash?: string,
    deadlineAt?: number,
  ) => {
    const epoch = stream.currentEpoch,
      segmentId = `${epoch.id}-${startFrame}`;
    const input: RenderSegmentInput = {
      streamId: stream.spec.id,
      epoch: epoch.id,
      segmentId,
      startFrame,
      frameCount,
      warmupStartFrame: Math.max(0, startFrame - warmupFrames),
      warmupFrames,
      width: stream.spec.width,
      height: stream.spec.height,
      fps: stream.spec.fps,
      codec: stream.spec.codec,
      sceneHash: epoch.sceneHash,
      renderHash: epoch.renderHash,
      ...(stream.sceneChanges?.length?{sceneChanges:structuredClone(stream.sceneChanges)}:{}),
      snapshotHash,
    };
    const state = createTask(stream.spec.capability, input, {
      cause: {
        streamId: stream.spec.id,
        epoch: epoch.id,
        segmentId,
        startFrame,
      },
      affinityKey: `${epoch.sceneHash}:${epoch.renderHash}`,
      deadlineAt,
      priority: 100,
    });
    enqueue(state);
    return state;
  };

  const refillLive = () => {
    for (const stream of streams.values()) {
      if (!stream.spec.live || stream.status !== "active") continue;
      const segments = streamSegments(stream.spec.id, stream.currentEpoch.id);
      if (segments.some((s) => s.status === "failed")) continue;
      const eligible = [...workers.values()].filter(
        (w) =>
          w.descriptor.capabilities.includes(stream.spec.capability) &&
          (!stream.sceneChanges?.length||w.descriptor.capabilities.includes('scene.live-controls.v1')) &&
          w.descriptor.videoCodecs?.includes(stream.spec.codec),
      ).length;
      const ahead = Math.min(32, Math.max(2, eligible * 2));
      // Count completed results blocked behind a missing range as outstanding too.
      // Otherwise a slow or missing worker would create unbounded queued work.
      const published = playableSegments(stream, segments).length;
      const outstanding =
        segments.filter((s) => s.status !== "cancelled").length - published;
      for (let n = outstanding; n < ahead; n++) {
        createSegment(
          stream,
          stream.nextFrame,
          stream.spec.segmentFrames,
          stream.spec.warmupFrames,
          stream.currentEpoch.snapshotHash,
          Date.now() +
            (((n + 1) * stream.spec.segmentFrames) / stream.spec.fps) * 1000,
        );
        stream.nextFrame += stream.spec.segmentFrames;
      }
    }
  };

  const pruneLive = () => {
    for (const stream of streams.values()) {
      if (!stream.spec.live) continue;
      const contiguous = playableSegments(
        stream,
        streamSegments(stream.spec.id, stream.currentEpoch.id),
      );
      for (const state of contiguous.slice(
        0,
        Math.max(0, contiguous.length - 24),
      )) {
        const input = state.task.input as RenderSegmentInput;
        stream.playbackStartFrame = input.startFrame + input.frameCount;
        stream.mediaSequence = (stream.mediaSequence ?? 0) + 1;
        tasks.delete(state.task.id);
        rejectedWorkers.delete(state.task.id);
        for (const key of uploaded.keys())
          if (key.startsWith(state.task.id + ":")) uploaded.delete(key);
        void rm(join(artifactDir, state.task.id), {
          recursive: true,
          force: true,
        }).catch(() => {});
      }
    }
  };

  const reap = () => {
    const now = Date.now();

    for (const [workerId, worker] of workers) {
      if (now - worker.lastSeenAt <= heartbeatTimeoutMs) {
        // A server-driven heartbeat does not depend on background page timers.
        send(worker, { type: 'worker.ping' });
        continue;
      }
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

  const chat=new LiveChat(config.chatDir??'.meshwork/chat',config.director??(config.enableDirector?codexDirector():undefined),async id=>{
    const stream=streams.get(id)!;
    const manifest=await readScene(sceneDir,stream.currentEpoch.sceneHash);
    const source=manifest?(await import(pathToFileURL(resolve(sceneDir,manifest.sceneHash,manifest.entry)).href)).default:undefined;
    const last=stream.sceneChanges?.at(-1)?.values??{};
    return {scene:manifest?.name,controls:{snowfall:source?.environment?.snowfall,wind:source?.environment?.wind,lightScale:1,audioGain:1,cameraSpeed:1,...last,rendering:{...source?.rendering,...last.rendering}},recentMessages:chat.snapshot(id).messages.slice(-8)};
  },(id,raw)=>{
    const stream=streams.get(id)!;
    if(stream.status!=='active')throw Error('Stream stopped');
    const patch=validateSettings(raw),last=stream.sceneChanges?.at(-1);
    const values:SceneSettings={...last?.values,...patch,rendering:{...last?.values.rendering,...patch.rendering}};
    const existing=streamSegments(id,stream.currentEpoch.id);
    const boundary=Math.max(stream.currentEpoch.startFrame,...existing.filter(s=>s.status==='running'||s.status==='completed').map(s=>{const input=s.task.input as RenderSegmentInput;return input.startFrame+input.frameCount;}));
    const change={revision:(last?.revision??0)+1,startFrame:boundary,transitionFrames:stream.spec.fps,values};
    const changes=stream.sceneChanges??=[];changes.push(change);
    if(changes.length>64){changes.shift();changes[0]={...changes[0]!,startFrame:0,transitionFrames:1};}
    // Revisions of unleased future work can change without invalidating any
    // submitted frame or interrupting a worker already rendering a segment.
    for(const state of existing)if(state.status==='pending'&&(state.task.input as RenderSegmentInput).startFrame>=boundary)(state.task.input as RenderSegmentInput).sceneChanges=structuredClone(changes);
    stream.updatedAt=Date.now();return change;
  });
  const viewerRate=new Map<string,number>();

  const server = Bun.serve<SocketData>({
    idleTimeout:60,
    port,
    hostname: config.hostname,
    maxRequestBodySize: 32 * 1024 * 1024,
    tls: config.tls
      ? {
          cert: Bun.file(config.tls.certPath),
          key: Bun.file(config.tls.keyPath),
        }
      : undefined,
    fetch: async (request, bunServer) => {
      try {
        const url = new URL(request.url);
        const parts = url.pathname.split("/").filter(Boolean);

        if (config.accessToken) {
          if (
            request.method === "GET" &&
            url.searchParams.get("access_token") === config.accessToken
          ) {
            url.searchParams.delete("access_token");
            return new Response(null, {
              status: 303,
              headers: {
                location: url.pathname + url.search,
                "set-cookie": `meshwork_access=${config.accessToken}; Path=/; HttpOnly; SameSite=Strict${url.protocol === "https:" || request.headers.get("x-forwarded-proto") === "https" ? "; Secure" : ""}`,
                "cache-control": "no-store",
                "referrer-policy": "no-referrer",
              },
            });
          }
          const cookie = request.headers
            .get("cookie")
            ?.split(";")
            .some((v) => v.trim() === `meshwork_access=${config.accessToken}`);
          const bearer =
            request.headers.get("authorization") ===
            `Bearer ${config.accessToken}`;
          if (!cookie && !bearer)
            return json({ error: "Access token required" }, 401);
          const origin = request.headers.get("origin");
          if (cookie && origin && new URL(origin).host !== url.host)
            return json({ error: "Cross-origin request rejected" }, 403);
        }

        if (url.pathname === "/ws") {
          if (bunServer.upgrade(request, { data: {} })) return;
          return new Response("WebSocket upgrade failed", { status: 400 });
        }

        if (request.method === "GET" && url.pathname === "/") {
          return new Response(
            Bun.file(new URL("../web/index.html", import.meta.url)),
          );
        }

        if (request.method === "GET" && url.pathname === "/worker.js") {
          return new Response(
            Bun.file(new URL("../web/worker.js", import.meta.url)),
            {
              headers: { "content-type": "text/javascript; charset=utf-8" },
            },
          );
        }

        const webModules: Record<string, string> = {
          "/render-video.js": "../web/render-video.js",
          "/worker-connection.js": "../web/worker-connection.js",
          "/player.js": "../web/player.js",
          "/live-scene.js": "../web/live-scene.js",
          "/live-chat.js": "../web/live-chat.js",
          "/vendor/mediabunny.js":
            "../node_modules/mediabunny/dist/bundles/mediabunny.min.mjs",
          "/vendor/hls.js": "../node_modules/hls.js/dist/hls.mjs",
        };
        if (request.method === "GET" && webModules[url.pathname])
          return new Response(
            Bun.file(new URL(webModules[url.pathname]!, import.meta.url)),
            { headers: { "content-type": "text/javascript" } },
          );
        if (
          request.method === "GET" &&
          /^\/watch\/[a-zA-Z0-9-]+$/.test(url.pathname)
        )
          return new Response(
            Bun.file(new URL("../web/player.html", import.meta.url)),
          );
        if (request.method === "GET" && parts[0] === "scenes") {
          if (parts[1]) {
            const response = await serveScene(
              sceneDir,
              parts[1],
              parts.slice(2).join("/"),
            );
            if (config.accessToken)
              response.headers.set(
                "cache-control",
                "private,max-age=31536000,immutable",
              );
            return response;
          }
          const names = await readdir(sceneDir).catch(() => [] as string[]);
          const scenes = await Promise.all(
            names.filter(validHash).map((hash) => readScene(sceneDir, hash)),
          );
          return json(scenes.filter(Boolean));
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
              busy: worker.taskId !== undefined || worker.shardId !== undefined,
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
            return json(
              { error: "capability must be a non-empty string" },
              400,
            );
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
              live?: unknown;
            };

            if (!nonEmpty(body.capability)) {
              return json(
                { error: "capability must be a non-empty string" },
                400,
              );
            }
            if (
              !positiveInt(body.width, 16_384) ||
              !positiveInt(body.height, 16_384)
            ) {
              return json(
                { error: "width and height must be positive integers" },
                400,
              );
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
              return json(
                { error: "segmentFrames must be a positive integer" },
                400,
              );
            }
            if (
              body.warmupFrames !== undefined &&
              !nonNegativeInt(body.warmupFrames, 10_000)
            ) {
              return json(
                { error: "warmupFrames must be a non-negative integer" },
                400,
              );
            }
            if (
              body.startFrame !== undefined &&
              !nonNegativeInt(body.startFrame)
            ) {
              return json(
                { error: "startFrame must be a non-negative integer" },
                400,
              );
            }
            if (!nonEmpty(body.sceneHash) || !nonEmpty(body.renderHash)) {
              return json(
                { error: "sceneHash and renderHash are required" },
                400,
              );
            }
            if (body.live !== undefined && typeof body.live !== "boolean")
              return json({ error: "live must be a boolean" }, 400);
            if (body.live && body.capability !== "render.video.segment.v1")
              return json(
                { error: "live streams require render.video.segment.v1" },
                400,
              );
            if (body.capability === "render.video.segment.v1") {
              const scene = await readScene(sceneDir, body.sceneHash);
              if (!scene || scene.renderHash !== body.renderHash)
                return json(
                  { error: "Register a matching scene/renderer bundle first" },
                  400,
                );
              if (!["avc", "hevc"].includes(body.codec))
                return json({ error: "Video codec must be avc or hevc" }, 400);
              if (
                (body.width as number) > 4096 ||
                (body.height as number) > 4096 ||
                (body.width as number) % 2 ||
                (body.height as number) % 2 ||
                body.fps > 120
              )
                return json(
                  {
                    error:
                      "Video requires even dimensions <= 4096 and fps <= 120",
                  },
                  400,
                );
              if (body.snapshotHash)
                return json(
                  {
                    error:
                      "This engine adapter uses deterministic replay; snapshot restore is not supported",
                  },
                  400,
                );
              if (((body.segmentFrames as number | undefined) ?? 30) > 240)
                return json(
                  {
                    error:
                      "Video segmentFrames must be <= 240 to bound device memory",
                  },
                  400,
                );
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
              live: body.live as boolean | undefined,
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
            schedule();
            return json(stream, 201);
          }
        }

        if (parts[0] === "streams" && parts[1]) {
          const streamId = parts[1];
          const stream = streams.get(streamId);
          if (!stream) return json({ error: "stream not found" }, 404);

          if(parts[2]==='events'&&request.method==='GET')return chat.events(streamId,request.signal);
          if(parts[2]==='chat'&&request.method==='GET')return json(chat.snapshot(streamId));
          if((parts[2]==='chat'||parts[2]==='reactions')&&request.method==='POST'){
            const origin=request.headers.get('origin');
            if(origin&&new URL(origin).host!==url.host)return json({error:'Cross-origin request rejected'},403);
            if(Number(request.headers.get('content-length')??0)>4096)return json({error:'Message too large'},413);
            const raw=await request.text();if(raw.length>4096)return json({error:'Message too large'},413);
            const body=JSON.parse(raw);
            const client=typeof body.clientId==='string'&&/^[a-zA-Z0-9-]{1,80}$/.test(body.clientId)?body.clientId:request.headers.get('cf-connecting-ip')??bunServer.requestIP(request)?.address??'anonymous';
            const key=streamId+':'+parts[2]+':'+client,now=Date.now(),minimum=parts[2]==='chat'?3000:250;
            if(now-(viewerRate.get(key)??0)<minimum)return json({error:'Подожди немного перед следующим сообщением.'},429);
            viewerRate.set(key,now);if(viewerRate.size>10000)for(const [k,t] of viewerRate)if(now-t>60000)viewerRate.delete(k);
            try{return json(parts[2]==='chat'?chat.submit(streamId,String(body.name??'Гость'),String(body.text??'')):chat.reaction(streamId,String(body.emoji)),202);}
            catch(error){return json({error:String(error)},400);}
          }

          if (parts.length === 2 && request.method === "GET") {
            return json(stream);
          }
          if (parts[2] === "playlist.m3u8" && request.method === "GET") {
            const epoch = url.searchParams.get("epoch");
            if (epoch !== null && Number(epoch) !== stream.currentEpoch.id)
              return json(
                { error: "Stream epoch changed; reload the player" },
                409,
              );
            return new Response(
              playlist(
                stream,
                streamSegments(streamId, stream.currentEpoch.id),
              ),
              {
                headers: {
                  "content-type": "application/vnd.apple.mpegurl",
                  "cache-control": "no-store",
                },
              },
            );
          }
          if (parts[2] === "stop" && request.method === "POST") {
            stream.status = "stopped";
            stream.updatedAt = Date.now();
            for (const state of streamSegments(
              streamId,
              stream.currentEpoch.id,
            ))
              if (state.status === "pending" || state.status === "running")
                cancelTask(state, "stream stopped");
            schedule();
            return json(stream);
          }

          if (parts[2] === "epochs" && request.method === "POST") {
            if (stream.status !== "active")
              return json({ error: "stream is not active" }, 409);
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
              return json(
                { error: "startFrame must be a non-negative integer" },
                400,
              );
            }
            if (body.sceneHash !== undefined && !nonEmpty(body.sceneHash)) {
              return json(
                { error: "sceneHash must be a non-empty string" },
                400,
              );
            }
            if (body.renderHash !== undefined && !nonEmpty(body.renderHash)) {
              return json(
                { error: "renderHash must be a non-empty string" },
                400,
              );
            }
            if (
              body.snapshotHash !== undefined &&
              typeof body.snapshotHash !== "string"
            ) {
              return json({ error: "snapshotHash must be a string" }, 400);
            }

            const previous = stream.currentEpoch;
            if (stream.spec.capability === "render.video.segment.v1") {
              const scene = await readScene(
                sceneDir,
                (body.sceneHash as string | undefined) ?? previous.sceneHash,
              );
              if (
                !scene ||
                scene.renderHash !==
                  ((body.renderHash as string | undefined) ??
                    previous.renderHash) ||
                body.snapshotHash
              )
                return json(
                  { error: "Invalid scene bundle or unsupported snapshot" },
                  400,
                );
            }
            const now = Date.now();
            const next: StreamEpoch = {
              streamId,
              id: previous.id + 1,
              startFrame:
                (body.startFrame as number | undefined) ?? stream.nextFrame,
              sceneHash:
                (body.sceneHash as string | undefined) ?? previous.sceneHash,
              renderHash:
                (body.renderHash as string | undefined) ?? previous.renderHash,
              snapshotHash:
                body.snapshotHash === undefined
                  ? previous.snapshotHash
                  : (body.snapshotHash as string),
              createdAt: now,
            };
            stream.currentEpoch = next;
            stream.nextFrame = next.startFrame;
            stream.playbackStartFrame = next.startFrame;
            stream.mediaSequence = 0;
            stream.updatedAt = now;
            invalidateOlderEpochs(streamId, next.id);
            schedule();
            return json(next, 201);
          }

          if (parts[2] === "segments" && request.method === "GET") {
            const rawEpoch = url.searchParams.get("epoch");
            const epoch =
              rawEpoch === null ? undefined : Number.parseInt(rawEpoch, 10);
            if (
              epoch !== undefined &&
              (!Number.isInteger(epoch) || epoch < 0)
            ) {
              return json(
                { error: "epoch must be a non-negative integer" },
                400,
              );
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
              (body.frameCount as number | undefined) ??
              stream.spec.segmentFrames;
            const warmupFrames =
              (body.warmupFrames as number | undefined) ??
              stream.spec.warmupFrames;
            const startFrame =
              (body.startFrame as number | undefined) ?? stream.nextFrame;

            if (!positiveInt(count, 256)) {
              return json(
                { error: "count must be a positive integer <= 256" },
                400,
              );
            }
            if (!positiveInt(frameCount, 10_000)) {
              return json(
                { error: "frameCount must be a positive integer" },
                400,
              );
            }
            if (!nonNegativeInt(warmupFrames, 10_000)) {
              return json(
                { error: "warmupFrames must be a non-negative integer" },
                400,
              );
            }
            if (!nonNegativeInt(startFrame)) {
              return json(
                { error: "startFrame must be a non-negative integer" },
                400,
              );
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
                return json(
                  {
                    error:
                      "snapshots must be a string array with one entry per segment",
                  },
                  400,
                );
              }
              snapshots = body.snapshots as string[];
            }

            const epoch = stream.currentEpoch;
            if (startFrame < epoch.startFrame)
              return json(
                { error: "Segment starts before the current epoch" },
                400,
              );
            if (
              stream.spec.capability === "render.video.segment.v1" &&
              frameCount !== stream.spec.segmentFrames
            )
              return json(
                {
                  error: "Video frameCount must match the stream segmentFrames",
                },
                400,
              );
            if (
              stream.spec.capability === "render.video.segment.v1" &&
              (body.snapshotHash || snapshots?.some(Boolean))
            )
              return json(
                {
                  error:
                    "Snapshot restore is not supported by this engine adapter",
                },
                400,
              );
            const endFrame = startFrame + count * frameCount;
            if (
              !Number.isSafeInteger(endFrame) ||
              streamSegments(streamId, epoch.id).some((s) => {
                const i = s.task.input as RenderSegmentInput;
                return (
                  s.status !== "cancelled" &&
                  i.startFrame < endFrame &&
                  i.startFrame + i.frameCount > startFrame
                );
              })
            )
              return json({ error: "Segment ranges must not overlap" }, 409);
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
                  ...(stream.sceneChanges?.length?{sceneChanges:structuredClone(stream.sceneChanges)}:{}),
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

        if (
          request.method === "POST" &&
          url.pathname.startsWith("/artifacts/")
        ) {
          const [, , taskId, attemptId] = url.pathname.split("/");
          if (!taskId || !attemptId || !safeId(taskId) || !safeId(attemptId)) {
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

          let bytes:Uint8Array = new Uint8Array(await request.arrayBuffer());
          if (bytes.byteLength > 32 * 1024 * 1024) {
            return json({ error: "artifact exceeds 32 MiB" }, 413);
          }

          const contentType =
            request.headers.get("content-type") ?? "application/octet-stream";
          if (state.task.capability === "render.video.segment.v1") {
            if (contentType !== "video/mp4")
              return json(
                { error: "Video segments must be fragmented MP4" },
                415,
              );
            try {
              splitMp4(bytes);
            } catch (error) {
              return json({ error: String(error) }, 400);
            }
          }
          if (state.status !== "running" || state.attemptId !== attemptId)
            return json({ error: "stale task attempt" }, 409);
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
          if(state.task.capability === 'render.video.segment.v1'){
            try{bytes=await addSceneAudio(bytes,state.task.input as RenderSegmentInput,sceneDir,taskDir);}
            catch(error){return json({error:String(error)},500);}
            if(state.status!=='running'||state.attemptId!==attemptId)return json({error:'stale task attempt'},409);
          }
          const fileName = `${attemptId}.${extension}`;
          await Bun.write(join(taskDir, fileName), bytes);
          if (state.status !== "running" || state.attemptId !== attemptId)
            return json({ error: "stale task attempt" }, 409);
          const receipt = {
            url: new URL(
              `/artifacts/${taskId}/${fileName}`,
              request.url,
            ).toString(),
            contentType,
            bytes: bytes.byteLength,
          };
          uploaded.set(`${taskId}:${attemptId}`, receipt);
          return json(receipt);
        }

        if (
          request.method === "GET" &&
          url.pathname.startsWith("/artifacts/")
        ) {
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
          if (!(await file.exists()))
            return json({ error: "artifact not found" }, 404);
          const part = url.searchParams.get("part");
          if (part) {
            if (part !== "init" && part !== "media")
              return json({ error: "Invalid MP4 part" }, 400);
            const split = splitMp4(new Uint8Array(await file.arrayBuffer()));
            return new Response(split[part], {
              headers: {
                "content-type": "video/mp4",
                "cache-control": `${config.accessToken ? "private" : "public"},max-age=31536000,immutable`,
              },
            });
          }
          return new Response(file, {
            headers: fileName.endsWith(".m4s")
              ? { "content-type": "video/mp4" }
              : undefined,
          });
        }

        return new Response("Not found", { status: 404 });
      } catch (error) {
        return json(
          { error: error instanceof Error ? error.message : String(error) },
          400,
        );
      }
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
          if (
            !message.worker ||
            !safeId(message.worker.id) ||
            typeof message.worker.name !== "string" ||
            typeof message.worker.platform !== "string" ||
            !Array.isArray(message.worker.capabilities) ||
            !message.worker.capabilities.every((c) => typeof c === "string") ||
            (message.worker.videoCodecs !== undefined &&
              (!Array.isArray(message.worker.videoCodecs) ||
                !message.worker.videoCodecs.every(
                  (c) => c === "avc" || c === "hevc",
                )))
          ) {
            socket.close(1003, "invalid worker descriptor");
            return;
          }
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
          completeTask(workerId, message.taskId, message.attemptId, {
            output: message.output,
          });
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
      close: (socket, code, reason) => {
        const workerId = socket.data.workerId;
        if (!workerId) return;
        const worker = workers.get(workerId);
        if (!worker || worker.socket !== socket) return;
        console.log(JSON.stringify({event:'worker.disconnected',workerId,
          code,reason,heartbeatAgeMs:Date.now()-worker.lastSeenAt,time:Date.now()}));

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
  console.log(
    `meshwork broker listening on ${scheme}://localhost:${server.port}`,
  );

  const originalStop = server.stop.bind(server);
  return Object.assign(server, {
    stop(closeActiveConnections?: boolean) {
      chat.close();
      clearInterval(reaper);
      return originalStop(closeActiveConnections);
    },
  });
}
