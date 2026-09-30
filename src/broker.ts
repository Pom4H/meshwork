import { mkdirSync } from "node:fs";
import { join } from "node:path";
import type { ServerWebSocket } from "bun";
import type {
  BrokerToWorker,
  JsonValue,
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
  lastSeenAt: number;
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
  const pending: string[] = [];

  const send = (worker: ConnectedWorker, message: BrokerToWorker) => {
    worker.socket.send(JSON.stringify(message));
  };

  const enqueue = (state: TaskState, front = false) => {
    if (state.status === "pending" && !pending.includes(state.task.id)) {
      if (front) pending.unshift(state.task.id);
      else pending.push(state.task.id);
    }
  };

  const releaseWorker = (workerId: string, taskId: string, attemptId: string) => {
    const worker = workers.get(workerId);
    if (
      worker?.taskId === taskId &&
      worker.attemptId === attemptId
    ) {
      worker.taskId = undefined;
      worker.attemptId = undefined;
    }
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

  const schedule = () => {
    for (let index = 0; index < pending.length; ) {
      const taskId = pending[index];
      const state = taskId ? tasks.get(taskId) : undefined;

      if (!taskId || !state || state.status !== "pending") {
        pending.splice(index, 1);
        continue;
      }

      const worker = [...workers.values()].find(
        (candidate) =>
          candidate.taskId === undefined &&
          candidate.descriptor.capabilities.includes(state.task.capability),
      );

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

    if ("output" in result) {
      state.status = "completed";
      state.output = result.output;
      state.error = undefined;
    } else {
      state.status = "failed";
      state.error = result.error;
    }

    state.leaseUntil = undefined;
    state.updatedAt = Date.now();
    releaseWorker(workerId, taskId, attemptId);
    schedule();
  };

  const reap = () => {
    const now = Date.now();

    for (const [workerId, worker] of workers) {
      if (now - worker.lastSeenAt <= heartbeatTimeoutMs) continue;
      if (worker.taskId) {
        const state = tasks.get(worker.taskId);
        if (state?.status === "running") requeue(state);
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

    schedule();
  };

  const reaper = setInterval(reap, Math.max(1_000, Math.floor(heartbeatTimeoutMs / 3)));

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
        return json({ ok: true, workers: workers.size, tasks: tasks.size });
      }

      if (request.method === "GET" && url.pathname === "/workers") {
        return json(
          [...workers.values()].map((worker) => ({
            ...worker.descriptor,
            busy: worker.taskId !== undefined,
            taskId: worker.taskId,
            attemptId: worker.attemptId,
            lastSeenAt: worker.lastSeenAt,
          })),
        );
      }

      if (request.method === "POST" && url.pathname === "/tasks") {
        const body = (await request.json()) as {
          capability?: unknown;
          input?: JsonValue;
        };

        if (typeof body.capability !== "string" || body.capability.length === 0) {
          return json({ error: "capability must be a non-empty string" }, 400);
        }

        const now = Date.now();
        const task: Task = {
          id: crypto.randomUUID(),
          capability: body.capability,
          input: body.input ?? null,
        };
        const state: TaskState = {
          task,
          status: "pending",
          attempts: 0,
          createdAt: now,
          updatedAt: now,
        };
        tasks.set(task.id, state);
        enqueue(state);
        schedule();
        return json(state, 202);
      }

      if (request.method === "GET" && url.pathname.startsWith("/tasks/")) {
        const taskId = url.pathname.slice("/tasks/".length);
        const state = tasks.get(taskId);
        return state ? json(state) : json({ error: "task not found" }, 404);
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

        const contentType = request.headers.get("content-type") ?? "application/octet-stream";
        const extension = contentType === "image/png" ? "png" : "bin";
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

        workers.delete(workerId);
        schedule();
      },
    },
  });

  const scheme = config.tls ? "https" : "http";
  console.log(`meshwork broker listening on ${scheme}://localhost:${server.port}`);

  return {
    ...server,
    stop(closeActiveConnections?: boolean) {
      clearInterval(reaper);
      return server.stop(closeActiveConnections);
    },
  };
}
