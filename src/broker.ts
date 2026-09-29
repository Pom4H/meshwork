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
  lastSeenAt: number;
};

const json = (value: unknown, status = 200) =>
  Response.json(value, { status, headers: { "cache-control": "no-store" } });

export function startBroker(port = 8787) {
  const workers = new Map<string, ConnectedWorker>();
  const tasks = new Map<string, TaskState>();
  const pending: string[] = [];

  const send = (worker: ConnectedWorker, message: BrokerToWorker) => {
    worker.socket.send(JSON.stringify(message));
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

      worker.taskId = taskId;
      state.status = "running";
      state.workerId = worker.descriptor.id;
      pending.splice(index, 1);
      send(worker, { type: "task.assign", task: state.task });
    }
  };

  const releaseWorker = (workerId: string, taskId: string) => {
    const worker = workers.get(workerId);
    if (worker?.taskId === taskId) worker.taskId = undefined;
  };

  const completeTask = (
    workerId: string,
    taskId: string,
    result: { output: JsonValue } | { error: string },
  ) => {
    const state = tasks.get(taskId);
    if (!state || state.workerId !== workerId || state.status !== "running") return;

    if ("output" in result) {
      state.status = "completed";
      state.output = result.output;
    } else {
      state.status = "failed";
      state.error = result.error;
    }

    releaseWorker(workerId, taskId);
    schedule();
  };

  const server = Bun.serve<SocketData>({
    port,
    fetch: async (request, bunServer) => {
      const url = new URL(request.url);

      if (url.pathname === "/ws") {
        if (bunServer.upgrade(request, { data: {} })) return;
        return new Response("WebSocket upgrade failed", { status: 400 });
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

        const task: Task = {
          id: crypto.randomUUID(),
          capability: body.capability,
          input: body.input ?? null,
        };
        const state: TaskState = { task, status: "pending" };
        tasks.set(task.id, state);
        pending.push(task.id);
        schedule();
        return json(state, 202);
      }

      if (request.method === "GET" && url.pathname.startsWith("/tasks/")) {
        const taskId = url.pathname.slice("/tasks/".length);
        const state = tasks.get(taskId);
        return state ? json(state) : json({ error: "task not found" }, 404);
      }

      return new Response("Meshwork broker", { status: 200 });
    },
    websocket: {
      message: (socket, raw) => {
        const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
        let message: WorkerToBroker;

        try {
          message = JSON.parse(text) as WorkerToBroker;
        } catch {
          socket.close(1003, "invalid json");
          return;
        }

        if (message.type === "worker.hello") {
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
        if (!worker) return;

        worker.lastSeenAt = Date.now();

        if (message.type === "worker.heartbeat") return;
        if (message.type === "task.result") {
          completeTask(workerId, message.taskId, { output: message.output });
        } else if (message.type === "task.error") {
          completeTask(workerId, message.taskId, { error: message.error });
        }
      },
      close: (socket) => {
        const workerId = socket.data.workerId;
        if (!workerId) return;
        const worker = workers.get(workerId);
        if (!worker || worker.socket !== socket) return;

        if (worker.taskId) {
          const state = tasks.get(worker.taskId);
          if (state?.status === "running") {
            state.status = "pending";
            state.workerId = undefined;
            pending.unshift(state.task.id);
          }
        }

        workers.delete(workerId);
        schedule();
      },
    },
  });

  console.log(`meshwork broker listening on http://localhost:${server.port}`);
  return server;
}
