import { hostname } from "node:os";
import type {
  BrokerToWorker,
  JsonValue,
  WorkerDescriptor,
  WorkerToBroker,
} from "./protocol";

export type CapabilityHandler = (input: JsonValue) => JsonValue | Promise<JsonValue>;

export type WorkerOptions = {
  brokerUrl?: string;
  id?: string;
  name?: string;
  handlers?: Record<string, CapabilityHandler>;
};

const defaultHandlers: Record<string, CapabilityHandler> = {
  echo: (input) => input,
  "system.info": () => ({
    platform: process.platform,
    arch: process.arch,
    bun: Bun.version,
    hostname: hostname(),
  }),
};

export function startWorker(options: WorkerOptions = {}) {
  const brokerUrl = options.brokerUrl ?? "ws://localhost:8787/ws";
  const handlers = { ...defaultHandlers, ...options.handlers };
  const worker: WorkerDescriptor = {
    id: options.id ?? `${hostname()}-${crypto.randomUUID().slice(0, 8)}`,
    name: options.name ?? hostname(),
    platform: process.platform,
    capabilities: Object.keys(handlers).sort(),
  };

  let socket: WebSocket | undefined;
  let heartbeat: Timer | undefined;
  let stopped = false;

  const send = (message: WorkerToBroker) => {
    if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
  };

  const connect = () => {
    if (stopped) return;
    socket = new WebSocket(brokerUrl);

    socket.addEventListener("open", () => {
      send({ type: "worker.hello", worker });
      heartbeat = setInterval(
        () => send({ type: "worker.heartbeat", workerId: worker.id }),
        5_000,
      );
      console.log(
        `meshwork worker ${worker.name} connected (${worker.capabilities.join(", ")})`,
      );
    });

    socket.addEventListener("message", async (event) => {
      let message: BrokerToWorker;
      try {
        message = JSON.parse(String(event.data)) as BrokerToWorker;
      } catch {
        return;
      }

      if (message.type !== "task.assign") return;
      const handler = handlers[message.task.capability];
      if (!handler) {
        send({
          type: "task.error",
          taskId: message.task.id,
          error: `capability not available: ${message.task.capability}`,
        });
        return;
      }

      try {
        const output = await handler(message.task.input);
        send({ type: "task.result", taskId: message.task.id, output });
      } catch (error) {
        send({
          type: "task.error",
          taskId: message.task.id,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    socket.addEventListener("close", () => {
      if (heartbeat) clearInterval(heartbeat);
      heartbeat = undefined;
      if (!stopped) setTimeout(connect, 1_000);
    });
  };

  connect();

  return {
    descriptor: worker,
    stop() {
      stopped = true;
      if (heartbeat) clearInterval(heartbeat);
      socket?.close();
    },
  };
}
