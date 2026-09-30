#!/usr/bin/env bun
import { startBroker } from "./broker";
import type { JsonValue, TaskState } from "./protocol";
import { startWorker } from "./worker";

const args = Bun.argv.slice(2);
const command = args[0];

const flag = (name: string, fallback?: string) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};

const httpBase = () =>
  (flag("--broker", process.env.MESHWORK_BROKER) ?? "http://localhost:8787")
    .replace(/^ws:/, "http:")
    .replace(/^wss:/, "https:")
    .replace(/\/ws\/?$/, "");

const wsUrl = () =>
  `${httpBase()
    .replace(/^http:/, "ws:")
    .replace(/^https:/, "wss:")}/ws`;

if (command === "broker") {
  const port = Number(flag("--port", process.env.PORT ?? "8787"));
  const certPath = flag("--tls-cert", process.env.MESHWORK_TLS_CERT);
  const keyPath = flag("--tls-key", process.env.MESHWORK_TLS_KEY);
  if ((certPath && !keyPath) || (!certPath && keyPath)) {
    throw new Error("--tls-cert and --tls-key must be supplied together");
  }

  startBroker({
    port,
    tls: certPath && keyPath ? { certPath, keyPath } : undefined,
  });
} else if (command === "worker") {
  startWorker({
    brokerUrl: wsUrl(),
    id: flag("--id", process.env.MESHWORK_WORKER_ID),
    name: flag("--name", process.env.MESHWORK_WORKER_NAME),
  });
} else if (command === "run") {
  const capability = args[1];
  if (!capability) {
    throw new Error("usage: meshwork run <capability> [--input JSON]");
  }

  const rawInput = flag("--input", "null") ?? "null";
  const input = JSON.parse(rawInput) as JsonValue;
  const response = await fetch(`${httpBase()}/tasks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ capability, input }),
  });

  if (!response.ok) throw new Error(await response.text());
  const submitted = (await response.json()) as TaskState;

  for (;;) {
    const taskResponse = await fetch(
      `${httpBase()}/tasks/${submitted.task.id}`,
    );
    const state = (await taskResponse.json()) as TaskState;

    if (state.status === "completed") {
      console.log(JSON.stringify(state.output, null, 2));
      break;
    }
    if (state.status === "failed") {
      throw new Error(state.error ?? "task failed");
    }
    await Bun.sleep(100);
  }
} else if (command === "workers") {
  const response = await fetch(`${httpBase()}/workers`);
  if (!response.ok) throw new Error(await response.text());
  console.log(JSON.stringify(await response.json(), null, 2));
} else {
  console.log(`meshwork

Commands:
  meshwork broker [--port 8787] [--tls-cert CERT --tls-key KEY]
  meshwork worker [--broker http://localhost:8787] [--name NAME]
  meshwork run <capability> [--input JSON] [--broker URL]
  meshwork workers [--broker URL]
`);
}
