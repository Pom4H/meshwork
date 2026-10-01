#!/usr/bin/env bun
import { startBroker } from "./broker";
import type { JsonValue, TaskState } from "./protocol";
import { startWorker } from "./worker";
import { bundleScene } from "./scenes";

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

const apiFetch = (url: string, init: RequestInit = {}) => {
  const headers = new Headers(init.headers);
  if (process.env.MESHWORK_ACCESS_TOKEN)
    headers.set("authorization", `Bearer ${process.env.MESHWORK_ACCESS_TOKEN}`);
  return fetch(url, { ...init, headers });
};

if (command === "broker") {
  const port = Number(flag("--port", process.env.PORT ?? "8787"));
  const certPath = flag("--tls-cert", process.env.MESHWORK_TLS_CERT);
  const keyPath = flag("--tls-key", process.env.MESHWORK_TLS_KEY);
  if ((certPath && !keyPath) || (!certPath && keyPath)) {
    throw new Error("--tls-cert and --tls-key must be supplied together");
  }

  startBroker({
    port,
    sceneDir: flag("--scenes-dir", process.env.MESHWORK_SCENES_DIR),
    accessToken: process.env.MESHWORK_ACCESS_TOKEN,
    hostname: flag("--host", process.env.MESHWORK_HOST),
    enableDirector: args.includes('--codex-director')||process.env.MESHWORK_CODEX_DIRECTOR==='1',
    tls: certPath && keyPath ? { certPath, keyPath } : undefined,
  });
} else if (command === "scene") {
  const engine = flag("--engine"),
    entry = flag("--entry");
  if (!engine || !entry)
    throw Error(
      "usage: meshwork scene --engine PATH --entry tools/scene.mjs [--name NAME]",
    );
  console.log(
    JSON.stringify(
      await bundleScene(
        engine,
        entry,
        flag("--name", "scene")!,
        flag("--scenes-dir"),
        flag("--audio"),
      ),
      null,
      2,
    ),
  );
} else if (command === "stream") {
  const selected = flag("--scene");
  if (!selected)
    throw Error(
      "usage: meshwork stream --scene NAME_OR_HASH [--broker URL] [--width 3840 --height 2160 --fps 60 --codec avc]",
    );
  const sceneResponse = await apiFetch(`${httpBase()}/scenes`);
  if (!sceneResponse.ok) throw Error(await sceneResponse.text());
  const scenes = (await sceneResponse.json()) as {
    name: string;
    sceneHash: string;
    renderHash: string;
  }[];
  const matches = scenes.filter(
    (s) => s.name === selected || s.sceneHash === selected,
  );
  if (matches.length !== 1)
    throw Error(
      `Expected one registered scene matching ${selected}, got ${matches.length}; use its exact sceneHash`,
    );
  const scene = matches[0]!;
  const response = await apiFetch(`${httpBase()}/streams`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      capability: "render.video.segment.v1",
      width: Number(flag("--width", "3840")),
      height: Number(flag("--height", "2160")),
      fps: Number(flag("--fps", "60")),
      codec: flag("--codec", "avc"),
      segmentFrames: Number(flag("--segment-frames", "120")),
      warmupFrames: Number(flag("--warmup-frames", "24")),
      sceneHash: scene.sceneHash,
      renderHash: scene.renderHash,
      live: true,
    }),
  });
  if (!response.ok) throw Error(await response.text());
  const stream = (await response.json()) as { spec: { id: string } };
  const access = process.env.MESHWORK_ACCESS_TOKEN
    ? `?access_token=${encodeURIComponent(process.env.MESHWORK_ACCESS_TOKEN)}`
    : "";
  console.log(
    JSON.stringify(
      {
        ...stream,
        workerUrl: `${httpBase()}/${access}`,
        playerUrl: `${httpBase()}/watch/${stream.spec.id}${access}`,
        playlistUrl: `${httpBase()}/streams/${stream.spec.id}/playlist.m3u8`,
      },
      null,
      2,
    ),
  );
} else if (command === "stop-stream") {
  if (!args[1])
    throw Error("usage: meshwork stop-stream STREAM_ID [--broker URL]");
  const response = await apiFetch(
    `${httpBase()}/streams/${encodeURIComponent(args[1])}/stop`,
    { method: "POST" },
  );
  if (!response.ok) throw Error(await response.text());
  console.log(JSON.stringify(await response.json(), null, 2));
} else if (command === "worker") {
  startWorker({
    brokerUrl: wsUrl(),
    id: flag("--id", process.env.MESHWORK_WORKER_ID),
    name: flag("--name", process.env.MESHWORK_WORKER_NAME),
    accessToken: process.env.MESHWORK_ACCESS_TOKEN,
  });
} else if (command === "run") {
  const capability = args[1];
  if (!capability) {
    throw new Error("usage: meshwork run <capability> [--input JSON]");
  }

  const rawInput = flag("--input", "null") ?? "null";
  const input = JSON.parse(rawInput) as JsonValue;
  const response = await apiFetch(`${httpBase()}/tasks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ capability, input }),
  });

  if (!response.ok) throw new Error(await response.text());
  const submitted = (await response.json()) as TaskState;

  for (;;) {
    const taskResponse = await apiFetch(
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
  const response = await apiFetch(`${httpBase()}/workers`);
  if (!response.ok) throw new Error(await response.text());
  console.log(JSON.stringify(await response.json(), null, 2));
} else {
  console.log(`meshwork

Commands:
  meshwork broker [--port 8787] [--tls-cert CERT --tls-key KEY]
  meshwork worker [--broker http://localhost:8787] [--name NAME]
  meshwork run <capability> [--input JSON] [--broker URL]
  meshwork workers [--broker URL]
  meshwork scene --engine PATH --entry tools/scene.mjs [--name NAME]
  meshwork stream --scene NAME_OR_HASH [--broker URL] [--width 3840 --height 2160 --fps 60 --codec avc]
  meshwork stop-stream STREAM_ID [--broker URL]
`);
}
