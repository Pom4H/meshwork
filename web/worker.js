const stateEl = document.querySelector("#state");
const logEl = document.querySelector("#log");
const errorEl = document.createElement("pre");
errorEl.id = "last-error";
errorEl.hidden = true;
errorEl.style.borderColor = "#b74c4c";
logEl.before(errorEl);
const connectionEl = document.createElement('pre');
connectionEl.id = 'last-disconnect';
connectionEl.hidden = true;
logEl.before(connectionEl);
const showError = (error) => {
  errorEl.hidden = false;
  errorEl.textContent = `Last error (${new Date().toLocaleTimeString()}):\n${error instanceof Error ? error.stack || error.message : String(error)}\n${navigator.userAgent}`;
};
const startButton = document.querySelector("#start");
const nameInput = document.querySelector("#name");
nameInput.value = localStorage.meshworkWorkerName || nameInput.value;
const canvas = document.querySelector("#preview");
const preview = canvas.getContext("2d");
import { renderVideoSegment, videoCodecs } from "./render-video.js";
import { workerConnection } from "./worker-connection.js";
const engineCanvas = document.createElement("canvas");
engineCanvas.hidden = true;
canvas.before(engineCanvas);
let renderQueue = Promise.resolve();

let socket;
let wakeLock;
let device;
let pipeline;
let active;

const log = (message) => {
  logEl.textContent = String(message);
};

const setState = (message) => {
  stateEl.textContent = message;
};

const workerId = localStorage.meshworkWorkerId ?? crypto.randomUUID();
localStorage.meshworkWorkerId = workerId;

const wsUrl = new URL("/ws", location.href);
wsUrl.protocol = location.protocol === "https:" ? "wss:" : "ws:";

async function ensureGpu() {
  if (!globalThis.navigator.gpu) {
    throw new Error(
      "WebGPU is not available. Use iPadOS 26+ Safari over HTTPS.",
    );
  }
  if (device) return device;

  const adapter = await navigator.gpu.requestAdapter({
    powerPreference: "high-performance",
  });
  if (!adapter) throw new Error("No WebGPU adapter");

  device = await adapter.requestDevice();
  device.lost.then((info) => {
    device = undefined;
    pipeline = undefined;
    setState("GPU lost");
    log(`GPU device lost: ${info.message || info.reason}`);
    showError(`GPU device lost: ${info.message || info.reason}`);
    socket?.reconnect();
  });

  return device;
}

function ensurePipeline(gpu) {
  if (pipeline) return pipeline;

  const shader = gpu.createShaderModule({
    code: `
      struct Params {
        seed: f32,
        aspect: f32,
      }

      @group(0) @binding(0) var<uniform> params: Params;

      struct Out {
        @builtin(position) position: vec4f,
        @location(0) uv: vec2f,
      }

      @vertex fn vs(@builtin(vertex_index) index: u32) -> Out {
        var p = array<vec2f, 3>(
          vec2f(-1.0, -1.0),
          vec2f( 3.0, -1.0),
          vec2f(-1.0,  3.0)
        );
        var out: Out;
        out.position = vec4f(p[index], 0.0, 1.0);
        out.uv = p[index] * 0.5 + vec2f(0.5);
        return out;
      }

      @fragment fn fs(in: Out) -> @location(0) vec4f {
        let uv = vec2f(in.uv.x * params.aspect, in.uv.y);
        let center = vec2f(0.5 * params.aspect, 0.5);
        let d = distance(uv, center);
        let glow = exp(-8.0 * d);
        let wave = 0.5 + 0.5 * sin((uv.x + uv.y + params.seed) * 8.0);
        let sky = mix(vec3f(0.015, 0.02, 0.035), vec3f(0.12, 0.28, 0.55), in.uv.y);
        let warm = vec3f(1.0, 0.46, 0.08) * glow * (0.7 + 0.3 * wave);
        return vec4f(sky + warm, 1.0);
      }
    `,
  });

  pipeline = gpu.createRenderPipeline({
    layout: "auto",
    vertex: { module: shader, entryPoint: "vs" },
    fragment: {
      module: shader,
      entryPoint: "fs",
      targets: [{ format: "rgba8unorm" }],
    },
    primitive: { topology: "triangle-list" },
  });

  return pipeline;
}

function taskInput(value) {
  const input =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const width = Math.min(
    4096,
    Math.max(1, Math.floor(Number(input.width) || 1280)),
  );
  const height = Math.min(
    4096,
    Math.max(1, Math.floor(Number(input.height) || 720)),
  );
  const seed = Number.isFinite(Number(input.seed)) ? Number(input.seed) : 0;
  return { width, height, seed };
}

async function renderFrame(input) {
  const gpu = await ensureGpu();
  const renderPipeline = ensurePipeline(gpu);
  const { width, height, seed } = taskInput(input);
  const startedAt = performance.now();

  const texture = gpu.createTexture({
    size: [width, height],
    format: "rgba8unorm",
    usage: GPUTextureUsage.RENDER_ATTACHMENT | GPUTextureUsage.COPY_SRC,
  });

  const params = new Float32Array([seed, width / height, 0, 0]);
  const uniform = gpu.createBuffer({
    size: 16,
    usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
  });
  gpu.queue.writeBuffer(uniform, 0, params);

  const bindGroup = gpu.createBindGroup({
    layout: renderPipeline.getBindGroupLayout(0),
    entries: [{ binding: 0, resource: { buffer: uniform } }],
  });

  const bytesPerPixel = 4;
  const unpaddedBytesPerRow = width * bytesPerPixel;
  const bytesPerRow = Math.ceil(unpaddedBytesPerRow / 256) * 256;
  const readback = gpu.createBuffer({
    size: bytesPerRow * height,
    usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ,
  });

  const encoder = gpu.createCommandEncoder();
  const pass = encoder.beginRenderPass({
    colorAttachments: [
      {
        view: texture.createView(),
        clearValue: { r: 0, g: 0, b: 0, a: 1 },
        loadOp: "clear",
        storeOp: "store",
      },
    ],
  });
  pass.setPipeline(renderPipeline);
  pass.setBindGroup(0, bindGroup);
  pass.draw(3);
  pass.end();

  encoder.copyTextureToBuffer(
    { texture },
    { buffer: readback, bytesPerRow, rowsPerImage: height },
    [width, height],
  );

  gpu.queue.submit([encoder.finish()]);
  await readback.mapAsync(GPUMapMode.READ);

  const mapped = new Uint8Array(readback.getMappedRange());
  const pixels = new Uint8ClampedArray(unpaddedBytesPerRow * height);
  for (let y = 0; y < height; y += 1) {
    const source = mapped.subarray(
      y * bytesPerRow,
      y * bytesPerRow + unpaddedBytesPerRow,
    );
    pixels.set(source, y * unpaddedBytesPerRow);
  }

  readback.unmap();
  readback.destroy();
  uniform.destroy();
  texture.destroy();

  canvas.width = width;
  canvas.height = height;
  preview.putImageData(new ImageData(pixels, width, height), 0, 0);

  const blob = await new Promise((resolve, reject) => {
    canvas.toBlob(
      (value) =>
        value ? resolve(value) : reject(new Error("PNG encode failed")),
      "image/png",
    );
  });

  return {
    blob,
    width,
    height,
    renderMs: performance.now() - startedAt,
  };
}

async function uploadArtifact(taskId, attemptId, blob) {
  const response = await fetch(
    `/artifacts/${encodeURIComponent(taskId)}/${encodeURIComponent(attemptId)}`,
    {
      method: "POST",
      headers: { "content-type": blob.type },
      body: blob,
    },
  );
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function send(message) {
  return socket?.send(message);
}

async function handleAssignment(message) {
  active?.controller.abort();
  const job = {
    taskId: message.task.id,
    attemptId: message.attemptId,
    controller: new AbortController(),
  };
  active = job;
  renderQueue = renderQueue
    .catch(() => {})
    .then(() => executeAssignment(message, job));
}

async function executeAssignment(message, job) {
  if (job.controller.signal.aborted) return;
  setState("rendering");
  log(
    `Rendering ${message.task.id}\n${JSON.stringify(message.task.input, null, 2)}`,
  );

  try {
    const video = message.task.capability === "render.video.segment.v1";
    engineCanvas.hidden = !video;
    canvas.hidden = video;
    const result = video
      ? await renderVideoSegment(
          message.task.input,
          engineCanvas,
          job.controller.signal,
          (n, total) => setState(`rendering ${n}/${total}`),
          (stage) => {
            if (active !== job || job.controller.signal.aborted) return;
            socket?.heartbeat();
            setState(stage);
          },
        )
      : await renderFrame(message.task.input);
    if (active !== job || job.controller.signal.aborted) return;

    setState("uploading");
    const artifact = await uploadArtifact(
      message.task.id,
      message.attemptId,
      result.blob,
    );
    if (active !== job || job.controller.signal.aborted) return;

    send({
      type: "task.result",
      taskId: message.task.id,
      attemptId: message.attemptId,
      output: {
        artifact,
        width: result.width,
        height: result.height,
        renderMs: result.renderMs,
        ...(video
          ? {
              frameCount: result.frameCount,
              startFrame: result.startFrame,
              mimeType: result.mimeType,
              encoder: result.encoder,
              renderer: result.renderer,
            }
          : {}),
        userAgent: navigator.userAgent,
      },
    });
    setState("ready");
    log(
      `Completed ${message.task.id}\n${result.width}×${result.height} in ${result.renderMs.toFixed(1)} ms\n${artifact.url}`,
    );
  } catch (error) {
    if (active !== job || job.controller.signal.aborted) return;
    showError(error);
    send({
      type: "task.error",
      taskId: message.task.id,
      attemptId: message.attemptId,
      error: error instanceof Error ? error.message : String(error),
    });
    setState("error");
    log(error instanceof Error ? error.stack || error.message : String(error));
  } finally {
    if (active === job) active = undefined;
  }
}

async function requestWakeLock() {
  try {
    wakeLock = await navigator.wakeLock?.request("screen");
  } catch {
    wakeLock = undefined;
  }
}

async function start() {
  localStorage.meshworkWorkerName = nameInput.value.trim();
  active?.controller.abort();
  active = undefined;
  socket?.stop();
  startButton.disabled = true;
  setState("initializing GPU");

  try {
    // The scene runtime owns its GPU device. Allocate the demo frame device
    // lazily only if a render.webgpu.frame.v1 task actually arrives.
    if (!navigator.gpu) throw Error("WebGPU is unavailable in this browser");
    if (!await navigator.gpu.requestAdapter({ powerPreference: "high-performance" }))
      throw Error("No WebGPU adapter");
    await requestWakeLock();
    const codecs = await videoCodecs();

    socket = workerConnection({
      url: wsUrl,
      hello: () => ({
        type: "worker.hello",
        worker: {
          id: workerId,
          name: nameInput.value.trim() || "iPad WebGPU",
          platform: navigator.platform || "browser",
          capabilities: [
            "render.webgpu.frame.v1",
            'scene.live-controls.v1',
            ...(codecs.length ? ["render.video.segment.v1"] : []),
          ],
          videoCodecs: codecs,
        },
      }),
      heartbeat: () => ({
          type: "worker.heartbeat",
          workerId,
          taskId: active?.taskId,
          attemptId: active?.attemptId,
      }),
      onOpen: () => {
      setState("ready");
      log(
        `Connected. Video codecs (720p60 probe): ${codecs.join(", ") || "none"}. Waiting for tasks.`,
      );
      },
      onMessage: (event) => {
      let message;
      try {
        message = JSON.parse(String(event.data));
      } catch {
        return;
      }

      if (message.type === 'worker.ping') {
        socket?.heartbeat(true);
      } else if (message.type === "task.assign") {
        void handleAssignment(message);
      } else if (
        message.type === "task.cancel" &&
        active?.taskId === message.taskId &&
        active?.attemptId === message.attemptId
      ) {
        active.controller.abort();
        setState("cancelling");
      }
      },
      onDisconnect: (event) => {
      active?.controller.abort();
      active = undefined;
      setState("disconnected");
      connectionEl.hidden = false;
      connectionEl.textContent = `Last disconnect (${new Date().toLocaleTimeString()}): ${event.code} ${event.reason || '(no reason)'}`;
      },
      onRetry: delay => setState(`reconnecting in ${delay / 1000}s`),
      onFatal: () => { setState('connection rejected'); startButton.disabled = false; },
    });
  } catch (error) {
    showError(error);
    setState("error");
    log(error instanceof Error ? error.stack || error.message : String(error));
    startButton.disabled = false;
  }
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    socket?.resume();
    void requestWakeLock();
  }
});
window.addEventListener('online', () => socket?.resume());

startButton.addEventListener("click", () => void start());
startButton.disabled = false;
