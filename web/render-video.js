import {
  Output,
  Mp4OutputFormat,
  BufferTarget,
  CanvasSource,
  Quality,
  canEncodeVideo,
} from "/vendor/mediabunny.js";
import {applyLiveScene} from '/live-scene.js';

let cached;
const check = (signal) => {
  if (signal.aborted) throw new DOMException("Task cancelled", "AbortError");
};
const yieldThread = () => new Promise((resolve) => setTimeout(resolve, 0));

function encodingOptions(width, height, fps, hardwareAcceleration) {
  return {
    width,
    height,
    frameRate: fps,
    quality: new Quality({ bitrate: Math.round(width * height * fps * 0.12) }),
    hardwareAcceleration,
    latencyMode: "realtime",
  };
}

async function supportedEncoding(codec, width, height, fps) {
  for (const preference of ["prefer-hardware", "no-preference"]) {
    const options = encodingOptions(width, height, fps, preference);
    if (await canEncodeVideo(codec, options)) return options;
  }
}

export async function videoCodecs() {
  if (!globalThis.VideoEncoder) return [];
  const codecs = [];
  for (const codec of ["avc", "hevc"]) {
    try {
      if (
        await supportedEncoding(codec, 1280, 720, 60)
      )
        codecs.push(codec);
    } catch {}
  }
  return codecs;
}

function rewriteTextures(spec, base) {
  const visited = new WeakSet();
  for (const entity of spec.entities) {
    const texture = entity.material?.texture;
    if (!texture || visited.has(texture)) continue;
    visited.add(texture);
    for (const field of ["url", "normalUrl", "ormUrl"])
      if (texture[field]) {
        const path = texture[field];
        if (!path.startsWith("/tools/") && !path.startsWith("/assets/"))
          throw Error("Scene textures must be bundled local assets");
        texture[field] = base + path;
      }
  }
  return spec;
}

async function sceneRuntime(input, canvas, signal) {
  const key = `${input.sceneHash}:${input.renderHash}:${input.width}:${input.height}`;
  if (cached?.key === key) return cached;
  cached?.runtime.dispose();
  cached = undefined;
  const base = `/scenes/${input.sceneHash}`;
  const response = await fetch(base, { signal });
  if (!response.ok) throw Error(await response.text());
  const manifest = await response.json();
  if (
    manifest.sceneHash !== input.sceneHash ||
    manifest.renderHash !== input.renderHash
  )
    throw Error("Scene/renderer hash mismatch");
  const [{ default: spec }, { BrowserRuntime }, {cameraOnRail}] = await Promise.all([
    import(base + "/" + manifest.entry),
    import(base + "/" + manifest.runtime),
    import(base + '/dist/scene/camera-rail.js'),
  ]);
  check(signal);
  const scene = rewriteTextures(structuredClone(spec), base);
  const runtime = await BrowserRuntime.create(canvas, scene, {
    autoplay: false,
    audio: false,
    exposeBridge: false,
    resolution: [input.width, input.height],
  });
  if (
    !runtime.renderer.setTimelineFrame ||
    !runtime.renderer.resetTemporalHistory
  ) {
    runtime.dispose();
    throw Error(
      "This game-engine build needs setTimelineFrame/resetTemporalHistory for distributed rendering",
    );
  }
  cached = { key, runtime, spec: scene, cameraOnRail };
  check(signal);
  return cached;
}

/** Deterministic absolute simulation time; yield during replay to renew leases. */
async function advance(runtime, frame, fps, signal, capture) {
  const target = Math.round((frame / fps) * runtime.tickRate);
  if (runtime.scene.tick > target)
    throw Error("Simulation timeline moved backwards without reset");
  while (runtime.scene.tick < target) {
    check(signal);
    for (let n = 0; n < 120 && runtime.scene.tick < target; n++) {
      runtime.scene.step(runtime.stepSeconds);
      runtime.scene.drainEvents();
      runtime.scene.contacts.drainFootsteps();
    }
    await yieldThread();
  }
  runtime.renderer.setTimelineFrame(frame);
  if(cached?.runtime===runtime)applyLiveScene(runtime,cached.spec,cached.sceneChanges,frame,fps,cached.cameraOnRail);
  runtime.render();
  capture?.();
  await runtime.drain();
  check(signal);
}

export async function renderVideoSegment(
  input,
  canvas,
  signal,
  progress = () => {},
) {
  let stage = "validate segment";
  try {
    return await encodeSegment(input, canvas, signal, progress, (value) => {
      stage = value;
    });
  } catch (error) {
    // A failed GPU/encoder must not poison every subsequent assignment.
    cached?.runtime.dispose();
    cached = undefined;
    if (signal.aborted) throw error;
    throw new Error(`${stage}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
}

async function encodeSegment(input, canvas, signal, progress, reportStage) {
  if (input.snapshotHash)
    throw Error("Snapshot restore is unavailable; use deterministic replay");
  if (
    !["avc", "hevc"].includes(input.codec) ||
    !Number.isSafeInteger(input.frameCount) ||
    input.frameCount < 1 ||
    input.frameCount > 10000
  )
    throw Error("Invalid video segment");
  reportStage("check video encoder");
  const encoding = await supportedEncoding(input.codec, input.width, input.height, input.fps);
  if (!encoding)
    throw Error(
      `${input.codec} encoding unavailable for ${input.width}×${input.height} at ${input.fps} fps`,
    );
  const startedAt = performance.now();
  reportStage("load scene / initialize WebGPU");
  const cache = await sceneRuntime(input, canvas, signal),
    runtime = cache.runtime;
  cache.sceneChanges=input.sceneChanges;
  const continuation =
    cache.nextFrame === input.startFrame &&
    cache.streamId === input.streamId &&
    cache.epoch === input.epoch;
  cache.nextFrame = undefined;
  // Reconstruct physical/surface state from t=0 when the next range overlaps
  // the prior range's history. Keep GPU geometry and material caches resident.
  if (!continuation) {
    reportStage("warm up scene");
    if (
      runtime.scene.tick >
      Math.round((input.warmupStartFrame / input.fps) * runtime.tickRate)
    )
      await runtime.replaceScene(cache.spec);
    check(signal);
    runtime.renderer.resetTemporalHistory();
    await advance(runtime, input.warmupStartFrame, input.fps, signal);
    // Initial range also needs a full convergence warmup even when there is no
    // earlier source timeline. Repeated t=0 frames are never published.
    for (
      let n = input.startFrame - input.warmupStartFrame;
      n < input.warmupFrames;
      n++
    ) {
      runtime.render();
      await runtime.drain();
      check(signal);
    }
    for (
      let frame = input.warmupStartFrame + 1;
      frame < input.startFrame;
      frame++
    )
      await advance(runtime, frame, input.fps, signal);
  }
  let encodedFrames = 0,
    encoderConfig;
  const output = new Output({
    target: new BufferTarget(),
    format: new Mp4OutputFormat({
      fastStart: "fragmented",
      minimumFragmentDuration: input.frameCount / input.fps,
    }),
  });
  // Some browser/driver pairs hand an uninitialized WebGPU swapchain surface
  // to VideoFrame. Materialize its color conversion through a 2D canvas first.
  reportStage("initialize capture / video encoder");
  const encodeCanvas = typeof OffscreenCanvas === "function"
    ? new OffscreenCanvas(input.width, input.height)
    : Object.assign(document.createElement("canvas"), { width: input.width, height: input.height });
  const encodeContext = encodeCanvas.getContext("2d", { alpha: false });
  if (!encodeContext) throw Error("2D capture context unavailable");
  const source = new CanvasSource(encodeCanvas, {
    codec: input.codec,
    quality: encoding.quality,
    hardwareAcceleration: encoding.hardwareAcceleration,
    latencyMode: encoding.latencyMode,
    keyFrameInterval: input.frameCount / input.fps,
    onEncodedPacket: () => encodedFrames++,
    onEncoderConfig: (config) => {
      encoderConfig = config;
    },
  });
  output.addVideoTrack(source, { frameRate: input.fps });
  try {
    await output.start();
    for (let n = 0; n < input.frameCount; n++) {
      reportStage(`render / encode frame ${n + 1}/${input.frameCount}`);
      const frame = input.startFrame + n;
      await advance(runtime, frame, input.fps, signal, () =>
        encodeContext.drawImage(canvas, 0, 0),
      );
      // Absolute PTS survives independently created muxers and out-of-order
      // completion. Frame 0 of each segment is independently decodable.
      await source.add(frame / input.fps, 1 / input.fps, { keyFrame: n === 0 });
      check(signal);
      progress(n + 1, input.frameCount);
    }
    source.close();
    reportStage("finalize MP4");
    await output.finalize();
    check(signal);
    if (encodedFrames !== input.frameCount)
      throw Error(
        `Encoder emitted ${encodedFrames}/${input.frameCount} frames`,
      );
    const renderer = runtime.inspect().renderer;
    if (renderer.errors.length) throw Error(renderer.errors.join("\n"));
    cache.nextFrame = input.startFrame + input.frameCount;
    cache.streamId = input.streamId;
    cache.epoch = input.epoch;
    return {
      blob: new Blob([output.target.buffer], { type: "video/mp4" }),
      width: input.width,
      height: input.height,
      frameCount: encodedFrames,
      startFrame: input.startFrame,
      renderMs: performance.now() - startedAt,
      mimeType: await output.getMimeType(),
      encoder: {
        codec: encoderConfig.codec,
        hardwareAcceleration: encoderConfig.hardwareAcceleration,
      },
      renderer: {
        backend: renderer.backend,
        triangles: renderer.triangles,
        materialTextures: renderer.materialTextures,
      },
    };
  } finally {
    if (output.state !== "finalized" && output.state !== "canceled")
      await output.cancel();
  }
}
