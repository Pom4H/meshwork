// Native browser integration test. The two contexts model separate iPad nodes;
// they are desktop Chrome contexts, not measurements of physical iPads.
import { startBroker } from "../src/broker";
import { mkdir, readFile } from "node:fs/promises";
import { spawn, spawnSync } from "node:child_process";
import { resolve } from "node:path";
import assert from "node:assert/strict";

const selected = process.argv[2] ?? "snowy-arcade-fidelity";
const frameCount = Number(process.env.QA_SEGMENT_FRAMES ?? 12);
const startFrame=Number(process.env.QA_START_FRAME??0);
const liveChat=process.env.QA_LIVE_CHAT==='1';
assert(Number.isSafeInteger(frameCount) && frameCount > 0 && frameCount <= 240);
const width = Number(process.argv[3] ?? 3840),
  height = Math.round((width * 9) / 16);
const directory = resolve("artifacts/qa-video/" + Date.now());
await mkdir(directory, { recursive: true });
const broker = startBroker({ port: 0, artifactDir: directory + "/segments",chatDir:directory+'/chat',...(liveChat?{director:async()=>({reply:'Подготовил более прозрачный туман.',patch:{rendering:{fogDensity:.14},audioGain:.8,cameraSpeed:.7,lightScale:.85,entities:[{id:'chat-qa-bench',position:[-8,.7,-3.5],mesh:{kind:'box',size:[1.6,.12,.45]},material:{color:[.14,.07,.04,1],roughness:.8}}]}})}:{}) });
const base = `http://127.0.0.1:${broker.port}`;
const chrome = spawn(
  process.env.CHROME_PATH ??
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
  [
    "--headless=new",
    "--no-first-run",
    "--no-default-browser-check",
    "--enable-unsafe-webgpu",
    "--enable-gpu",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--user-data-dir=" + directory + "/chrome",
    "--remote-debugging-port=0",
    "--window-size=1280,720",
    "about:blank",
  ],
  { windowsHide: true, stdio: "ignore" },
);
let ws: WebSocket | undefined;
try {
  let debugPort = 0;
  for (let n = 0; n < 100; n++) {
    try {
      debugPort = Number(
        (
          await readFile(directory + "/chrome/DevToolsActivePort", "utf8")
        ).split("\n")[0],
      );
      if (debugPort) break;
    } catch {}
    await Bun.sleep(100);
  }
  assert(debugPort, "Chrome DevTools did not start");
  const browser = await fetch(
    `http://127.0.0.1:${debugPort}/json/version`,
  ).then((r) => r.json() as Promise<{ webSocketDebuggerUrl: string }>);
  ws = new WebSocket(browser.webSocketDebuggerUrl);
  await new Promise<void>((r, j) => {
    ws!.addEventListener("open", () => r(), { once: true });
    ws!.addEventListener("error", () => j(Error("CDP failed")), { once: true });
  });
  let seq = 0;
  const pending = new Map<
      number,
      { resolve: (v: any) => void; reject: (e: Error) => void }
    >(),
    exceptions: any[] = [];
  ws.addEventListener("message", (e) => {
    const m = JSON.parse(String(e.data));
    if (m.method === "Runtime.exceptionThrown")
      exceptions.push(m.params.exceptionDetails);
    const p = pending.get(m.id);
    if (!p) return;
    pending.delete(m.id);
    m.error ? p.reject(Error(m.error.message)) : p.resolve(m.result);
  });
  const send = (method: string, params: any = {}, sessionId?: string) =>
    new Promise<any>((resolve, reject) => {
      const id = ++seq;
      pending.set(id, { resolve, reject });
      ws!.send(
        JSON.stringify({
          id,
          method,
          params,
          ...(sessionId ? { sessionId } : {}),
        }),
      );
    });
  const page = async (url: string) => {
    const context = await send("Target.createBrowserContext");
    const target = await send("Target.createTarget", {
      url: "about:blank",
      browserContextId: context.browserContextId,
    });
    const { sessionId } = await send("Target.attachToTarget", {
      targetId: target.targetId,
      flatten: true,
    });
    await send("Runtime.enable", {}, sessionId);
    await send("Page.enable", {}, sessionId);
    await send("Page.navigate", { url }, sessionId);
    return sessionId as string;
  };
  const evaluate = async (session: string, expression: string) => {
    const result = await send(
      "Runtime.evaluate",
      { expression, returnByValue: true, awaitPromise: true },
      session,
    );
    if (result.exceptionDetails)
      throw Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  };
  const a = await page(base),
    b = await page(base);
  for (const [n, s] of [a, b].entries()) {
    await evaluate(s, `(() => {
      window.qaDeviceCount = 0;
      window.qaEncoderConfigs = [];
      const requestAdapter = navigator.gpu.requestAdapter.bind(navigator.gpu);
      navigator.gpu.requestAdapter = async (...args) => {
        const adapter = await requestAdapter(...args);
        if (!adapter) return adapter;
        const requestDevice = adapter.requestDevice.bind(adapter);
        adapter.requestDevice = (...options) => {
          window.qaDeviceCount++;
          return requestDevice(...options);
        };
        return adapter;
      };
      const supported = VideoEncoder.isConfigSupported.bind(VideoEncoder);
      VideoEncoder.isConfigSupported = (config) => {
        window.qaEncoderConfigs.push(config);
        ${process.env.QA_ENCODER_FALLBACK === "1" && n === 0
          ? 'if (config.hardwareAcceleration === "prefer-hardware") return Promise.resolve({ supported: false, config });'
          : ""}
        return supported(config);
      };
    })()`);
  }
  for (const [n, s] of [a, b].entries()) {
    for (let i = 0; i < 100; i++) {
      const ready = await evaluate(
        s,
        "!!document.getElementById('start') && !document.getElementById('start').disabled && typeof navigator.gpu !== 'undefined'",
      );
      if (ready) break;
      await Bun.sleep(100);
    }
    await evaluate(
      s,
      `document.getElementById('name').value='QA node ${n + 1}';document.getElementById('start').click();`,
    );
  }
  let workers: any[] = [];
  for (let i = 0; i < 100; i++) {
    workers = await fetch(base + "/workers").then(
      (r) => r.json() as Promise<any[]>,
    );
    if (workers.length === 2) break;
    await Bun.sleep(200);
  }
  if (workers.length !== 2)
    throw Error(
      "Workers failed to connect: " +
        JSON.stringify(
          await Promise.all(
            [a, b].map((s) =>
              evaluate(s, "document.getElementById('log').textContent"),
            ),
          ),
        ),
    );
  assert(
    workers.every((w) => w.videoCodecs?.includes("avc")),
    "Native AVC encoder support unavailable: " + JSON.stringify(workers),
  );
  for (const s of [a, b]) {
    const initial = await evaluate(s, "({devices: window.qaDeviceCount, configs: window.qaEncoderConfigs})");
    assert.equal(initial.devices, 0, "Worker must not allocate an unused demo GPU device");
    assert(initial.configs.length > 0);
    assert(initial.configs.every((c: any) => c.width === 1280 && c.height === 720 && c.latencyMode === "realtime"));
  }
  const scenes = await fetch(base + "/scenes").then(
      (r) => r.json() as Promise<any[]>,
    ),
    scene = scenes.find((s) => s.name === selected || s.sceneHash === selected);
  assert(scene, "Register the scene before QA");
  const response = await fetch(base + "/streams", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      capability: "render.video.segment.v1",
      width,
      height,
      fps: 60,
      codec: "avc",
      segmentFrames: frameCount,
      warmupFrames: 24,
      startFrame,
      sceneHash: scene.sceneHash,
      renderHash: scene.renderHash,
    }),
  });
  assert.equal(response.status, 201, await response.clone().text());
  const stream = (await response.json()) as any,
    id = stream.spec.id;
  await fetch(`${base}/streams/${id}/segments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ count: 2 }),
  });
  let segments: any[] = [];
  const began = Date.now();
  while (Date.now() - began < 240000) {
    segments = await fetch(`${base}/streams/${id}/segments`).then(
      (r) => r.json() as Promise<any[]>,
    );
    if (segments.some((s) => s.status === "failed"))
      throw Error(JSON.stringify(segments));
    if (segments.every((s) => s.status === "completed")) break;
    await Bun.sleep(1000);
  }
  assert(
    segments.every((s) => s.status === "completed"),
    "Timed out: " + JSON.stringify(segments),
  );
  assert.equal(
    new Set(segments.map((s) => s.workerId)).size,
    2,
    "Both worker contexts must contribute",
  );
  if(liveChat){
    const response=await fetch(`${base}/streams/${id}/chat`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name:'QA',text:'Ослабь туман и замедли камеру',clientId:'native-qa'})});
    assert.equal(response.status,202);
    for(let n=0;n<50;n++){const room=await fetch(`${base}/streams/${id}/chat`).then(r=>r.json() as Promise<any>);if(!room.busy)break;await Bun.sleep(100);}
    const state=await fetch(`${base}/streams/${id}`).then(r=>r.json() as Promise<any>);
    assert.equal(state.currentEpoch.id,stream.currentEpoch.id);assert.equal(state.sceneChanges[0].startFrame,startFrame+frameCount*2);
    await fetch(`${base}/streams/${id}/reactions`,{method:'POST',body:JSON.stringify({emoji:'🐎',clientId:'native-qa'})});
    await fetch(`${base}/streams/${id}/segments`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({count:2})});
    for(let n=0;n<240;n++){
      segments=await fetch(`${base}/streams/${id}/segments`).then(r=>r.json() as Promise<any[]>);
      assert(!segments.some(s=>s.status==='failed'),JSON.stringify(segments));
      if(segments.length===4&&segments.every(s=>s.status==='completed'))break;
      await Bun.sleep(1000);
    }
    assert.equal(segments.length,4);assert(segments.every(s=>s.status==='completed'));
    for(const s of [a,b])assert.equal(await evaluate(s,'window.qaDeviceCount'),1,'Live patch created another GPU device');
  }
  const probes = [];
  for (const [n, segment] of segments.entries()) {
    const out = segment.output;
    assert.equal(out.frameCount, frameCount);
    assert.equal(out.width, width);
    assert.equal(out.height, height);
    assert(out.renderer.triangles > 0);
    const file = directory + `/segment-${n}.mp4`;
    await Bun.write(
      file,
      await fetch(out.artifact.url).then((r) => r.arrayBuffer()),
    );
    const result = spawnSync(
      process.env.FFPROBE_PATH ?? "ffprobe",
      [
        "-v",
        "error",
        "-count_frames",
        "-show_streams",
        "-show_packets",
        "-of",
        "json",
        file,
      ],
      { encoding: "utf8", windowsHide: true },
    );
    assert.equal(result.status, 0, result.stderr);
    const probe = JSON.parse(result.stdout),
      track = probe.streams.find((s:any)=>s.codec_type==='video');
    const videoPackets=probe.packets.filter((p:any)=>p.stream_index===track.index);
    assert.equal(track.width, width);
    assert.equal(track.height, height);
    assert.equal(Number(track.nb_read_frames), frameCount);
    assert(
      Math.abs(Number(videoPackets[0].pts_time) - (startFrame+n * frameCount) / 60) <
        0.002,
      "Absolute segment PTS mismatch",
    );
    const audio=probe.streams.find((s:any)=>s.codec_type==='audio');
    if(scene.audioEntry){
      assert.equal(audio?.codec_name,'aac');assert.equal(audio.channels,2);assert.equal(audio.sample_rate,'48000');
      assert(Math.abs(Number(audio.start_time)-(startFrame+n*frameCount)/60)<.03,'Audio PTS mismatch');
      const decoded=spawnSync(process.env.FFMPEG_PATH??'ffmpeg',['-v','error','-i',file,'-map','0:a:0','-f','f32le','pipe:1'],{windowsHide:true});
      assert.equal(decoded.status,0,decoded.stderr.toString());
      const pcm=new Float32Array(decoded.stdout.buffer,decoded.stdout.byteOffset,decoded.stdout.length/4);
      const rms=Math.sqrt(pcm.reduce((sum,v)=>sum+v*v,0)/pcm.length);assert(rms>.0001,'Silent soundtrack');
      audio.rms=rms;
    }
    // Metadata alone cannot detect a cleared WebGPU swapchain: decode pixels.
    const pixels = spawnSync(
      process.env.FFMPEG_PATH ?? "ffmpeg",
      [
        "-v",
        "error",
        "-i",
        file,
        "-frames:v",
        "1",
        "-vf",
        "scale=64:36",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "pipe:1",
      ],
      { windowsHide: true },
    );
    assert.equal(pixels.status, 0, pixels.stderr.toString());
    assert.equal(pixels.stdout.length, 64 * 36);
    const mean =
      pixels.stdout.reduce((sum, v) => sum + v, 0) / pixels.stdout.length;
    const deviation = Math.sqrt(
      pixels.stdout.reduce((sum, v) => sum + (v - mean) ** 2, 0) /
        pixels.stdout.length,
    );
    assert(
      deviation > 8 &&
        Math.max(...pixels.stdout) - Math.min(...pixels.stdout) > 50,
      "Decoded scene is blank or a solid color",
    );
    const preview = spawnSync(
      process.env.FFMPEG_PATH ?? "ffmpeg",
      [
        "-v",
        "error",
        "-y",
        "-i",
        file,
        "-frames:v",
        "1",
        "-vf",
        "scale=960:540",
        directory + `/decoded-${n}.png`,
      ],
      { windowsHide: true },
    );
    assert.equal(preview.status, 0, preview.stderr.toString());
    probes.push({
      workerId: segment.workerId,
      startFrame: out.startFrame,
      renderMs: out.renderMs,
      codec: track.codec_name,
      frames: track.nb_read_frames,
      firstPts: videoPackets[0].pts_time,
      ...(audio?{audio:{codec:audio.codec_name,channels:audio.channels,sampleRate:audio.sample_rate,firstPts:audio.start_time,rms:audio.rms}}:{}),
      pixelDeviation: deviation,
    });
  }
  await fetch(`${base}/streams/${id}/stop`, { method: "POST" });
  const viewer = await page(`${base}/watch/${id}`);
  let playback;
  for (let i = 0; i < 100; i++) {
    playback = await evaluate(
      viewer,
      "(()=>{const v=document.querySelector('video');return {width:v.videoWidth,height:v.videoHeight,time:v.currentTime,sourceTime:window.meshworkSourceTime?.(),error:document.getElementById('error')?.textContent,paused:v.paused,readyState:v.readyState,duration:v.duration,buffered:Array.from({length:v.buffered.length},(_,i)=>[v.buffered.start(i),v.buffered.end(i)]),hlsErrors:window.meshworkHlsErrors};})()",
    );
    if (playback?.sourceTime >= (startFrame+frameCount*(liveChat?3:1)) / 60) break;
    await Bun.sleep(200);
  }
  assert(
    playback.sourceTime >= (startFrame+frameCount*(liveChat?3:1)) / 60,
    "HLS did not play across the worker boundary: " + JSON.stringify(playback),
  );
  assert.equal(playback.width, width);
  assert.equal(exceptions.length, 0, JSON.stringify(exceptions));
  if(liveChat){
    const ui=await evaluate(viewer,"({messages:document.querySelector('#messages').textContent,reactions:document.querySelector('[data-emoji=\"🐎\"] span').textContent,muted:document.querySelector('video').muted})");
    assert(ui.messages.includes('Подготовил'));assert.equal(ui.reactions,'1');assert.equal(ui.muted,true);
    const button=await evaluate(viewer,"(()=>{const r=document.querySelector('#sound').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()");
    await send('Input.dispatchMouseEvent',{type:'mousePressed',...button,button:'left',clickCount:1},viewer);
    await send('Input.dispatchMouseEvent',{type:'mouseReleased',...button,button:'left',clickCount:1},viewer);
    assert.equal(await evaluate(viewer,"document.querySelector('video').muted"),false);
  }
  const screenshot = await send(
    "Page.captureScreenshot",
    { format: "png" },
    viewer,
  );
  await Bun.write(
    directory + "/player.png",
    Buffer.from(screenshot.data, "base64"),
  );
  const evidence = {
    scene: scene.name,
    sceneHash: scene.sceneHash,
    resolution: [width, height],
    probes,
    playback,
    liveChat,
    environment:
      "Two isolated native desktop Chrome contexts, not physical iPads",
    directory,
  };
  await Bun.write(
    directory + "/evidence.json",
    JSON.stringify(evidence, null, 2),
  );
  console.log(JSON.stringify(evidence, null, 2));
} finally {
  ws?.close();
  chrome.kill();
  broker.stop(true);
}
